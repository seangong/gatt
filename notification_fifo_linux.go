package gatt

import (
	"context"
	"errors"
	"sync"
)

var errPeripheralNotificationCapacity = errors.New("peripheral notification callback capacity exceeded")

// A connection drains notifications in wire order without making the ATT
// receiver or lifecycle callbacks wait for application code. Every queued or
// running callback retains one token from the device's existing global bound.
type peripheralNotifications struct {
	mu        sync.Mutex
	slots     chan struct{}
	pending   []func()
	running   bool
	stopped   bool
	stopWatch func() bool
}

func (p *peripheral) notificationQueue() *peripheralNotifications {
	p.notificationOnce.Do(func() {
		slots := p.callbackSlots
		if slots == nil {
			// Standalone internal connections have the same default bound as
			// a device configured for one connection.
			slots = make(chan struct{}, 16)
		}
		q := &peripheralNotifications{slots: slots}
		p.notifications = q
		if conn, ok := p.l2c.(interface{ Context() context.Context }); ok {
			// Release queued callbacks even if protocol IO has not observed
			// transport cancellation yet. No worker waits for application code.
			q.mu.Lock()
			q.stopWatch = context.AfterFunc(conn.Context(), q.stop)
			q.mu.Unlock()
		}
	})
	return p.notifications
}

func (q *peripheralNotifications) enqueue(p *peripheral, f func()) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped || p.closedError() != nil {
		return true
	}
	if p.d != nil {
		p.d.mu.Lock()
		defer p.d.mu.Unlock()
		if p.d.stopping {
			return false
		}
	}
	select {
	case q.slots <- struct{}{}:
	default:
		return false
	}
	q.pending = append(q.pending, f)
	if !q.running {
		q.running = true
		go q.run(p)
	}
	return true
}

func (q *peripheralNotifications) stop() {
	q.mu.Lock()
	q.stopped = true
	for range q.pending {
		<-q.slots
	}
	clear(q.pending)
	q.pending = nil
	stopWatch := q.stopWatch
	q.stopWatch = nil
	q.mu.Unlock()
	if stopWatch != nil {
		stopWatch()
	}
}

func (q *peripheralNotifications) run(p *peripheral) {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.pending = nil
			q.running = false
			q.mu.Unlock()
			return
		}
		f := q.pending[0]
		q.pending[0] = nil
		q.pending = q.pending[1:]
		q.mu.Unlock()
		if p.closedError() == nil {
			f()
		}
		// A running callback can wait for Stop or Disconnected. Its token
		// belongs to the application call until that call actually returns.
		<-q.slots
	}
}
