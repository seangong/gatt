package gatt

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// These are application-callback resource bounds, not controller capacities.
const darwinCallbackGroups = 16
const darwinNotificationQueueSize = 16

var errDarwinCallbackCapacity = errors.New("macOS peripheral callback capacity exhausted")
var errDarwinNotificationQueueFull = errors.New("macOS notification callback queue full")

// Each owner has independent lifecycle and notification workers. Connected may
// wait for notifications, and a running notification may wait for Disconnected.
// Native response delivery never enters either callback path.
type darwinCallbacks struct {
	ctx           context.Context
	wake          chan struct{}
	terminalReady chan struct{}
	mu            sync.Mutex
	release       func()
	workers       int

	notifications [darwinNotificationQueueSize]func()
	head, count   int
	terminal      func()
	terminalSet   bool
	notifyStopped bool
}

func (d *device) reserveCallbackGroup() bool {
	d.callbackOnce.Do(func() { d.callbackSlots = make(chan struct{}, darwinCallbackGroups) })
	select {
	case d.callbackSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

// Pending/rejected connects have no connected callback stream. Their one-shot
// error callbacks share the same bound rather than bypassing exhausted slots.
func (d *device) startPeripheralCallout(f func()) bool {
	if !d.reserveCallbackGroup() {
		return false
	}
	go func() {
		defer func() { <-d.callbackSlots }()
		f()
	}()
	return true
}

func (d *device) reportConnectError(p Peripheral, err error) {
	if f := d.peripheralConnected; f != nil && !d.startPeripheralCallout(func() { f(p, err) }) {
		d.fail(errDarwinCallbackCapacity)
	}
}

func (p *peripheral) callbackQueue(d *device, connected func()) *darwinCallbacks {
	p.callbackOnce.Do(func() {
		if !d.reserveCallbackGroup() {
			return
		}
		q := &darwinCallbacks{
			ctx: p.context(), wake: make(chan struct{}, 1), terminalReady: make(chan struct{}),
			workers: 2, release: func() { <-d.callbackSlots },
		}
		p.callbacks = q
		go func() {
			defer q.workerDone()
			q.runLifecycle(connected)
		}()
		go func() {
			defer q.workerDone()
			q.runNotifications()
		}()
	})
	return p.callbacks
}

func (q *darwinCallbacks) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *darwinCallbacks) enqueue(f func()) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.notifyStopped {
		return true
	}
	if q.count == len(q.notifications) {
		return false
	}
	q.notifications[(q.head+q.count)%len(q.notifications)] = f
	q.count++
	q.signal()
	return true
}

// Terminal delivery has an independent signal so it cannot consume a
// notification wake. Both callback workers retain their shared permit until
// they actually return, including when user code ignores cancellation.
func (q *darwinCallbacks) finish(f func()) {
	q.mu.Lock()
	if !q.terminalSet {
		q.terminalSet = true
		q.terminal = f
		q.stopNotificationsLocked()
		close(q.terminalReady)
	}
	q.mu.Unlock()
}

func (q *darwinCallbacks) stopNotificationsLocked() {
	q.notifyStopped = true
	clear(q.notifications[:])
	q.count = 0
	q.signal()
}

func (q *darwinCallbacks) stopNotifications() {
	q.mu.Lock()
	q.stopNotificationsLocked()
	q.mu.Unlock()
}

func (q *darwinCallbacks) workerDone() {
	q.mu.Lock()
	q.workers--
	last := q.workers == 0
	q.mu.Unlock()
	if last {
		q.release()
	}
}

func (q *darwinCallbacks) runLifecycle(connected func()) {
	if connected != nil {
		connected()
	}
	<-q.terminalReady
	q.mu.Lock()
	terminal := q.terminal
	q.terminal = nil
	q.mu.Unlock()
	if terminal != nil {
		terminal()
	}
}

func (q *darwinCallbacks) runNotifications() {
	for {
		q.mu.Lock()
		if q.notifyStopped {
			q.mu.Unlock()
			return
		}
		if q.count == 0 {
			q.mu.Unlock()
			select {
			case <-q.wake:
			case <-q.ctx.Done():
				q.stopNotifications()
			}
			continue
		}
		f := q.notifications[q.head]
		q.notifications[q.head] = nil
		q.head = (q.head + 1) % len(q.notifications)
		q.count--
		q.mu.Unlock()
		if context.Cause(q.ctx) == nil {
			f()
		}
	}
}

func (d *device) finishPeripheralCallbacks(p *peripheral, err error) bool {
	q := p.callbackQueue(d, nil)
	if q == nil {
		return false
	}
	if cause := context.Cause(p.context()); errors.Is(cause, errDarwinNotificationQueueFull) {
		err = cause
	}
	var terminal func()
	if f := d.peripheralDisconnected; f != nil {
		callbackPeripheral := p
		if p.connectSource != nil {
			callbackPeripheral = p.connectSource
		}
		terminal = func() { f(callbackPeripheral, err) }
	}
	q.finish(terminal)
	return true
}

func (p *peripheral) queueNotification(f func()) {
	q := p.callbackQueue(p.d, nil)
	if q == nil {
		p.stop(errDarwinCallbackCapacity)
		p.d.fail(errDarwinCallbackCapacity)
		return
	}
	if q.enqueue(f) {
		return
	}
	err := fmt.Errorf("notifications for %s: %w", p.ID(), errDarwinNotificationQueueFull)
	p.stop(err)
	q.stopNotifications()
	// Only native disconnection (or device failure) completes the lifecycle.
	// An earlier Disconnected callback would still find this owner closing and
	// prevent the callback from immediately reconnecting it.
	// CancelConnection marks the retained owner closing before local enqueue.
	// Do not pre-mark it here: that would suppress the native cancel command.
	p.d.CancelConnection(p)
}
