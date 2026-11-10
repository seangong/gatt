package linux

import (
	"errors"
	"sync"
)

const scanDispatchQueueSize = 16

var errScanQueueFull = errors.New("hci: scan callback queue full")

type scanBatch struct {
	handler func(*PlatData)
	reports []*PlatData
}

// Scan capacity counts complete HCI report batches, independently of the number
// of allowed connections. One worker admits reports outside the HCI core while
// independent slots preserve the configured parallel callback allowance.
type scanDispatcher struct {
	mu      sync.Mutex
	slots   chan struct{}
	pending []scanBatch
	active  scanBatch
	running bool
	stopped bool
}

func (h *HCI) queueScanBatch(batch scanBatch) {
	if len(batch.reports) == 0 {
		return
	}
	h.connsmu.Lock()
	h.initLifecycle()
	if h.closed {
		h.connsmu.Unlock()
		return
	}
	if h.scanDispatch == nil {
		h.scanDispatch = &scanDispatcher{slots: make(chan struct{}, max(1, h.maxConn))}
	}
	accepted := h.scanDispatch.enqueue(h, batch)
	h.connsmu.Unlock()
	if !accepted {
		// Losing an accepted report can hide a peer when duplicate filtering is
		// enabled. End the session visibly instead of silently dropping a batch.
		h.requestClose(errScanQueueFull)
	}
}

func (q *scanDispatcher) enqueue(h *HCI, batch scanBatch) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return true
	}
	if len(q.pending) == scanDispatchQueueSize {
		return false
	}
	q.pending = append(q.pending, batch)
	if !q.running {
		q.running = true
		go q.run(h)
	}
	return true
}

func (q *scanDispatcher) stop() {
	q.mu.Lock()
	q.stopped = true
	clear(q.pending)
	q.pending = nil
	q.active = scanBatch{}
	q.mu.Unlock()
}

func (q *scanDispatcher) run(h *HCI) {
	for {
		q.mu.Lock()
		if q.stopped || len(q.active.reports) == 0 && len(q.pending) == 0 {
			q.active = scanBatch{}
			q.pending = nil
			q.running = false
			q.mu.Unlock()
			return
		}
		if len(q.active.reports) == 0 {
			q.active = q.pending[0]
			q.pending[0] = scanBatch{}
			q.pending = q.pending[1:]
		}
		report := q.active.reports[0]
		q.active.reports[0] = nil
		q.active.reports = q.active.reports[1:]
		handler := q.active.handler
		q.mu.Unlock()
		select {
		case <-h.stopping:
			q.stop()
			continue
		case q.slots <- struct{}{}:
		}
		// Do not reverse requestClose's connsmu -> queue lock order. A report
		// taken from the active batch still needs the final shutdown check.
		h.connsmu.Lock()
		closed := h.closed
		h.connsmu.Unlock()
		if closed {
			<-q.slots
			q.stop()
		} else {
			go func() {
				defer func() { <-q.slots }()
				handler(report)
			}()
		}
	}
}
