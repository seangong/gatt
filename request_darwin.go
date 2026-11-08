package gatt

import (
	"context"
	"sync"

	"github.com/seangong/gatt/v2/xpc"
)

// xpcRequests serializes requests that have no protocol request identifier.
// Event handlers only deliver into the current buffered waiter; they never
// wait for a caller to consume a response.
type xpcRequests struct {
	once sync.Once
	gate chan struct{}
	mu   sync.Mutex
	wait *xpcResponse
}

type xpcResponse struct {
	id   int
	args chan xpc.Dict
}

func (q *xpcRequests) request(ctx context.Context, id int, send func() error) (xpc.Dict, error) {
	q.once.Do(func() { q.gate = make(chan struct{}, 1) })
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case q.gate <- struct{}{}:
	}
	defer func() { <-q.gate }()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}

	wait := &xpcResponse{id: id, args: make(chan xpc.Dict, 1)}
	q.mu.Lock()
	q.wait = wait
	q.mu.Unlock()
	defer func() {
		q.mu.Lock()
		q.wait = nil
		q.mu.Unlock()
	}()
	if err := send(); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case args := <-wait.args:
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		return args, nil
	}
}

func (q *xpcRequests) deliver(id int, args xpc.Dict) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.wait == nil || q.wait.id != id {
		return false
	}
	select {
	case q.wait.args <- args:
	default:
	}
	return true
}
