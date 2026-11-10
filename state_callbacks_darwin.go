package gatt

import (
	"context"
	"errors"

	"github.com/seangong/gatt/v2/xpc"
)

const darwinStateQueueSize = 16

var errDarwinStateQueueFull = errors.New("macOS state callback queue full")

type darwinStateCallback struct {
	state State
	f     func(Device, State)
	ready <-chan struct{}
}

// State receipt, registration and FIFO publication share stateMu. The worker
// never holds it while waiting for Init or calling application code.
func (d *device) queueStateCallback(args xpc.Dict) bool {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	if context.Cause(d.ctx) != nil || d.stateChanged == nil {
		return true
	}
	if len(d.stateQueue) == darwinStateQueueSize {
		return false
	}
	d.stateQueue = append(d.stateQueue, darwinStateCallback{
		state: State(args.MustGetInt("kCBMsgArgState")), f: d.stateChanged, ready: d.stateReady,
	})
	if !d.stateRunning {
		d.stateRunning = true
		go d.runStateCallbacks()
	}
	return true
}

func (d *device) stopStateCallbacks() {
	d.stateMu.Lock()
	clear(d.stateQueue)
	d.stateQueue = nil
	d.stateMu.Unlock()
}

func (d *device) runStateCallbacks() {
	for {
		d.stateMu.Lock()
		if context.Cause(d.ctx) != nil || len(d.stateQueue) == 0 {
			clear(d.stateQueue)
			d.stateQueue = nil
			d.stateRunning = false
			d.stateMu.Unlock()
			return
		}
		callback := d.stateQueue[0]
		d.stateQueue[0] = darwinStateCallback{}
		d.stateQueue = d.stateQueue[1:]
		d.stateMu.Unlock()
		if callback.ready != nil {
			select {
			case <-callback.ready:
			case <-d.ctx.Done():
				continue
			}
		}
		if context.Cause(d.ctx) == nil {
			callback.f(d, callback.state)
		}
	}
}
