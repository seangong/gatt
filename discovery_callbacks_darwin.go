package gatt

import (
	"context"
	"errors"

	"github.com/seangong/gatt/v2/xpc"
)

const darwinDiscoveryConcurrency = 16
const darwinDiscoveryQueueSize = 16

var errDarwinDiscoveryQueueFull = errors.New("macOS discovery callback queue full")

type darwinDiscoveryCallback struct {
	f    func(Peripheral, *Advertisement, int)
	p    Peripheral
	a    *Advertisement
	rssi int
}

// Scan commands and local acceptance share a boundary. SendError only queues
// the native message; failures must terminate the session outside discoveryMu.
func (d *device) sendScanCommand(id int, args xpc.Dict) {
	d.discoveryMu.Lock()
	if context.Cause(d.ctx) != nil {
		d.discoveryMu.Unlock()
		return
	}
	err := d.conn.SendError(xpc.Dict{"kCBMsgId": id, "kCBMsgArgs": args}, false)
	if err == nil {
		d.discoveryPaused = id == 30
		if d.discoveryPaused {
			clear(d.discoveryQueue)
			d.discoveryQueue = nil
		}
	}
	d.discoveryMu.Unlock()
	if err != nil {
		d.fail(err)
	}
}

func (d *device) queueDiscoveryCallback(p Peripheral, a *Advertisement, rssi int) bool {
	d.discoveryMu.Lock()
	defer d.discoveryMu.Unlock()
	if d.discoveryPaused || context.Cause(d.ctx) != nil {
		return true
	}
	callback := darwinDiscoveryCallback{f: d.peripheralDiscovered, p: p, a: a, rssi: rssi}
	if d.discoveryActive < darwinDiscoveryConcurrency {
		d.startDiscoveryCallbackLocked(callback)
		return true
	}
	if len(d.discoveryQueue) == darwinDiscoveryQueueSize {
		// Do not admit more reports while the caller ends the session. Silent
		// loss could hide a peer when duplicate filtering is enabled.
		d.discoveryPaused = true
		clear(d.discoveryQueue)
		d.discoveryQueue = nil
		return false
	}
	d.discoveryQueue = append(d.discoveryQueue, callback)
	return true
}

// Admission reserves an allowance until the handler actually returns. It is
// never reset by StopScanning or Scan, including for handlers ignoring Close.
func (d *device) startDiscoveryCallbackLocked(callback darwinDiscoveryCallback) {
	d.discoveryActive++
	go func() {
		defer d.finishDiscoveryCallback()
		if context.Cause(d.ctx) == nil {
			callback.f(callback.p, callback.a, callback.rssi)
		}
	}()
}

func (d *device) finishDiscoveryCallback() {
	d.discoveryMu.Lock()
	defer d.discoveryMu.Unlock()
	d.discoveryActive--
	if d.discoveryPaused || context.Cause(d.ctx) != nil || len(d.discoveryQueue) == 0 {
		return
	}
	callback := d.discoveryQueue[0]
	d.discoveryQueue[0] = darwinDiscoveryCallback{}
	d.discoveryQueue = d.discoveryQueue[1:]
	d.startDiscoveryCallbackLocked(callback)
}

func (d *device) stopDiscoveryCallbacks() {
	d.discoveryMu.Lock()
	d.discoveryPaused = true
	clear(d.discoveryQueue)
	d.discoveryQueue = nil
	d.discoveryMu.Unlock()
}
