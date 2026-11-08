//go:build gattxpc_test

package xpc

/*
#include <dispatch/dispatch.h>
#include "xpc_wrapper_darwin.h"
extern void handleXpcEvent(xpc_object_t, uintptr_t);

struct XpcTestPair {
    xpc_connection_t listener;
    xpc_connection_t peer;
    dispatch_queue_t queue;
};

// An anonymous endpoint stays entirely in this process. No named service or
// Bluetooth daemon is contacted by these tests.
static struct XpcTestPair XpcTestCreate(uintptr_t token) {
    dispatch_queue_t queue = dispatch_queue_create("gatt.xpc.test", DISPATCH_QUEUE_SERIAL);
    xpc_connection_t listener = xpc_connection_create(NULL, queue);
    xpc_connection_set_event_handler(listener, ^(xpc_object_t event) {
        if (xpc_get_type(event) != XPC_TYPE_CONNECTION) {
            return;
        }
        xpc_connection_t remote = (xpc_connection_t)event;
        xpc_retain(remote);
        xpc_connection_set_event_handler(remote, ^(xpc_object_t message) {
            if (message == XPC_ERROR_CONNECTION_INVALID) {
                xpc_release(remote);
            } else if (xpc_get_type(message) == XPC_TYPE_DICTIONARY) {
                xpc_connection_send_message(remote, message);
            }
        });
        xpc_connection_resume(remote);
    });
    xpc_connection_resume(listener);
    xpc_endpoint_t endpoint = xpc_endpoint_create(listener);
    xpc_connection_t peer = xpc_connection_create_from_endpoint(endpoint);
    xpc_release(endpoint);
    xpc_connection_set_target_queue(peer, queue);
    if (!XpcSetHandler(peer, token)) {
        abort();
    }
    return (struct XpcTestPair){listener, peer, queue};
}

// Exercise the C->Go callback with the same native reference lifetime as a
// queued event, including when Close runs before this callback finishes.
static void XpcTestEmit(xpc_connection_t peer, dispatch_queue_t queue,
        uintptr_t token, xpc_object_t event) {
    xpc_retain(peer);
    xpc_retain(event);
    dispatch_async(queue, ^{
        handleXpcEvent(event, token);
        xpc_release(event);
        xpc_release(peer);
    });
}

static void XpcTestDestroy(struct XpcTestPair pair) {
    xpc_connection_cancel(pair.listener);
    xpc_release(pair.listener);
    dispatch_release(pair.queue);
}
*/
import "C"

import "runtime/cgo"

type connectionTestPair struct {
	x     XPC
	pair  C.struct_XpcTestPair
	token cgo.Handle
}

func newConnectionTestPair(handler XpcEventHandler) *connectionTestPair {
	return newTestPair(handler, false)
}

// NewTestEchoConnection exposes the in-process peer to adapter integration
// tests. This harness is excluded from builds without the gattxpc_test tag.
func NewTestEchoConnection(handler XpcEventHandler) (XPC, func()) {
	pair := newConnectionTestPair(handler)
	return pair.x, pair.close
}

func newInvalidConnectionTestPair(handler XpcEventHandler) *connectionTestPair {
	return newTestPair(handler, true)
}

func newTestPair(handler XpcEventHandler, invalid bool) *connectionTestPair {
	s, handle := newConnectionState(handler)
	pair := C.XpcTestCreate(C.uintptr_t(handle))
	if invalid {
		C.xpc_connection_cancel(pair.peer)
	}
	x := activateConnection(s, handle, pair.peer)
	return &connectionTestPair{x: x, pair: pair, token: handle}
}

func (p *connectionTestPair) emit(event Dict, err error) {
	p.x.state.mu.Lock()
	defer p.x.state.mu.Unlock()
	if p.x.state.conn == nil {
		return
	}
	var value C.xpc_object_t
	switch err {
	case CONNECTION_INVALID:
		value = C.ERROR_CONNECTION_INVALID
	case CONNECTION_INTERRUPTED:
		value = C.ERROR_CONNECTION_INTERRUPTED
	case CONNECTION_TERMINATED:
		value = C.ERROR_CONNECTION_TERMINATED
	default:
		value = goToXpc(event)
		defer C.xpc_release(value)
	}
	C.XpcTestEmit(p.pair.peer, p.pair.queue, C.uintptr_t(p.token), value)
}

func (p *connectionTestPair) close() {
	_ = p.x.Close()
	C.XpcTestDestroy(p.pair)
}

func failedTestConnection(handler XpcEventHandler) XPC {
	s, handle := newConnectionState(handler)
	return activateConnection(s, handle, nil)
}
