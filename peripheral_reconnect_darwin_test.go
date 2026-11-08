//go:build gattxpc_test

package gatt

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/seangong/gatt/v2/xpc"
)

func TestDarwinClientReconnectWithDisconnectedPeripheral(t *testing.T) {
	for _, mode := range []string{"remote", "cancel", "database-change"} {
		t.Run(mode, func(t *testing.T) {
			var cancels atomic.Int32
			barrier := make(chan struct{}, 1)
			d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				switch id {
				case 31:
					return peripheralConnected, xpc.Dict{}
				case 32:
					cancels.Add(1)
				case 65:
					return characteristicRead, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgData": []byte("new")}
				case 999:
					barrier <- struct{}{}
				}
				return 0, nil
			})
			service := &Service{h: 1, endh: 10}
			old.svcs = []*Service{service}
			old.sub.subscribe(2, func([]byte, error) {})
			type result struct {
				p   Peripheral
				err error
			}
			connected, disconnected := make(chan result, 2), make(chan result, 2)
			d.peripheralConnected = func(p Peripheral, err error) { connected <- result{p, err} }
			d.peripheralDisconnected = func(p Peripheral, err error) {
				disconnected <- result{p, err}
				if p == old {
					d.Connect(p)
				}
			}
			if mode == "database-change" {
				old.stop(ErrDatabaseChanged)
			}
			if mode != "remote" {
				d.CancelConnection(old)
			}
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": old.id}}, nil)
			ended := receiveDarwin(t, disconnected)
			if ended.p != old || (mode == "database-change" && !errors.Is(ended.err, ErrDatabaseChanged)) || (mode != "database-change" && ended.err != nil) {
				t.Fatalf("old disconnect = %v", ended)
			}
			got := receiveDarwin(t, connected)
			if got.err != nil || got.p == old {
				t.Fatalf("reconnect = %v, want a new live session", got)
			}
			live := got.p.(*peripheral)
			if context.Cause(live.context()) != nil || len(live.Services()) != 0 || live.sub.fn(2) != nil {
				t.Fatal("reconnect inherited the old session state")
			}
			if context.Cause(old.context()) == nil || len(old.Services()) != 1 || old.Services()[0] != service || old.sub.fn(2) == nil {
				t.Fatal("reconnect reset the retired session")
			}
			value, err := live.ReadCharacteristic(&Characteristic{h: 2, vh: 3})
			if err != nil || string(value) != "new" {
				t.Fatalf("read on reconnected session = %q, %v", value, err)
			}
			before := cancels.Load()
			d.CancelConnection(old)
			if err := d.sendCmd(999, xpc.Dict{}); err != nil {
				t.Fatal(err)
			}
			receiveDarwin(t, barrier)
			if cancels.Load() != before || context.Cause(live.context()) != nil {
				t.Fatal("retired object canceled the reconnected session")
			}
		})
	}
}

func TestDarwinClientReconnectCanCancelPendingWithOriginalArgument(t *testing.T) {
	commands := make(chan int, 3)
	d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	old.stop(ErrDatabaseChanged)
	d.plistmu.Lock()
	delete(d.plist, old.ID())
	d.plistmu.Unlock()
	disconnected := make(chan Peripheral, 1)
	d.peripheralDisconnected = func(p Peripheral, err error) {
		if err != nil {
			t.Errorf("new pending cancellation inherited old cause: %v", err)
		}
		disconnected <- p
	}
	d.Connect(old)
	if id := receiveDarwin(t, commands); id != 31 {
		t.Fatalf("connect command = %d", id)
	}
	d.CancelConnection(old)
	if id := receiveDarwin(t, commands); id != 32 {
		t.Fatalf("pending cancel command = %d", id)
	}
	// A late success must not replace a pending attempt already being canceled.
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": old.id}}, nil)
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": old.id}}, nil)
	if got := receiveDarwin(t, disconnected); got != old {
		t.Fatal("pending cancellation changed the caller's argument identity")
	}
}

func TestDarwinClientReconnectFailureReportsOriginalArgument(t *testing.T) {
	for _, synchronous := range []bool{false, true} {
		t.Run(map[bool]string{false: "session-error", true: "enqueue-error"}[synchronous], func(t *testing.T) {
			commands := make(chan int, 1)
			d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				commands <- id
				return 0, nil
			})
			old.stop(io.ErrClosedPipe)
			d.plistmu.Lock()
			delete(d.plist, old.ID())
			d.plistmu.Unlock()
			type result struct {
				p   Peripheral
				err error
			}
			connected, disconnected := make(chan result, 2), make(chan result, 1)
			d.peripheralConnected = func(p Peripheral, err error) { connected <- result{p, err} }
			d.peripheralDisconnected = func(p Peripheral, err error) { disconnected <- result{p, err} }
			if synchronous {
				// The fixture still owns and cleans up its native connection.
				// A zero connection forces a synchronous local enqueue failure.
				d.conn = xpc.XPC{}
			}
			d.Connect(old)
			if !synchronous {
				if id := receiveDarwin(t, commands); id != 31 {
					t.Fatalf("connect command = %d", id)
				}
				d.HandleXpcEvent(nil, xpc.CONNECTION_INTERRUPTED)
			}
			got := receiveDarwin(t, connected)
			if got.p != old || d.Err() == nil || !errors.Is(got.err, d.Err()) {
				t.Fatalf("pending failure = %v, want original argument and current session cause %v", got, d.Err())
			}
			_ = d.Close()
			select {
			case extra := <-connected:
				t.Fatalf("duplicate pending failure: %v", extra)
			case wrong := <-disconnected:
				t.Fatalf("pending failure reported as disconnection: %v", wrong)
			default:
			}
		})
	}
}

func TestDarwinClientReconnectBeforeOldStopPublishes(t *testing.T) {
	commands := make(chan int, 1)
	d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	// Model the native disconnection boundary: removal is published under the
	// registry lock, before stop cancels the old connection outside that lock.
	d.plistmu.Lock()
	delete(d.plist, old.ID())
	d.plistmu.Unlock()
	connected := make(chan Peripheral, 1)
	d.peripheralConnected = func(p Peripheral, err error) {
		if err != nil {
			t.Errorf("reconnect failed: %v", err)
		}
		connected <- p
	}
	d.Connect(old)
	if id := receiveDarwin(t, commands); id != 31 {
		t.Fatalf("connect command = %d", id)
	}
	old.stop(io.ErrClosedPipe)
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": old.id}}, nil)
	if live := receiveDarwin(t, connected); live == old || context.Cause(live.(*peripheral).context()) != nil {
		t.Fatal("old stop canceled the new attempt")
	}
}

func TestDarwinClientRepeatedConnectPreservesActiveSession(t *testing.T) {
	commands := make(chan int, 4)
	d, live := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	type readResult struct {
		value []byte
		err   error
	}
	reads := make(chan readResult, 1)
	read := func() {
		value, err := live.ReadCharacteristic(&Characteristic{h: 2, vh: 3})
		reads <- readResult{value, err}
	}
	go read()
	if id := receiveDarwin(t, commands); id != 65 {
		t.Fatalf("read command = %d", id)
	}
	d.Connect(live)
	if id := receiveDarwin(t, commands); id != 31 {
		t.Fatalf("repeated connect command = %d", id)
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(characteristicRead), "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": live.id, "kCBMsgArgResult": int64(0), "kCBMsgArgData": []byte("active"),
	}}, nil)
	if got := receiveDarwin(t, reads); got.err != nil || string(got.value) != "active" {
		t.Fatalf("read during repeated connect = %q, %v", got.value, got.err)
	}
	go read()
	if id := receiveDarwin(t, commands); id != 65 {
		t.Fatalf("second read command = %d", id)
	}
	d.CancelConnection(live)
	if id := receiveDarwin(t, commands); id != 32 {
		t.Fatalf("cancel command = %d", id)
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": live.id}}, nil)
	if got := receiveDarwin(t, reads); !errors.Is(got.err, io.ErrClosedPipe) || context.Cause(live.context()) == nil {
		t.Fatalf("active request after disconnection = %v", got.err)
	}
}
