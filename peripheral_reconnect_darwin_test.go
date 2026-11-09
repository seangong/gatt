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
	for _, kind := range []string{"live", "scan-alias", "new-peripheral", "retired-alias"} {
		t.Run(kind, func(t *testing.T) { testDarwinRepeatedConnectPreservesActiveSession(t, kind) })
	}
}

func testDarwinRepeatedConnectPreservesActiveSession(t *testing.T, kind string) {
	t.Helper()
	commands := make(chan int, 4)
	d, live := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	argument := live
	switch kind {
	case "scan-alias":
		argument = &peripheral{id: live.id, d: d}
	case "new-peripheral":
		argument = NewPeripheral(UUID{live.id}).(*peripheral)
	case "retired-alias":
		argument = &peripheral{id: live.id, d: d}
		argument.ctx, argument.cancel = context.WithCancelCause(d.ctx)
		argument.stop(io.ErrClosedPipe)
	}
	service := &Service{h: 1, endh: 10}
	live.svcs = []*Service{service}
	notified := make(chan string, 1)
	live.sub.subscribe(2, func(b []byte, _ error) { notified <- string(b) })
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
	d.Connect(argument)
	if id := receiveDarwin(t, commands); id != 31 {
		t.Fatalf("repeated connect command = %d", id)
	}
	d.plistmu.Lock()
	current := d.plist[live.ID()]
	d.plistmu.Unlock()
	if current != live || len(live.Services()) != 1 || live.Services()[0] != service {
		t.Fatal("repeated connect replaced the active session or its cache")
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(characteristicRead), "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": live.id, "kCBMsgArgResult": int64(0), "kCBMsgArgData": []byte("active"),
	}}, nil)
	if got := receiveDarwin(t, reads); got.err != nil || string(got.value) != "active" {
		t.Fatalf("read during repeated connect = %q, %v", got.value, got.err)
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(characteristicRead), "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": live.id, "kCBMsgArgIsNotification": int64(1),
		"kCBMsgArgCharacteristicHandle": int64(2), "kCBMsgArgData": []byte("notification"),
	}}, nil)
	if got := receiveDarwin(t, notified); got != "notification" {
		t.Fatalf("notification after repeated connect = %q", got)
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

func TestDarwinClientRepeatedConnectedEventPreservesActiveSession(t *testing.T) {
	commands := make(chan int, 2)
	d, live := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	connected := make(chan Peripheral, 2)
	d.peripheralConnected = func(p Peripheral, _ error) { connected <- p }
	reads := make(chan error, 1)
	go func() {
		_, err := live.ReadCharacteristic(&Characteristic{h: 2, vh: 3})
		reads <- err
	}()
	if id := receiveDarwin(t, commands); id != 65 {
		t.Fatalf("read command = %d", id)
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": live.id}}, nil)
	d.plistmu.Lock()
	current := d.plist[live.ID()]
	d.plistmu.Unlock()
	if current != live {
		t.Fatal("duplicate connected event replaced the active owner")
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(characteristicRead), "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": live.id, "kCBMsgArgResult": int64(0), "kCBMsgArgData": []byte("active"),
	}}, nil)
	if err := receiveDarwin(t, reads); err != nil {
		t.Fatalf("read after duplicate connected event = %v", err)
	}
	select {
	case p := <-connected:
		t.Fatalf("duplicate connected event reported another connection: %s", p.ID())
	default:
	}
}

func TestDarwinClientRepeatedConnectPreservesPendingSource(t *testing.T) {
	for _, reconnect := range []bool{false, true} {
		for _, outcome := range []string{"success", "cancel", "session-error", "enqueue-error"} {
			t.Run(map[bool]string{false: "scan", true: "reconnect"}[reconnect]+"/"+outcome, func(t *testing.T) {
				commands := make(chan int, 5)
				d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
					commands <- id
					return 0, nil
				})
				d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": old.id}}, nil)
				source := old
				if !reconnect {
					source = &peripheral{id: old.id, d: d}
				}
				type result struct {
					p   Peripheral
					err error
				}
				connected, disconnected := make(chan result, 3), make(chan result, 2)
				d.peripheralConnected = func(p Peripheral, err error) { connected <- result{p, err} }
				d.peripheralDisconnected = func(p Peripheral, err error) { disconnected <- result{p, err} }
				d.Connect(source)
				if id := receiveDarwin(t, commands); id != 31 {
					t.Fatalf("initial connect command = %d", id)
				}
				d.plistmu.Lock()
				pending := d.plist[source.ID()]
				d.plistmu.Unlock()
				alias := &peripheral{id: source.id, d: d}
				retired := &peripheral{id: source.id, d: d}
				retired.ctx, retired.cancel = context.WithCancelCause(d.ctx)
				retired.stop(io.ErrClosedPipe)
				for _, argument := range []*peripheral{source, alias, retired} {
					d.Connect(argument)
					if id := receiveDarwin(t, commands); id != 31 {
						t.Fatalf("repeated connect command = %d", id)
					}
					d.plistmu.Lock()
					current := d.plist[source.ID()]
					d.plistmu.Unlock()
					if current != pending || reconnect && current.connectSource != source {
						t.Fatal("repeated connect replaced the pending owner or its original source")
					}
				}
				switch outcome {
				case "success":
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
					got := receiveDarwin(t, connected)
					if got.err != nil || got.p == source || context.Cause(got.p.(*peripheral).context()) != nil {
						t.Fatalf("connection result = %+v", got)
					}
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
					d.plistmu.Lock()
					current := d.plist[source.ID()]
					d.plistmu.Unlock()
					if current != got.p {
						t.Fatal("duplicate connected event replaced the published connection")
					}
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
					if ended := receiveDarwin(t, disconnected); ended.p != got.p || ended.err != nil || context.Cause(current.context()) == nil {
						t.Fatalf("disconnection result = %+v", ended)
					}
				case "cancel":
					d.CancelConnection(source)
					if id := receiveDarwin(t, commands); id != 32 {
						t.Fatalf("pending cancel command = %d", id)
					}
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
					if ended := receiveDarwin(t, disconnected); ended.p != source || ended.err != nil {
						t.Fatalf("pending cancellation result = %+v", ended)
					}
				default:
					cause := xpc.CONNECTION_INTERRUPTED
					if outcome == "enqueue-error" {
						cause = xpc.ErrClosed
						d.conn = xpc.XPC{}
						d.Connect(alias)
					} else {
						d.HandleXpcEvent(nil, cause)
					}
					if got := receiveDarwin(t, connected); got.p != source || !errors.Is(got.err, cause) {
						t.Fatalf("failed pending connection result = %+v", got)
					}
				}
				select {
				case got := <-connected:
					t.Fatalf("unexpected additional connection callback: %+v", got)
				case got := <-disconnected:
					t.Fatalf("unexpected additional disconnection callback: %+v", got)
				default:
				}
			})
		}
	}
}

func TestDarwinClientReconnectAfterPendingCancellation(t *testing.T) {
	for _, outcome := range []string{"success", "cancel", "session-error", "enqueue-error"} {
		t.Run(outcome, func(t *testing.T) {
			commands := make(chan int, 3)
			d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				commands <- id
				return 0, nil
			})
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": old.id}}, nil)
			source := &peripheral{id: old.id, d: d}
			d.Connect(source)
			if id := receiveDarwin(t, commands); id != 31 {
				t.Fatalf("initial connect command = %d", id)
			}
			d.CancelConnection(source)
			if id := receiveDarwin(t, commands); id != 32 {
				t.Fatalf("initial cancel command = %d", id)
			}
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
			type result struct {
				p   Peripheral
				err error
			}
			connected, disconnected := make(chan result, 2), make(chan result, 2)
			d.peripheralConnected = func(p Peripheral, err error) { connected <- result{p, err} }
			d.peripheralDisconnected = func(p Peripheral, err error) { disconnected <- result{p, err} }
			d.Connect(source)
			if id := receiveDarwin(t, commands); id != 31 {
				t.Fatalf("reconnect command = %d", id)
			}
			d.plistmu.Lock()
			pending := d.plist[source.ID()]
			d.plistmu.Unlock()
			if pending == source || pending == nil || pending.connectSource != source || pending.closing || !source.closing || source.cancel != nil {
				t.Fatal("reconnect reused or reset the canceled pending object")
			}
			switch outcome {
			case "success":
				d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
				got := receiveDarwin(t, connected)
				if got.err != nil || got.p == source || context.Cause(got.p.(*peripheral).context()) != nil {
					t.Fatalf("reconnect after pending cancellation = %+v", got)
				}
			case "cancel":
				d.CancelConnection(source)
				if id := receiveDarwin(t, commands); id != 32 {
					t.Fatalf("reconnect cancel command = %d", id)
				}
				d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
				d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": source.id}}, nil)
				if got := receiveDarwin(t, disconnected); got.p != source || got.err != nil {
					t.Fatalf("reconnect cancellation result = %+v", got)
				}
			default:
				cause := xpc.CONNECTION_INTERRUPTED
				if outcome == "enqueue-error" {
					cause = xpc.ErrClosed
					d.conn = xpc.XPC{}
					d.Connect(source)
				} else {
					d.HandleXpcEvent(nil, cause)
				}
				if got := receiveDarwin(t, connected); got.p != source || !errors.Is(got.err, cause) {
					t.Fatalf("failed reconnect result = %+v", got)
				}
			}
			select {
			case got := <-connected:
				t.Fatalf("unexpected additional connection callback: %+v", got)
			case got := <-disconnected:
				t.Fatalf("unexpected additional disconnection callback: %+v", got)
			default:
			}
		})
	}
}

func TestDarwinClientDiscoveryAndConnectionPreserveName(t *testing.T) {
	for _, tc := range []struct {
		name, advertised, fallback, connected, want string
	}{
		{name: "advertised", advertised: "Printer", want: "Printer"},
		{name: "fallback", fallback: "GAP name", want: "GAP name"},
		{name: "native connection name", advertised: "Printer", connected: "Printer 2", want: "Printer 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				if id == 31 {
					return peripheralConnected, xpc.Dict{"kCBMsgArgName": tc.connected}
				}
				return 0, nil
			})
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": old.id}}, nil)
			discovered, connected := make(chan Peripheral, 1), make(chan Peripheral, 1)
			d.peripheralDiscovered = func(p Peripheral, _ *Advertisement, _ int) { discovered <- p }
			d.peripheralConnected = func(p Peripheral, err error) {
				if err != nil {
					t.Errorf("connection failed: %v", err)
				}
				connected <- p
			}
			advertisement := xpc.Dict{"kCBAdvDataTxPowerLevel": int64(0)}
			if tc.advertised != "" {
				advertisement["kCBAdvDataLocalName"] = tc.advertised
			}
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDiscovered), "kCBMsgArgs": xpc.Dict{
				"kCBMsgArgDeviceUUID": old.id, "kCBMsgArgRssi": int64(-40),
				"kCBMsgArgName": tc.fallback, "kCBMsgArgAdvertisementData": advertisement,
			}}, nil)
			source := receiveDarwin(t, discovered)
			name := tc.advertised
			if name == "" {
				name = tc.fallback
			}
			if got := source.Name(); got != name {
				t.Errorf("discovered name = %q, want %q", got, name)
			}
			d.Connect(source)
			if got := receiveDarwin(t, connected); got == source || got.Name() != tc.want || source.Name() != name {
				t.Fatalf("connected name = %q, want %q; source name = %q", got.Name(), tc.want, source.Name())
			}
		})
	}
}
