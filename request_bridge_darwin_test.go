//go:build gattxpc_test

package gatt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

type darwinBridgeHandler func(xpc.Dict, error)

func (f darwinBridgeHandler) HandleXpcEvent(event xpc.Dict, err error) { f(event, err) }

func TestDarwinAdapterNativeRequestsAndResponses(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	ctx, stop := context.WithTimeout(d.ctx, 3*time.Second)
	defer stop()
	d.ctx = ctx
	d.ready = make(chan struct{})
	commands := make(chan xpc.Dict, 8)
	// The peer echoes through the native C bridge in both directions. This
	// handler supplies the corresponding Bluetooth event identifiers.
	conn, cleanup := xpc.NewTestEchoConnection(darwinBridgeHandler(func(event xpc.Dict, err error) {
		if err != nil {
			d.HandleXpcEvent(nil, err)
			return
		}
		id := event.MustGetInt("kCBMsgId")
		args := event.MustGetDict("kCBMsgArgs")
		commands <- xpc.Dict{"kCBMsgId": event["kCBMsgId"], "kCBMsgArgs": args}
		args["kCBMsgArgResult"] = int64(0)
		switch id {
		case 1:
			event["kCBMsgId"] = int64(6)
			args["kCBMsgArgState"] = int64(StatePoweredOn)
		case 8:
			event["kCBMsgId"] = int64(16)
		case 9:
			event["kCBMsgId"] = int64(17)
		case 10:
			event["kCBMsgId"] = int64(18)
		case 65:
			event["kCBMsgId"] = int64(characteristicRead)
			args["kCBMsgArgData"] = []byte("value")
		case 66:
			event["kCBMsgId"] = int64(characteristicWritten)
		case 68:
			event["kCBMsgId"] = int64(notificationValueSet)
		default:
			t.Errorf("unexpected command %d", id)
			_ = d.Close()
			return
		}
		d.HandleXpcEvent(event, nil)
	}))
	d.conn = conn
	close(d.ready)
	defer cleanup()
	defer d.Close()
	stateChanged := make(chan State, 1)
	if err := d.Init(func(_ Device, state State) { stateChanged <- state }); err != nil {
		t.Fatal(err)
	}
	select {
	case state := <-stateChanged:
		if state != StatePoweredOn {
			t.Fatalf("initial state = %v", state)
		}
	case <-ctx.Done():
		t.Fatal("initial state callback missing")
	}
	receiveDarwin(t, commands)
	svc := NewService(UUID16(0xfff0))
	svc.AddCharacteristic(UUID16(0xfff1)).SetValue([]byte("ready"))
	if err := d.AddService(svc); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, commands)
	p := &peripheral{d: d, id: xpc.MakeUUID("00112233445566778899aabbccddeeff"), sub: newSubscriber()}
	p.ctx, p.cancel = context.WithCancelCause(d.ctx)
	d.plist[p.ID()] = p
	c := &Characteristic{h: 2, vh: 3}
	if err := p.WriteCharacteristic(c, []byte("command"), false); err != nil {
		t.Fatal(err)
	}
	write := receiveDarwin(t, commands)
	if write.MustGetInt("kCBMsgId") != 66 || write.MustGetDict("kCBMsgArgs").MustGetInt("kCBMsgArgType") != 0 {
		t.Fatalf("acknowledged write command = %v", write)
	}
	b, err := p.ReadCharacteristic(c)
	if err != nil || string(b) != "value" {
		t.Fatalf("read = %q, %v", b, err)
	}
	receiveDarwin(t, commands)
	if err := d.AdvertiseNameAndServices("test", nil); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, commands)
	if err := d.StopAdvertising(); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, commands)
	if err := p.SetNotifyValue(c, func(*Characteristic, []byte, error) {}); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, commands)
	if err := p.SetNotifyValue(c, nil); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, commands)
}

func TestDarwinAdapterCloseFromNativeCallback(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	ctx, stop := context.WithTimeout(d.ctx, 3*time.Second)
	defer stop()
	d.ctx = ctx
	d.ready = make(chan struct{})
	var sends atomic.Int32
	conn, cleanup := xpc.NewTestEchoConnection(darwinBridgeHandler(func(event xpc.Dict, err error) {
		if err != nil {
			d.HandleXpcEvent(nil, err)
			return
		}
		sends.Add(1)
		_ = d.Close()
	}))
	d.conn = conn
	close(d.ready)
	defer cleanup()
	result := make(chan error, 1)
	go func() { result <- d.AdvertiseNameAndServices("test", nil) }()
	if err := receiveDarwin(t, result); !errors.Is(err, xpc.ErrClosed) {
		t.Fatalf("advertise canceled by native callback = %v", err)
	}
	if sends.Load() != 1 {
		t.Fatalf("sent %d commands", sends.Load())
	}
}

func TestDarwinIdleScanReportsSessionTermination(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	d.ready = make(chan struct{})
	var public Device = d
	session, ok := public.(interface {
		Done() <-chan struct{}
		Err() error
	})
	if !ok {
		t.Fatal("macOS device does not expose session termination")
	}
	// A scan sends no request with a response waiter. Terminate the session
	// from the native callback, with no connected peripheral to report it.
	conn, cleanup := xpc.NewTestEchoConnection(darwinBridgeHandler(func(event xpc.Dict, err error) {
		if err != nil {
			d.HandleXpcEvent(nil, err)
			return
		}
		if event.MustGetInt("kCBMsgId") != 29 {
			t.Error("unexpected command during scan")
			_ = d.Close()
			return
		}
		d.HandleXpcEvent(nil, xpc.CONNECTION_INTERRUPTED)
	}))
	d.conn = conn
	close(d.ready)
	defer cleanup()
	defer d.Close()
	public.Scan(nil, false)
	receiveDarwin(t, session.Done())
	if !errors.Is(session.Err(), xpc.CONNECTION_INTERRUPTED) {
		t.Fatalf("scan session termination = %v", session.Err())
	}
	// Void operations still return promptly after termination. Callers can
	// continue to observe the original failure through the optional interface.
	public.StopScanning()
	public.Scan(nil, false)
	if !errors.Is(session.Err(), xpc.CONNECTION_INTERRUPTED) {
		t.Fatalf("later operation replaced the scan failure: %v", session.Err())
	}
}
