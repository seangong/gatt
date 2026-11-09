//go:build gattxpc_test

package gatt

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

func TestDarwinServerATTHandlerCanStopAdvertising(t *testing.T) {
	for _, read := range []bool{false, true} {
		name := "write"
		if read {
			name = "read"
		}
		t.Run(name, func(t *testing.T) {
			d := newDarwinLifecycleTestDevice()
			stopped, replies := make(chan error, 1), make(chan xpc.Dict, 1)
			c := NewService(UUID16(0xff00)).AddCharacteristic(UUID16(0xff01))
			if read {
				c.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
					err := d.StopAdvertising()
					stopped <- err
					if err != nil {
						w.SetStatus(StatusUnexpectedError)
						return
					}
					_, _ = w.Write([]byte("done"))
				})
			} else {
				c.HandleWriteFunc(func(Request, []byte) byte {
					err := d.StopAdvertising()
					stopped <- err
					if err != nil {
						return StatusUnexpectedError
					}
					return StatusSuccess
				})
			}
			d.attrs[1] = &attr{h: 1, nativeID: 1, pvt: c}
			conn, cleanup := xpc.NewTestEchoConnection(darwinBridgeHandler(func(event xpc.Dict, err error) {
				if err != nil {
					d.HandleXpcEvent(nil, err)
					return
				}
				switch event.MustGetInt("kCBMsgId") {
				case 900:
					args, id := darwinRequestForTest(), 20
					if read {
						id = 19
						args["kCBMsgArgAttributeID"], args["kCBMsgArgOffset"] = int64(1), int64(0)
					} else {
						args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(1, 0, 0)}
					}
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(id), "kCBMsgArgs": args}, nil)
				case 9:
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(17), "kCBMsgArgs": xpc.Dict{"kCBMsgArgResult": int64(0)}}, nil)
				case 13:
					replies <- event.MustGetDict("kCBMsgArgs")
				}
			}))
			d.conn = conn
			t.Cleanup(func() { _ = d.Close(); cleanup() })
			if err := d.sendCmd(900, nil); err != nil {
				t.Fatal(err)
			}
			if err := receiveDarwin(t, stopped); err != nil {
				t.Fatalf("StopAdvertising from ATT handler = %v", err)
			}
			if got := receiveDarwin(t, replies); got.MustGetInt("kCBMsgArgResult") != 0 || read && string(got.MustGetBytes("kCBMsgArgData")) != "done" {
				t.Fatalf("ATT response after reentrant StopAdvertising = %v", got)
			}
		})
	}
}

func TestDarwinServerATTRequestsPreserveOrder(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	entered, release := make(chan struct{}), make(chan struct{})
	order := make(chan byte, 4)
	c := &Characteristic{}
	c.HandleWriteFunc(func(_ Request, b []byte) byte {
		if b[0] == 0 {
			close(entered)
			<-release
		}
		order <- b[0]
		return StatusSuccess
	})
	d.attrs[1] = &attr{h: 1, nativeID: 1, pvt: c}
	t.Cleanup(func() {
		_ = d.Close()
		select {
		case <-release:
		default:
			close(release)
		}
		receiveDarwin(t, d.attDone)
	})
	for i := 0; i < 4; i++ {
		args := darwinRequestForTest()
		args["kCBMsgArgTransactionID"] = int64(i + 1)
		write := darwinWriteForTest(1, 0, 0)
		write["kCBMsgArgData"] = []byte{byte(i)}
		args["kCBMsgArgATTWrites"] = xpc.Array{write}
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(20), "kCBMsgArgs": args}, nil)
		if i == 0 {
			receiveDarwin(t, entered)
		}
	}
	close(release)
	for i := 0; i < 4; i++ {
		if got := receiveDarwin(t, order); got != byte(i) {
			t.Fatalf("business request %d executed as %d", i, got)
		}
		response := receiveDarwin(t, messages).MustGetDict("kCBMsgArgs")
		if response.MustGetInt("kCBMsgArgTransactionID") != i+1 || response.MustGetInt("kCBMsgArgResult") != 0 {
			t.Fatalf("response %d = %v", i, response)
		}
	}
}

func TestDarwinServerFullATTQueueRejectsWithoutBusinessSideEffects(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	c := &Characteristic{}
	c.HandleWriteFunc(func(Request, []byte) byte {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return StatusSuccess
	})
	c.HandleReadFunc(func(ResponseWriter, *ReadRequest) { calls.Add(1) })
	d.attrs[1] = &attr{h: 1, nativeID: 1, pvt: c}
	d.attrs[2] = &attr{h: 2, nativeID: 2, value: []byte("static"), pvt: c}
	t.Cleanup(func() {
		_ = d.Close()
		close(release)
		receiveDarwin(t, d.attDone)
	})
	request := func(writes xpc.Array) {
		args := darwinRequestForTest()
		args["kCBMsgArgATTWrites"] = writes
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(20), "kCBMsgArgs": args}, nil)
	}
	request(xpc.Array{darwinWriteForTest(1, 0, 0)})
	receiveDarwin(t, entered)
	for i := 0; i < cap(d.attQueue); i++ {
		request(xpc.Array{darwinWriteForTest(1, 0, 1)})
	}
	for _, tc := range []struct {
		writes xpc.Array
		handle int
		code   attEcode
	}{
		{writes: xpc.Array{darwinWriteForTest(1, 0, 0)}, handle: 1, code: attEcodeInsuffResources},
		{writes: xpc.Array{darwinWriteForTest(1, 0, 1), darwinWriteForTest(2, 0, 0)}, handle: 2, code: attEcodeReqNotSupp},
		{writes: xpc.Array{darwinWriteForTest(1, 1, 0)}, handle: 1, code: attEcodeInvalidOffset},
		{writes: xpc.Array{darwinWriteForTest(99, 0, 0)}, handle: 99, code: attEcodeInvalidHandle},
	} {
		request(tc.writes)
		response := receiveDarwin(t, messages).MustGetDict("kCBMsgArgs")
		if response.MustGetInt("kCBMsgArgAttributeID") != tc.handle || response.MustGetInt("kCBMsgArgResult") != int(tc.code) {
			t.Fatalf("full-queue write response = %v, want handle %d code %d", response, tc.handle, tc.code)
		}
	}
	for _, id := range []int{1, 2, 99} {
		args := darwinRequestForTest()
		args["kCBMsgArgAttributeID"], args["kCBMsgArgOffset"] = int64(id), int64(0)
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(19), "kCBMsgArgs": args}, nil)
		response := receiveDarwin(t, messages).MustGetDict("kCBMsgArgs")
		want := attEcodeInsuffResources
		if id == 99 {
			want = attEcodeInvalidHandle
		}
		if response.MustGetInt("kCBMsgArgResult") != int(want) || len(response.GetBytes("kCBMsgArgData", nil)) != 0 {
			t.Fatalf("full-queue read response = %v, want code %d without value", response, want)
		}
	}
	request(xpc.Array{darwinWriteForTest(1, 0, 1)})
	if replies := darwinServerReplies(t, d, messages); len(replies) != 0 || calls.Load() != 1 {
		t.Fatalf("full queue ran business handlers or replied to a command: calls=%d replies=%v", calls.Load(), replies)
	}
}

func TestDarwinServerCloseDoesNotWaitForATTHandlerOrRunQueuedBusiness(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	c := &Characteristic{}
	c.HandleWriteFunc(func(Request, []byte) byte {
		if calls.Add(1) == 1 {
			close(entered)
			<-release // This handler deliberately does not cooperate with cancellation.
		}
		return StatusSuccess
	})
	d.attrs[1] = &attr{h: 1, nativeID: 1, pvt: c}
	t.Cleanup(func() {
		_ = d.Close()
		select {
		case <-release:
		default:
			close(release)
		}
		receiveDarwin(t, d.attDone)
	})
	for i := 0; i < 4; i++ {
		args := darwinRequestForTest()
		args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(1, 0, 0)}
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(20), "kCBMsgArgs": args}, nil)
		if i == 0 {
			receiveDarwin(t, entered)
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	if err := receiveDarwin(t, closed); err != nil {
		t.Fatal(err)
	}
	select {
	case <-d.attDone:
		t.Fatal("worker exited before the uncooperative handler returned")
	default:
	}
	close(release)
	receiveDarwin(t, d.attDone)
	if calls.Load() != 1 {
		t.Fatalf("Close allowed %d queued handlers to run", calls.Load()-1)
	}
	select {
	case reply := <-messages:
		t.Fatalf("handler sent an ATT response after Close: %v", reply)
	default:
	}
}

func TestDarwinServerATTResponseEnqueueOrdersWithClose(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	paused := &pauseDarwinCauseContext{Context: d.ctx, entered: make(chan struct{}), release: make(chan struct{})}
	paused.paused.Store(true)
	d.ctx = paused
	defer func() {
		select {
		case <-paused.release:
		default:
			close(paused.release)
		}
	}()
	c := &Characteristic{}
	c.HandleWriteFunc(func(Request, []byte) byte {
		// Pause the next cancellation check, after the active handler returns
		// and while its response owns the enqueue boundary.
		paused.paused.Store(false)
		return StatusSuccess
	})
	d.attrs[1] = &attr{h: 1, nativeID: 1, pvt: c}
	args := darwinRequestForTest()
	args["kCBMsgArgTransactionID"] = int64(1)
	args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(1, 0, 0)}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(20), "kCBMsgArgs": args}, nil)
	receiveDarwin(t, paused.entered)
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	select {
	case <-d.Done():
		t.Fatal("Close overtook the admitted ATT response enqueue")
	case <-time.After(100 * time.Millisecond):
	}
	close(paused.release)
	if err := receiveDarwin(t, closed); err != nil || !errors.Is(d.Err(), xpc.ErrClosed) {
		t.Fatalf("Close = %v, session cause = %v", err, d.Err())
	}
	receiveDarwin(t, d.attDone)
	// An admitted enqueue may precede Close. Subsequent responses are rejected.
	d.respondATT(1, 2, nil, attEcodeSuccess)
	for {
		select {
		case event := <-messages:
			if event.MustGetDict("kCBMsgArgs").MustGetInt("kCBMsgArgTransactionID") != 1 {
				t.Fatalf("response enqueued after Close: %v", event)
			}
		default:
			return
		}
	}
}

func TestDarwinServerRemoveServicesKeepsActiveATTRequestAndRejectsQueued(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	c := &Characteristic{}
	c.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		_, _ = w.Write([]byte("active"))
	})
	d.attrs[1] = &attr{h: 1, nativeID: 1, pvt: c}
	t.Cleanup(func() {
		_ = d.Close()
		select {
		case <-release:
		default:
			close(release)
		}
		receiveDarwin(t, d.attDone)
	})
	for _, transaction := range []int64{1, 2} {
		args := darwinRequestForTest()
		args["kCBMsgArgTransactionID"] = transaction
		args["kCBMsgArgAttributeID"], args["kCBMsgArgOffset"] = int64(1), int64(0)
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(19), "kCBMsgArgs": args}, nil)
		if transaction == 1 {
			receiveDarwin(t, entered)
		}
	}
	if err := d.RemoveAllServices(); err != nil {
		t.Fatal(err)
	}
	if event := receiveDarwin(t, messages); event.MustGetInt("kCBMsgId") != 12 {
		t.Fatalf("service removal command = %v", event)
	}
	close(release)
	for _, transaction := range []int{1, 2} {
		response := receiveDarwin(t, messages).MustGetDict("kCBMsgArgs")
		want := attEcodeSuccess
		if transaction == 2 {
			want = attEcodeInvalidHandle
		}
		if response.MustGetInt("kCBMsgArgTransactionID") != transaction || response.MustGetInt("kCBMsgArgResult") != int(want) {
			t.Fatalf("request %d after removal = %v", transaction, response)
		}
		if transaction == 1 && string(response.MustGetBytes("kCBMsgArgData")) != "active" {
			t.Fatal("service removal interrupted the active handler")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("removed service ran %d handlers", calls.Load())
	}
}
