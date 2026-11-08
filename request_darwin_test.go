package gatt

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

func receiveDarwin[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Darwin request")
		var zero T
		return zero
	}
}

func TestXPCRequestsSynchronousResponseAndUnsolicitedEvents(t *testing.T) {
	var requests xpcRequests
	if requests.deliver(16, nil) {
		t.Fatal("accepted a response with no pending request")
	}
	args, err := requests.request(context.Background(), 16, func() error {
		if requests.deliver(6, nil) {
			t.Fatal("unrelated state event completed advertising")
		}
		if !requests.deliver(16, xpc.Dict{"value": int64(1)}) {
			t.Fatal("response was not accepted")
		}
		// A repeated delivery must not block the native event handler or replace
		// the response already delivered into the single waiter.
		requests.deliver(16, xpc.Dict{"value": int64(2)})
		return nil
	})
	if err != nil || args.MustGetInt("value") != 1 {
		t.Fatalf("response = %v, %v", args, err)
	}
	if requests.deliver(16, nil) {
		t.Fatal("retained the completed waiter")
	}
}

func TestXPCRequestsSerializeConcurrentCalls(t *testing.T) {
	var requests xpcRequests
	const count = 12
	started := make(chan int, count)
	results := make(chan error, count)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sends atomic.Int32
	for i := 0; i < count; i++ {
		go func(i int) {
			args, err := requests.request(ctx, 71, func() error {
				sends.Add(1)
				started <- i
				return nil
			})
			if err == nil && args.MustGetInt("value") != i {
				err = errors.New("response delivered to another caller")
			}
			results <- err
		}(i)
	}
	for i := 0; i < count; i++ {
		id := receiveDarwin(t, started)
		if got := sends.Load(); got != int32(i+1) {
			t.Fatalf("sent %d requests before response %d", got, i+1)
		}
		requests.deliver(71, xpc.Dict{"value": int64(id)})
	}
	for i := 0; i < count; i++ {
		if err := receiveDarwin(t, results); err != nil {
			t.Fatal(err)
		}
	}
}

func TestXPCRequestsCancelPendingAndQueued(t *testing.T) {
	var requests xpcRequests
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("transport interrupted")
	started := make(chan struct{}, 1)
	results := make(chan error, 2)
	go func() {
		_, err := requests.request(ctx, 71, func() error { started <- struct{}{}; return nil })
		results <- err
	}()
	receiveDarwin(t, started)
	var queuedSends atomic.Int32
	go func() {
		_, err := requests.request(ctx, 71, func() error { queuedSends.Add(1); return nil })
		results <- err
	}()
	cancel(cause)
	for i := 0; i < 2; i++ {
		if err := receiveDarwin(t, results); !errors.Is(err, cause) {
			t.Fatalf("request error = %v, want %v", err, cause)
		}
	}
	if queuedSends.Load() != 0 || requests.deliver(71, nil) {
		t.Fatal("sent a queued request or retained a waiter after cancellation")
	}
}

func TestXPCRequestsPreserveCancellationCauseDuringSend(t *testing.T) {
	var requests xpcRequests
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("original interruption")
	_, err := requests.request(ctx, 16, func() error {
		cancel(cause)
		return xpc.ErrClosed
	})
	if !errors.Is(err, cause) {
		t.Fatalf("request error = %v, want original cause", err)
	}
}

func newDarwinLifecycleTestDevice() *device {
	ctx, cancel := context.WithCancelCause(context.Background())
	ready := make(chan struct{})
	close(ready)
	return &device{
		ctx: ctx, cancel: cancel, ready: ready,
		plist: make(map[string]*peripheral), plistmu: &sync.Mutex{},
		attrs: make(map[int]*attr), subscribers: make(map[string]*central),
	}
}

func TestDarwinSessionTerminationNotification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{"interrupted", xpc.CONNECTION_INTERRUPTED},
		{"invalid", xpc.CONNECTION_INVALID},
		{"terminated", xpc.CONNECTION_TERMINATED},
		{"closed", xpc.ErrClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newDarwinLifecycleTestDevice()
			defer d.Close()
			var public Device = d
			session, ok := public.(interface {
				Done() <-chan struct{}
				Err() error
			})
			if !ok {
				t.Fatal("macOS device does not expose session termination")
			}
			done := session.Done()
			if done == nil || session.Err() != nil {
				t.Fatal("live session reported termination")
			}
			select {
			case <-done:
				t.Fatal("live session Done is closed")
			default:
			}
			// No pending requests or connected peripherals can forward this error.
			if tc.cause == xpc.ErrClosed {
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				d.HandleXpcEvent(nil, tc.cause)
			}
			receiveDarwin(t, done)
			if !errors.Is(session.Err(), tc.cause) {
				t.Fatalf("session error = %v, want %v", session.Err(), tc.cause)
			}
			// Observers attaching after termination see the same persistent signal.
			if session.Done() != done {
				t.Fatal("session changed its Done channel")
			}
			receiveDarwin(t, session.Done())
			d.HandleXpcEvent(nil, xpc.CONNECTION_INVALID)
			_ = d.Close()
			if !errors.Is(session.Err(), tc.cause) {
				t.Fatalf("later cleanup replaced the first session error: %v", session.Err())
			}
		})
	}
}

func TestDarwinSessionTerminationNotifiesAllObservers(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	defer d.Close()
	var public Device = d
	session, ok := public.(interface {
		Done() <-chan struct{}
		Err() error
	})
	if !ok {
		t.Fatal("macOS device does not expose session termination")
	}
	const count = 8
	ready := make(chan struct{}, count)
	results := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			done := session.Done()
			ready <- struct{}{}
			<-done
			results <- session.Err()
		}()
	}
	for i := 0; i < count; i++ {
		receiveDarwin(t, ready)
	}
	d.HandleXpcEvent(nil, xpc.CONNECTION_INTERRUPTED)
	var closers sync.WaitGroup
	for i := 0; i < count; i++ {
		closers.Go(func() { _ = d.Close() })
	}
	for i := 0; i < count; i++ {
		if err := receiveDarwin(t, results); !errors.Is(err, xpc.CONNECTION_INTERRUPTED) {
			t.Fatalf("observer error = %v", err)
		}
	}
	closers.Wait()
}

func TestDarwinInterruptionCancelsSessionAndRetainsFirstCause(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	p := &peripheral{d: d, id: xpc.MakeUUID("00112233445566778899aabbccddeeff"), sub: newSubscriber()}
	p.ctx, p.cancel = context.WithCancelCause(d.ctx)
	d.plist[p.ID()] = p
	c := newCentral(d, UUID{p.id})
	n := newNotifier(c, &attr{h: 1}, 20)
	c.notifiers[1] = n
	d.subscribers[c.ID()] = c
	disconnected := make(chan error, 2)
	d.peripheralDisconnected = func(_ Peripheral, err error) { disconnected <- err }
	results := make(chan error, 2)
	started := make(chan struct{}, 2)
	for _, call := range []struct {
		q   *xpcRequests
		ctx context.Context
	}{{&d.requests, d.ctx}, {&p.requests, p.ctx}} {
		go func() {
			_, err := call.q.request(call.ctx, 71, func() error { started <- struct{}{}; return nil })
			results <- err
		}()
	}
	receiveDarwin(t, started)
	receiveDarwin(t, started)
	d.HandleXpcEvent(nil, xpc.CONNECTION_INTERRUPTED)
	d.HandleXpcEvent(nil, xpc.CONNECTION_INVALID)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := receiveDarwin(t, results); !errors.Is(err, xpc.CONNECTION_INTERRUPTED) {
			t.Fatalf("request error = %v", err)
		}
	}
	if err := receiveDarwin(t, disconnected); !errors.Is(err, xpc.CONNECTION_INTERRUPTED) {
		t.Fatalf("disconnect error = %v", err)
	}
	select {
	case <-disconnected:
		t.Fatal("reported disconnect twice")
	default:
	}
	if !n.Done() || !errors.Is(context.Cause(c.Context()), xpc.CONNECTION_INTERRUPTED) {
		t.Fatal("notification or handler context survived shutdown")
	}
	if err := d.AdvertiseNameAndServices("test", nil); !errors.Is(err, xpc.CONNECTION_INTERRUPTED) {
		t.Fatalf("advertise after interruption = %v", err)
	}
	if _, err := p.ReadCharacteristic(&Characteristic{}); !errors.Is(err, xpc.CONNECTION_INTERRUPTED) {
		t.Fatalf("read after interruption = %v", err)
	}
	if err := p.WriteCharacteristic(&Characteristic{}, nil, false); !errors.Is(err, xpc.CONNECTION_INTERRUPTED) {
		t.Fatalf("write after interruption = %v", err)
	}
	if n, err := c.sendNotification(n, []byte{1}); n != 0 || !errors.Is(err, xpc.CONNECTION_INTERRUPTED) {
		t.Fatalf("notification after interruption = %d, %v", n, err)
	}
	if got := p.ReadRSSI(); got != -1 {
		t.Fatalf("RSSI after interruption = %d", got)
	}
}

func TestDarwinCloseFromReadCallback(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	uuid := xpc.MakeUUID("00112233445566778899aabbccddeeff")
	c := &Characteristic{}
	c.HandleReadFunc(func(_ ResponseWriter, req *ReadRequest) {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
		if !errors.Is(context.Cause(req.Context()), xpc.ErrClosed) {
			t.Error("handler context was not canceled")
		}
	})
	d.attrs[1] = &attr{h: 1, pvt: c}
	done := make(chan struct{})
	go func() {
		d.respondToRequest(19, xpc.Dict{
			"kCBMsgArgDeviceUUID": uuid, "kCBMsgArgTransactionID": int64(1),
			"kCBMsgArgAttributeID": int64(1), "kCBMsgArgOffset": int64(0),
		})
		close(done)
	}()
	receiveDarwin(t, done)
}

func TestDarwinEventsWaitForConnectionPublication(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	d.ready = make(chan struct{})
	done := make(chan struct{})
	go func() { d.HandleXpcEvent(nil, xpc.CONNECTION_INVALID); close(done) }()
	select {
	case <-done:
		t.Fatal("event ran before connection publication")
	default:
	}
	close(d.ready)
	receiveDarwin(t, done)
	if !errors.Is(context.Cause(d.ctx), xpc.CONNECTION_INVALID) {
		t.Fatalf("device error = %v", context.Cause(d.ctx))
	}
}

func TestDarwinConnectedEventReplacesPendingBySameIdentity(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	uuid := xpc.MakeUUID("00112233445566778899aabbccddeeff")
	pending := &peripheral{id: uuid}
	d.plist[pending.ID()] = pending
	connected := make(chan Peripheral, 2)
	disconnected := make(chan Peripheral, 1)
	d.peripheralConnected = func(p Peripheral, err error) {
		if err != nil {
			t.Errorf("unexpected pending connection failure: %v", err)
		}
		connected <- p
	}
	d.peripheralDisconnected = func(p Peripheral, _ error) { disconnected <- p }
	d.HandleXpcEvent(xpc.Dict{
		"kCBMsgId":   int64(peripheralConnected),
		"kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": uuid},
	}, nil)
	live := receiveDarwin(t, connected)
	if len(d.plist) != 1 || d.plist[pending.ID()] != live {
		t.Fatal("connected event left a duplicate pending connection")
	}
	_ = d.Close()
	if got := receiveDarwin(t, disconnected); got != live {
		t.Fatal("closed a different peripheral")
	}
	select {
	case <-connected:
		t.Fatal("reported a failure for an already completed connection")
	default:
	}
}
