//go:build gattxpc_test

package gatt

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

func TestDarwinCallbackLifecycleOrder(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	d := newDarwinLifecycleTestDevice()
	defer d.Close()
	events := make(chan string, 2)
	d.peripheralConnected = func(Peripheral, error) { events <- "connected" }
	d.peripheralDisconnected = func(Peripheral, error) { events <- "disconnected" }
	args := xpc.Dict{"kCBMsgArgDeviceUUID": xpc.MakeUUID("00112233445566778899aabbccddeeff")}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": args}, nil)
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": args}, nil)
	first, second := receiveDarwin(t, events), receiveDarwin(t, events)
	if first != "connected" || second != "disconnected" {
		t.Fatalf("lifecycle callbacks = %s/%s, want connected/disconnected", first, second)
	}
}

func TestDarwinCallbackConnectedCanWaitForNotification(t *testing.T) {
	var d *device
	d, old := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 31:
			return peripheralConnected, xpc.Dict{}
		case 68:
			// The notification arrives before the subscription result. Both
			// results must pass the receiver while Connected is still running.
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(characteristicRead), "kCBMsgArgs": xpc.Dict{
				"kCBMsgArgDeviceUUID": args["kCBMsgArgDeviceUUID"], "kCBMsgArgIsNotification": int64(1),
				"kCBMsgArgCharacteristicHandle": int64(2), "kCBMsgArgData": []byte("early"),
			}}, nil)
			return notificationValueSet, xpc.Dict{"kCBMsgArgResult": int64(0)}
		case 65:
			return characteristicRead, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgData": []byte("value")}
		}
		return 0, nil
	})
	delete(d.plist, old.ID())
	notified := make(chan error, 1)
	results := make(chan error, 1)
	d.peripheralConnected = func(p Peripheral, err error) {
		if err == nil {
			c := &Characteristic{h: 2, vh: 3}
			err = p.SetNotifyValue(c, func(_ *Characteristic, b []byte, err error) {
				if err != nil || string(b) != "early" {
					notified <- fmt.Errorf("early notification = %q, %v", b, err)
				} else {
					notified <- nil
				}
			})
			if err == nil {
				select {
				case err = <-notified:
				case <-time.After(time.Second):
					err = errors.New("notification waited for Connected to return")
				}
			}
			if err == nil {
				var b []byte
				b, err = p.ReadCharacteristic(c)
				if err == nil && string(b) != "value" {
					err = fmt.Errorf("read value = %q", b)
				}
			}
		}
		results <- err
	}
	d.Connect(old)
	if err := receiveDarwin(t, results); err != nil {
		t.Fatal(err)
	}
}

func TestDarwinCallbackOverflowCancelsOnlyItsOwner(t *testing.T) {
	cancels, reconnects := make(chan struct{}, 2), make(chan struct{}, 1)
	d, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 32 {
			cancels <- struct{}{}
		} else if id == 31 {
			reconnects <- struct{}{}
		}
		return 0, nil
	})
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	p.sub.subscribe(2, func([]byte, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
	})
	ended := make(chan error, 2)
	d.peripheralDisconnected = func(got Peripheral, err error) {
		if got != p {
			err = fmt.Errorf("terminal callback changed owner: %v", got)
		}
		d.Connect(got)
		ended <- err
	}
	notify := func() {
		p.handleResponse(characteristicRead, xpc.Dict{
			"kCBMsgArgIsNotification": int64(1), "kCBMsgArgCharacteristicHandle": int64(2), "kCBMsgArgData": []byte("queued"),
		})
	}
	notify()
	receiveDarwin(t, entered)
	for i := 0; i <= darwinNotificationQueueSize; i++ {
		notify()
	}
	receiveDarwin(t, cancels)
	if !errors.Is(context.Cause(p.context()), errDarwinNotificationQueueFull) || context.Cause(d.ctx) != nil {
		t.Fatalf("overflow cancellation: owner=%v device=%v", context.Cause(p.context()), context.Cause(d.ctx))
	}
	d.plistmu.Lock()
	retained := d.plist[p.ID()] == p && p.closing
	d.plistmu.Unlock()
	if !retained {
		t.Fatal("overflow did not retain the closing native owner")
	}
	once.Do(func() { close(release) })
	select {
	case err := <-ended:
		t.Fatalf("overflow reported Disconnected before native disconnection: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// Only native disconnection releases the owner for immediate reconnection
	// from its terminal callback. The overflow cause must survive that boundary.
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": p.id}}, nil)
	if err := receiveDarwin(t, ended); !errors.Is(err, errDarwinNotificationQueueFull) {
		t.Fatalf("overflow terminal = %v", err)
	}
	receiveDarwin(t, reconnects)
	// Avoid an unrelated pending-connect callback when the fixture closes.
	d.plistmu.Lock()
	delete(d.plist, p.ID())
	d.plistmu.Unlock()
	if calls.Load() != 1 {
		t.Fatalf("termination ran %d queued notifications", calls.Load())
	}
	select {
	case <-ended:
		t.Fatal("overflow delivered duplicate terminal callbacks")
	case <-cancels:
		t.Fatal("overflow sent duplicate native cancels")
	default:
	}
}

func TestDarwinNotificationCallbackCanCancelAndWaitForDisconnect(t *testing.T) {
	d, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 32 {
			return peripheralDisconnected, xpc.Dict{}
		}
		return 0, nil
	})
	ended, result := make(chan Peripheral, 1), make(chan error, 1)
	observed, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	d.peripheralDisconnected = func(got Peripheral, _ error) { ended <- got }
	p.sub.subscribe(2, func([]byte, error) {
		d.CancelConnection(p)
		select {
		case got := <-ended:
			if got != p {
				result <- errors.New("disconnection changed the callback owner")
				return
			}
		case <-time.After(time.Second):
			result <- errors.New("Disconnected waited for the running notification callback")
			return
		}
		observed <- struct{}{}
		<-release
		result <- d.Close()
	})
	p.handleResponse(characteristicRead, xpc.Dict{
		"kCBMsgArgIsNotification": int64(1), "kCBMsgArgCharacteristicHandle": int64(2), "kCBMsgArgData": []byte("value"),
	})
	receiveDarwin(t, observed)
	if len(d.callbackSlots) != 1 {
		t.Fatal("lifecycle completion released a group still running a notification")
	}
	once.Do(func() { close(release) })
	if err := receiveDarwin(t, result); err != nil {
		t.Fatal(err)
	}
}

func TestDarwinNotificationCallbackCanCloseSession(t *testing.T) {
	d, p := newDarwinClientTest(t, func(int, xpc.Dict) (int, xpc.Dict) { return 0, nil })
	ended, result := make(chan error, 1), make(chan error, 1)
	d.peripheralDisconnected = func(_ Peripheral, err error) { ended <- err }
	p.sub.subscribe(2, func([]byte, error) {
		if err := d.Close(); err != nil {
			result <- err
			return
		}
		select {
		case err := <-ended:
			if !errors.Is(err, xpc.ErrClosed) {
				result <- fmt.Errorf("closed callback error = %v", err)
				return
			}
		case <-time.After(time.Second):
			result <- errors.New("Close joined the notification callback")
			return
		}
		result <- nil
	})
	p.handleResponse(characteristicRead, xpc.Dict{
		"kCBMsgArgIsNotification": int64(1), "kCBMsgArgCharacteristicHandle": int64(2), "kCBMsgArgData": []byte("value"),
	})
	if err := receiveDarwin(t, result); err != nil {
		t.Fatal(err)
	}
}

func TestDarwinCallbackPendingKnownUUIDCanBeCanceled(t *testing.T) {
	commands := make(chan int, 2)
	d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		if id == 32 {
			return peripheralDisconnected, xpc.Dict{}
		}
		return 0, nil
	})
	delete(d.plist, old.ID())
	p := NewPeripheral(UUID{old.id})
	if p.(*peripheral).d != nil {
		t.Fatal("known-UUID argument unexpectedly has a device before Connect")
	}
	ended := make(chan Peripheral, 1)
	d.peripheralDisconnected = func(got Peripheral, _ error) { ended <- got }
	d.Connect(p)
	if command := receiveDarwin(t, commands); command != 31 {
		t.Fatalf("connect command = %d", command)
	}
	d.CancelConnection(p)
	if command := receiveDarwin(t, commands); command != 32 {
		t.Fatalf("cancel command = %d", command)
	}
	if got := receiveDarwin(t, ended); got != p {
		t.Fatal("pending cancellation changed the original argument identity")
	}
}

func TestDarwinCallbackPendingKnownUUIDCanRetryAfterRemoteDisconnect(t *testing.T) {
	commands := make(chan int, 2)
	d, old := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	delete(d.plist, old.ID())
	p := NewPeripheral(UUID{old.id})
	ended := make(chan Peripheral, 2)
	d.peripheralDisconnected = func(got Peripheral, _ error) { ended <- got }
	for i := 0; i < 2; i++ {
		d.Connect(p)
		if command := receiveDarwin(t, commands); command != 31 {
			t.Fatalf("connect command = %d", command)
		}
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{
			"kCBMsgArgDeviceUUID": p.(*peripheral).id,
		}}, nil)
		if got := receiveDarwin(t, ended); got != p {
			t.Fatal("pending remote disconnection changed the original argument identity")
		}
	}
}

func TestDarwinCallbackWorkersBoundBlockedReconnects(t *testing.T) {
	for _, sameUUID := range []bool{true, false} {
		t.Run(fmt.Sprintf("sameUUID=%t", sameUUID), func(t *testing.T) {
			d := newDarwinLifecycleTestDevice()
			defer d.Close()
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			entered, ended := make(chan Peripheral, darwinCallbackGroups+1), make(chan Peripheral, darwinCallbackGroups+1)
			d.peripheralConnected = func(p Peripheral, _ error) { entered <- p; <-release }
			d.peripheralDisconnected = func(p Peripheral, _ error) { ended <- p }
			owners := make(map[Peripheral]bool)
			for i := 0; i < darwinCallbackGroups; i++ {
				key := i + 1
				if sameUUID {
					key = 1
				}
				args := xpc.Dict{"kCBMsgArgDeviceUUID": xpc.MakeUUID(fmt.Sprintf("%032x", key))}
				d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": args}, nil)
				p := receiveDarwin(t, entered)
				if owners[p] {
					t.Fatal("reconnection reused an older generation's callback owner")
				}
				owners[p] = true
				if sameUUID {
					d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": args}, nil)
				}
			}
			if len(d.callbackSlots) != darwinCallbackGroups {
				t.Fatal("a blocked callback released its worker permit early")
			}
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{
				"kCBMsgArgDeviceUUID": xpc.MakeUUID("ffffffffffffffffffffffffffffffff"),
			}}, nil)
			if !errors.Is(d.Err(), errDarwinCallbackCapacity) {
				t.Fatalf("exhausted callback session error = %v", d.Err())
			}
			select {
			case <-d.Done():
			default:
				t.Fatal("callback exhaustion did not signal device termination")
			}
			// Already-closed Connect calls must not bypass the same resource bound.
			for i := 0; i < 20; i++ {
				d.Connect(&peripheral{d: d, id: xpc.MakeUUID("ffffffffffffffffffffffffffffffff")})
			}
			select {
			case <-entered:
				t.Fatal("capacity failure started an unbounded fallback callback")
			default:
			}
			once.Do(func() { close(release) })
			for range darwinCallbackGroups {
				p := receiveDarwin(t, ended)
				if !owners[p] {
					t.Fatalf("terminal callback changed generation: %v", p)
				}
				delete(owners, p)
			}
			deadline := time.Now().Add(time.Second)
			for len(d.callbackSlots) != 0 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			if len(d.callbackSlots) != 0 {
				t.Fatal("finished callbacks retained their worker permits")
			}
		})
	}
}

func TestDarwinNotificationCallbacksKeepOrder(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	d := newDarwinLifecycleTestDevice()
	defer d.Close()
	p := &peripheral{d: d, id: xpc.MakeUUID("00112233445566778899aabbccddeeff"), sub: newSubscriber()}
	p.ctx, p.cancel = context.WithCancelCause(d.ctx)
	d.plist[p.ID()] = p
	events := make(chan byte, 2)
	p.sub.subscribe(2, func(b []byte, _ error) { events <- b[0] })
	for _, value := range []byte{1, 2} {
		p.handleResponse(characteristicRead, xpc.Dict{
			"kCBMsgArgIsNotification": int64(1), "kCBMsgArgCharacteristicHandle": int64(2),
			"kCBMsgArgData": []byte{value},
		})
	}
	first, second := receiveDarwin(t, events), receiveDarwin(t, events)
	if first != 1 || second != 2 {
		t.Fatalf("notification callbacks = %d/%d, want 1/2", first, second)
	}
}
