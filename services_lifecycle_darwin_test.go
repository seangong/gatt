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

func darwinServiceForTest(notifiers chan<- Notifier) *Service {
	s := NewService(UUID16(0xff00))
	c := s.AddCharacteristic(UUID16(0xff01))
	c.SetValue([]byte("value"))
	if notifiers != nil {
		c.HandleNotifyFunc(func(_ Request, n Notifier) { notifiers <- n })
	}
	return s
}

func darwinServiceAttributeID(args xpc.Dict) int {
	return args.MustGetArray("kCBMsgArgCharacteristics")[0].(xpc.Dict).MustGetInt("kCBMsgArgAttributeID")
}

func darwinSubscribeForTest(d *device, id int) {
	args := darwinRequestForTest()
	args["kCBMsgArgAttributeID"] = int64(id)
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(21), "kCBMsgArgs": args}, nil)
}

func TestDarwinServicesRemoveReleasesLocalAttributes(t *testing.T) {
	added := make(chan int, 4)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		if id == 10 {
			added <- darwinServiceAttributeID(args)
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	previous := 0
	for i := 0; i < 3; i++ {
		if err := d.SetServices([]*Service{darwinServiceForTest(nil)}); err != nil {
			t.Fatal(err)
		}
		id := receiveDarwin(t, added)
		if id <= previous {
			t.Fatalf("attribute ID reused: previous=%d current=%d", previous, id)
		}
		previous = id
		d.attrmu.RLock()
		n := len(d.attrs)
		d.attrmu.RUnlock()
		if n != 1 {
			t.Fatalf("reconfiguration %d retained %d attributes, want 1", i+1, n)
		}
	}
	if err := d.RemoveAllServices(); err != nil {
		t.Fatal(err)
	}
	d.attrmu.RLock()
	n := len(d.attrs)
	d.attrmu.RUnlock()
	if n != 0 {
		t.Fatalf("removed services retained %d attributes", n)
	}
}

func TestDarwinServicesRemoveStopsOldNotifiersAndKeepsOpaqueIDs(t *testing.T) {
	notifiers := make(chan Notifier, 2)
	added, notifications := make(chan int, 2), make(chan int, 3)
	barrier := make(chan struct{}, 1)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 10:
			added <- darwinServiceAttributeID(args)
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		case 15:
			notifications <- args.MustGetInt("kCBMsgArgAttributeID")
		case 999:
			barrier <- struct{}{}
		}
		return 0, nil
	})
	d.attrN = 70000
	if err := d.AddService(darwinServiceForTest(notifiers)); err != nil {
		t.Fatal(err)
	}
	firstID := receiveDarwin(t, added)
	darwinSubscribeForTest(d, firstID)
	old := receiveDarwin(t, notifiers)
	if _, err := old.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	if id := receiveDarwin(t, notifications); id != firstID || id <= 0xffff {
		t.Fatalf("notification ID=%d, want full native ID=%d", id, firstID)
	}
	if err := d.RemoveAllServices(); err != nil {
		t.Fatal(err)
	}
	if !old.Done() {
		t.Fatal("removed service left its notifier running")
	}
	if err := d.AddService(darwinServiceForTest(notifiers)); err != nil {
		t.Fatal(err)
	}
	secondID := receiveDarwin(t, added)
	if secondID <= firstID {
		t.Fatal("replacement service reused an old ID")
	}
	darwinSubscribeForTest(d, firstID) // A late subscription cannot revive it.
	darwinSubscribeForTest(d, secondID)
	live := receiveDarwin(t, notifiers)
	if _, err := old.Write([]byte("stale")); err == nil {
		t.Fatal("old notifier sent after reconfiguration")
	}
	if _, err := live.Write([]byte("current")); err != nil {
		t.Fatal(err)
	}
	if id := receiveDarwin(t, notifications); id != secondID {
		t.Fatalf("replacement notification ID=%d, want %d", id, secondID)
	}
	if err := d.sendCmd(999, xpc.Dict{}); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, barrier)
	select {
	case id := <-notifications:
		t.Fatalf("unexpected old notification for ID %d", id)
	case <-notifiers:
		t.Fatal("old subscription revived a removed notifier")
	default:
	}
}

func TestDarwinServicesFailedAddRollsBackOnlyOwnAttributes(t *testing.T) {
	added, notifiers := make(chan int, 2), make(chan Notifier, 2)
	var calls atomic.Int32
	var d *device
	d, _ = newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		if id != 10 {
			return 0, nil
		}
		attribute := darwinServiceAttributeID(args)
		added <- attribute
		darwinSubscribeForTest(d, attribute)
		result := int64(0)
		if calls.Add(1) == 2 {
			result = int64(attEcodeUnlikely)
		}
		return 18, xpc.Dict{"kCBMsgArgResult": result}
	})
	if err := d.AddService(darwinServiceForTest(notifiers)); err != nil {
		t.Fatal(err)
	}
	firstID, first := receiveDarwin(t, added), receiveDarwin(t, notifiers)
	if err := d.AddService(darwinServiceForTest(notifiers)); err == nil {
		t.Fatal("failed AddService succeeded")
	}
	failedID, failed := receiveDarwin(t, added), receiveDarwin(t, notifiers)
	d.attrmu.RLock()
	kept, removed, count := d.attrs[firstID], d.attrs[failedID], len(d.attrs)
	d.attrmu.RUnlock()
	if kept == nil || removed != nil || count != 1 || first.Done() || !failed.Done() {
		t.Fatalf("rollback touched unrelated state: count=%d kept=%v failed=%v firstDone=%t failedDone=%t", count, kept != nil, removed != nil, first.Done(), failed.Done())
	}
}

func TestDarwinServicesWriteCannotEnqueueAfterRemove(t *testing.T) {
	notifiers, added := make(chan Notifier, 1), make(chan int, 1)
	commands := make(chan int, 4)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		if id == 10 {
			added <- darwinServiceAttributeID(args)
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		if id == 12 || id == 15 {
			commands <- id
		}
		return 0, nil
	})
	if err := d.AddService(darwinServiceForTest(notifiers)); err != nil {
		t.Fatal(err)
	}
	darwinSubscribeForTest(d, receiveDarwin(t, added))
	n := receiveDarwin(t, notifiers)
	paused := &pauseDarwinCauseContext{Context: d.ctx, entered: make(chan struct{}), release: make(chan struct{})}
	d.ctx = paused
	defer func() {
		select {
		case <-paused.release:
		default:
			close(paused.release)
		}
	}()
	written, removed := make(chan error, 1), make(chan error, 1)
	go func() { _, err := n.Write([]byte("in flight")); written <- err }()
	receiveDarwin(t, paused.entered)
	go func() { removed <- d.RemoveAllServices() }()
	first := 0
	select {
	case first = <-commands:
	case <-time.After(100 * time.Millisecond):
	}
	close(paused.release)
	if err := receiveDarwin(t, written); err != nil {
		t.Fatal(err)
	}
	if err := receiveDarwin(t, removed); err != nil {
		t.Fatal(err)
	}
	if first == 0 {
		first = receiveDarwin(t, commands)
	}
	if second := receiveDarwin(t, commands); first != 15 || second != 12 {
		t.Fatalf("command order=[%d %d], want admitted notification before remove [15 12]", first, second)
	}
	if !n.Done() {
		t.Fatal("in-flight notifier remained live after removal")
	}
}

func TestDarwinServicesCallbackCanRemoveWhileAddAwaitsResponse(t *testing.T) {
	added := make(chan int, 2)
	readCompleted := make(chan struct{}, 1)
	var d *device
	var calls atomic.Int32
	d, _ = newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 10:
			added <- darwinServiceAttributeID(args)
			if calls.Add(1) == 1 {
				return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
			}
		case 999:
			d.respondToRequest(19, args)
		case 13:
			readCompleted <- struct{}{}
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	s := NewService(UUID16(0xff00))
	s.AddCharacteristic(UUID16(0xff01)).HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
		if err := d.RemoveAllServices(); err != nil {
			t.Errorf("RemoveAllServices from native callback: %v", err)
		}
		_, _ = w.Write([]byte("removed"))
	})
	if err := d.AddService(s); err != nil {
		t.Fatal(err)
	}
	firstID := receiveDarwin(t, added)
	result := make(chan error, 1)
	go func() { result <- d.AddService(darwinServiceForTest(nil)) }()
	receiveDarwin(t, added)
	args := darwinRequestForTest()
	args["kCBMsgArgAttributeID"], args["kCBMsgArgOffset"] = int64(firstID), int64(0)
	if err := d.sendCmd(999, args); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, readCompleted)
	if err := receiveDarwin(t, result); err != nil {
		t.Fatal(err)
	}
	d.attrmu.RLock()
	n := len(d.attrs)
	d.attrmu.RUnlock()
	if n != 0 {
		t.Fatalf("late Add success republished %d removed attributes", n)
	}
}

func TestDarwinServicesCloseReleasesPendingAdd(t *testing.T) {
	added := make(chan struct{}, 1)
	d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 10 {
			added <- struct{}{}
		}
		return 0, nil
	})
	result := make(chan error, 1)
	go func() { result <- d.AddService(darwinServiceForTest(nil)) }()
	receiveDarwin(t, added)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := receiveDarwin(t, result); !errors.Is(err, xpc.ErrClosed) {
		t.Fatalf("pending Add after Close = %v", err)
	}
}

func TestDarwinServicesLocalEnqueueFailureDoesNotDeadlock(t *testing.T) {
	for _, add := range []bool{false, true} {
		d := newDarwinLifecycleTestDevice()
		defer d.Close()
		result := make(chan error, 1)
		go func() {
			if add {
				result <- d.AddService(darwinServiceForTest(nil))
			} else {
				result <- d.RemoveAllServices()
			}
		}()
		if err := receiveDarwin(t, result); !errors.Is(err, xpc.ErrClosed) || !errors.Is(context.Cause(d.ctx), xpc.ErrClosed) {
			t.Fatalf("local enqueue failure: add=%t error=%v", add, err)
		}
	}
}

func TestDarwinServicesSubscriptionRegistrationPrecedesRemove(t *testing.T) {
	notifiers, added := make(chan Notifier, 1), make(chan int, 1)
	removeQueued := make(chan struct{}, 1)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 10:
			added <- darwinServiceAttributeID(args)
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		case 12:
			removeQueued <- struct{}{}
		}
		return 0, nil
	})
	if err := d.AddService(darwinServiceForTest(notifiers)); err != nil {
		t.Fatal(err)
	}
	id := receiveDarwin(t, added)
	paused := &pauseSecondDarwinCauseContext{Context: d.ctx, entered: make(chan struct{}), release: make(chan struct{})}
	d.ctx = paused
	defer func() {
		select {
		case <-paused.release:
		default:
			close(paused.release)
		}
	}()
	subscribed := make(chan struct{})
	go func() { darwinSubscribeForTest(d, id); close(subscribed) }()
	// HandleXpcEvent checks context first. The second check pauses startNotify
	// after lookup but before notifier registration, with no production hook.
	receiveDarwin(t, paused.entered)
	removed := make(chan error, 1)
	go func() { removed <- d.RemoveAllServices() }()
	early := false
	select {
	case <-removeQueued:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	close(paused.release)
	receiveDarwin(t, subscribed)
	if err := receiveDarwin(t, removed); err != nil {
		t.Fatal(err)
	}
	if !early {
		receiveDarwin(t, removeQueued)
	}
	if early || !receiveDarwin(t, notifiers).Done() {
		t.Fatal("Remove overtook admitted subscription registration")
	}
}

func TestDarwinServicesQueuedAddPublishesOnlyWhenEnqueued(t *testing.T) {
	added := make(chan int, 2)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		if id == 10 {
			added <- darwinServiceAttributeID(args)
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	if err := d.AddService(darwinServiceForTest(nil)); err != nil {
		t.Fatal(err)
	}
	oldID := receiveDarwin(t, added)
	started := make(chan struct{})
	blocked := make(chan error, 1)
	go func() {
		_, err := d.requests.request(d.ctx, 17, func() error { close(started); return nil })
		blocked <- err
	}()
	receiveDarwin(t, started)
	result := make(chan error, 1)
	go func() { result <- d.AddService(darwinServiceForTest(nil)) }()
	select {
	case err := <-result:
		t.Fatalf("queued Add returned before entering the request gate: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	d.attrmu.RLock()
	before := len(d.attrs)
	d.attrmu.RUnlock()
	if err := d.RemoveAllServices(); err != nil {
		t.Fatal(err)
	}
	d.requests.deliver(17, xpc.Dict{"kCBMsgArgResult": int64(0)})
	if err := receiveDarwin(t, blocked); err != nil {
		t.Fatal(err)
	}
	if err := receiveDarwin(t, result); err != nil {
		t.Fatal(err)
	}
	newID := receiveDarwin(t, added)
	d.attrmu.RLock()
	old, current, after := d.attrs[oldID], d.attrs[newID], len(d.attrs)
	d.attrmu.RUnlock()
	if before != 1 || after != 1 || old != nil || current == nil || newID <= oldID {
		t.Fatalf("queued publication/removal: before=%d after=%d old=%v current=%v IDs=[%d %d]", before, after, old != nil, current != nil, oldID, newID)
	}
}

func TestDarwinServicesSetKeepsPartialSuccessOnFailure(t *testing.T) {
	var calls atomic.Int32
	added := make(chan int, 3)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		if id != 10 {
			return 0, nil
		}
		added <- darwinServiceAttributeID(args)
		result := int64(0)
		if calls.Add(1) == 2 {
			result = int64(attEcodeUnlikely)
		}
		return 18, xpc.Dict{"kCBMsgArgResult": result}
	})
	if err := d.SetServices([]*Service{darwinServiceForTest(nil), darwinServiceForTest(nil), darwinServiceForTest(nil)}); err == nil {
		t.Fatal("partially failed SetServices succeeded")
	}
	firstID, failedID := receiveDarwin(t, added), receiveDarwin(t, added)
	d.attrmu.RLock()
	first, failed, n := d.attrs[firstID], d.attrs[failedID], len(d.attrs)
	d.attrmu.RUnlock()
	if calls.Load() != 2 || n != 1 || first == nil || failed != nil {
		t.Fatalf("partial success not preserved: calls=%d attrs=%d first=%v failed=%v", calls.Load(), n, first != nil, failed != nil)
	}
}
