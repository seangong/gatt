//go:build gattxpc_test

package gatt

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

func darwinStateEvent(d *device, state State) {
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(6), "kCBMsgArgs": xpc.Dict{"kCBMsgArgState": int64(state)}}, nil)
}

func TestDarwinStateCallbacksKeepNativeOrder(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	d := newDarwinLifecycleTestDevice()
	defer d.Close()
	states := make(chan State, 2)
	d.stateChanged = func(_ Device, state State) { states <- state }
	darwinStateEvent(d, StatePoweredOn)
	darwinStateEvent(d, StatePoweredOff)
	first, second := receiveDarwin(t, states), receiveDarwin(t, states)
	if first != StatePoweredOn || second != StatePoweredOff {
		t.Fatalf("native states On,Off delivered as %v,%v", first, second)
	}
}

func TestDarwinInitStateAndFollowingEventKeepNativeOrder(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	var d *device
	d, _ = newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 1 {
			darwinStateEvent(d, StatePoweredOn)
			darwinStateEvent(d, StatePoweredOff)
		}
		return 0, nil
	})
	states := make(chan State, 3)
	if err := d.Init(func(_ Device, state State) { states <- state }); err != nil {
		t.Fatal(err)
	}
	first, second := receiveDarwin(t, states), receiveDarwin(t, states)
	if first != StatePoweredOn || second != StatePoweredOff {
		t.Fatalf("Init and later state callbacks = %v,%v", first, second)
	}
	select {
	case state := <-states:
		t.Fatalf("Init delivered an extra state callback: %v", state)
	default:
	}
}

func TestDarwinConcurrentInitKeepsEachInitialHandler(t *testing.T) {
	commands := make(chan int, 2)
	d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	states := make(chan string, 2)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- d.Init(func(Device, State) { states <- "first" }) }()
	if command := receiveDarwin(t, commands); command != 1 {
		t.Fatalf("first Init command = %d", command)
	}
	started := make(chan struct{})
	go func() {
		close(started)
		second <- d.Init(func(Device, State) { states <- "second" })
	}()
	<-started
	// Give the second caller time to reach the occupied request gate. It may
	// not replace the handler of the Init whose native response is pending.
	select {
	case command := <-commands:
		t.Fatalf("second Init bypassed the active request: %d", command)
	case <-time.After(20 * time.Millisecond):
	}
	darwinStateEvent(d, StatePoweredOn)
	if err := receiveDarwin(t, first); err != nil {
		t.Fatal(err)
	}
	if command := receiveDarwin(t, commands); command != 1 {
		t.Fatalf("second Init command = %d", command)
	}
	darwinStateEvent(d, StatePoweredOff)
	if err := receiveDarwin(t, second); err != nil {
		t.Fatal(err)
	}
	if first, second := receiveDarwin(t, states), receiveDarwin(t, states); first != "first" || second != "second" {
		t.Fatalf("concurrent Init callbacks = %q,%q", first, second)
	}
}

func TestDarwinInitialStateCallbackCanClose(t *testing.T) {
	d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 1 {
			return 6, xpc.Dict{"kCBMsgArgState": int64(StatePoweredOn)}
		}
		return 0, nil
	})
	result := make(chan error, 1)
	var calls atomic.Int32
	if err := d.Init(func(_ Device, _ State) { calls.Add(1); result <- d.Close() }); err != nil {
		t.Fatalf("initial callback changed Init success into failure: %v", err)
	}
	if err := receiveDarwin(t, result); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(d.Err(), xpc.ErrClosed) {
		t.Fatalf("callback Close cause = %v", d.Err())
	}
	darwinStateEvent(d, StatePoweredOff)
	if calls.Load() != 1 {
		t.Fatal("Close fabricated or delivered an additional state callback")
	}
}

func TestDarwinStateCallbackCanAdvertiseWhileLaterStateIsQueued(t *testing.T) {
	var d *device
	d, _ = newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 1:
			return 6, xpc.Dict{"kCBMsgArgState": int64(StatePoweredOn)}
		case 8:
			darwinStateEvent(d, StatePoweredOff)
			return 16, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	states, result := make(chan State, 2), make(chan error, 1)
	if err := d.Init(func(_ Device, state State) {
		if state == StatePoweredOn {
			result <- d.AdvertiseNameAndServices("test", nil)
		}
		states <- state
	}); err != nil {
		t.Fatal(err)
	}
	if err := receiveDarwin(t, result); err != nil {
		t.Fatalf("state callback synchronous advertising = %v", err)
	}
	if first, second := receiveDarwin(t, states), receiveDarwin(t, states); first != StatePoweredOn || second != StatePoweredOff {
		t.Fatalf("state callback operation reordered states = %v,%v", first, second)
	}
}

func TestDarwinStateQueueBoundsBlockedHandler(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	defer d.Close()
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	d.stateChanged = func(Device, State) { calls.Add(1); close(entered); <-release; close(returned) }
	darwinStateEvent(d, StatePoweredOn)
	receiveDarwin(t, entered)
	for range darwinStateQueueSize {
		darwinStateEvent(d, StatePoweredOff)
	}
	if d.Err() != nil {
		t.Fatalf("state queue rejected before its bound: %v", d.Err())
	}
	darwinStateEvent(d, StatePoweredOn)
	if !errors.Is(d.Err(), errDarwinStateQueueFull) {
		t.Fatalf("state queue overflow = %v", d.Err())
	}
	receiveDarwin(t, d.Done())
	d.stateMu.Lock()
	queued, running := len(d.stateQueue), d.stateRunning
	d.stateMu.Unlock()
	if queued != 0 || !running {
		t.Fatalf("overflow queue/running = %d/%t", queued, running)
	}
	if err := d.Close(); err != nil || !errors.Is(d.Err(), errDarwinStateQueueFull) {
		t.Fatalf("Close changed the original overflow result: %v/%v", err, d.Err())
	}
	once.Do(func() { close(release) })
	receiveDarwin(t, returned)
	if calls.Load() != 1 {
		t.Fatalf("overflow ran %d callbacks", calls.Load())
	}
}

func TestDarwinStateCloseDiscardsQueuedCallbacks(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	d.stateChanged = func(Device, State) { calls.Add(1); close(entered); <-release }
	darwinStateEvent(d, StatePoweredOn)
	receiveDarwin(t, entered)
	darwinStateEvent(d, StatePoweredOff)
	if err := d.Close(); err != nil || !errors.Is(d.Err(), xpc.ErrClosed) {
		t.Fatalf("normal Close = %v/%v", err, d.Err())
	}
	d.stateMu.Lock()
	queued := len(d.stateQueue)
	d.stateMu.Unlock()
	if queued != 0 || calls.Load() != 1 {
		t.Fatalf("normal Close retained %d pending or ran %d callbacks", queued, calls.Load())
	}
	once.Do(func() { close(release) })
}
