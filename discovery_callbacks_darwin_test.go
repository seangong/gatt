//go:build gattxpc_test

package gatt

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/seangong/gatt/v2/xpc"
)

func darwinDiscoveryEvent(d *device, name string) {
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDiscovered), "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": xpc.MakeUUID("00112233445566778899aabbccddeeff"),
		"kCBMsgArgAdvertisementData": xpc.Dict{
			"kCBAdvDataLocalName": name, "kCBAdvDataManufacturerData": []byte(name),
		},
		"kCBMsgArgRssi": int64(-60),
	}}, nil)
}

func TestDarwinDiscoveryOverflowReportsSessionFailure(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			d, _ := newDarwinClientTest(t, func(int, xpc.Dict) (int, xpc.Dict) { return 0, nil })
			const limit = 16
			entered := make(chan struct{}, 2*limit+1)
			release := make(chan struct{})
			defer close(release)
			var calls atomic.Int32
			d.peripheralDiscovered = func(Peripheral, *Advertisement, int) {
				calls.Add(1)
				entered <- struct{}{}
				<-release
			}
			for i := 0; i < limit; i++ {
				darwinDiscoveryEvent(d, "active")
				receiveDarwin(t, entered)
			}
			if restart {
				// Restarting must not reset the allowance held by old callbacks.
				d.StopScanning()
				d.Scan(nil, true)
			}
			for i := 0; i < limit; i++ {
				darwinDiscoveryEvent(d, "pending")
			}
			darwinDiscoveryEvent(d, "overflow")
			select {
			case <-d.Done():
			default:
				t.Fatal("discovery overflow did not end the session")
			}
			if !errors.Is(d.Err(), errDarwinDiscoveryQueueFull) {
				t.Fatalf("discovery overflow cause = %v", d.Err())
			}
			cause := d.Err()
			_ = d.Close()
			if d.Err() != cause {
				t.Fatalf("Close replaced the first overflow cause: %v", d.Err())
			}
			if calls.Load() != limit {
				t.Fatalf("started %d callbacks while %d remained blocked", calls.Load(), limit)
			}
		})
	}
}

func TestDarwinDiscoveryQueuePreservesReportsAndParallelism(t *testing.T) {
	d := newDarwinLifecycleTestDevice()
	defer d.Close()
	const limit = 16
	type report struct {
		name, peripheralName, data string
		rssi                       int
	}
	entered := make(chan report, 2*limit)
	release := make(chan struct{})
	defer close(release)
	d.peripheralDiscovered = func(p Peripheral, a *Advertisement, rssi int) {
		entered <- report{a.LocalName, p.Name(), string(a.ManufacturerData), rssi}
		<-release
	}
	check := func(name string) {
		t.Helper()
		got := receiveDarwin(t, entered)
		if got != (report{name, name, name, -60}) {
			t.Fatalf("report = %+v, want %q with original data and RSSI", got, name)
		}
	}
	for i := 0; i < limit; i++ {
		name := fmt.Sprintf("active-%d", i)
		darwinDiscoveryEvent(d, name)
		check(name)
	}
	for i := 0; i < 4; i++ {
		// The same UUID must retain each report rather than being coalesced.
		darwinDiscoveryEvent(d, fmt.Sprintf("queued-%d", i))
	}
	for i := 0; i < 4; i++ {
		release <- struct{}{}
		check(fmt.Sprintf("queued-%d", i))
	}
	if err := d.Err(); err != nil {
		t.Fatalf("valid discovery burst ended the session: %v", err)
	}
}

func TestDarwinDiscoveryBlockedCallbacksKeepNativeResponsesAndCloseUsable(t *testing.T) {
	d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 8 {
			return 16, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	const limit = 16
	entered := make(chan struct{}, 2*limit)
	release := make(chan struct{})
	var calls atomic.Int32
	d.peripheralDiscovered = func(Peripheral, *Advertisement, int) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
	}
	t.Cleanup(func() { close(release) })
	for i := 0; i < limit; i++ {
		darwinDiscoveryEvent(d, "active")
		receiveDarwin(t, entered)
	}
	for i := 0; i < 4; i++ {
		darwinDiscoveryEvent(d, "pending")
	}
	result := make(chan error, 1)
	go func() { result <- d.AdvertiseNameAndServices("test", nil) }()
	if err := receiveDarwin(t, result); err != nil {
		t.Fatalf("blocked discovery prevented a native response: %v", err)
	}
	go func() { result <- d.Close() }()
	if err := receiveDarwin(t, result); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != limit {
		t.Fatalf("Close allowed pending callbacks to start: %d", calls.Load())
	}
}

func TestDarwinDiscoveryStopAndRestartKeepCallbackAllowance(t *testing.T) {
	commands := make(chan xpc.Dict, 4)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		commands <- xpc.Dict{"id": int64(id), "args": args}
		return 0, nil
	})
	const limit = 16
	entered := make(chan string, 2*limit)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	d.peripheralDiscovered = func(_ Peripheral, a *Advertisement, _ int) {
		entered <- a.LocalName
		<-release
	}
	d.Scan([]UUID{UUID16(0xff00)}, true)
	command := receiveDarwin(t, commands)
	args := command.MustGetDict("args")
	if command.MustGetInt("id") != 29 || len(args.MustGetArray("kCBMsgArgUUIDs")) != 1 ||
		args.MustGetDict("kCBMsgArgOptions").MustGetInt("kCBScanOptionAllowDuplicates") != 1 {
		t.Fatalf("initial Scan command = %v", command)
	}
	for i := 0; i < limit; i++ {
		darwinDiscoveryEvent(d, "active")
		receiveDarwin(t, entered)
	}
	darwinDiscoveryEvent(d, "old-pending")
	d.StopScanning()
	if command := receiveDarwin(t, commands); command.MustGetInt("id") != 30 {
		t.Fatalf("StopScanning command = %v", command)
	}
	darwinDiscoveryEvent(d, "after-stop")
	d.Scan(nil, false)
	command = receiveDarwin(t, commands)
	args = command.MustGetDict("args")
	if command.MustGetInt("id") != 29 || len(args.MustGetArray("kCBMsgArgUUIDs")) != 0 ||
		args.MustGetDict("kCBMsgArgOptions").MustGetInt("kCBScanOptionAllowDuplicates") != 0 {
		t.Fatalf("restart Scan command = %v", command)
	}
	darwinDiscoveryEvent(d, "new-pending")
	release <- struct{}{}
	if got := receiveDarwin(t, entered); got != "new-pending" {
		t.Fatalf("restart delivered %q, want the new pending report", got)
	}
	if err := d.Err(); err != nil {
		t.Fatalf("Stop/Scan ended the device session: %v", err)
	}
}

func TestDarwinDiscoveryCallbackCanStopRestartAndClose(t *testing.T) {
	commands := make(chan int, 2)
	d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		commands <- id
		return 0, nil
	})
	result := make(chan error, 1)
	d.peripheralDiscovered = func(Peripheral, *Advertisement, int) {
		d.StopScanning()
		d.Scan(nil, true)
		// Wait for both locally queued commands to cross the native bridge
		// before Close cancels their delivery.
		if first, second := <-commands, <-commands; first != 30 || second != 29 {
			result <- fmt.Errorf("reentrant scan commands = %d,%d", first, second)
			return
		}
		result <- d.Close()
	}
	darwinDiscoveryEvent(d, "reentrant")
	if err := receiveDarwin(t, result); err != nil {
		t.Fatal(err)
	}
}

func TestDarwinDiscoveryFailedScanCommandDoesNotDeadlock(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop=%v", stop), func(t *testing.T) {
			d := newDarwinLifecycleTestDevice()
			defer d.Close()
			done := make(chan struct{})
			go func() {
				if stop {
					d.StopScanning()
				} else {
					d.Scan(nil, false)
				}
				close(done)
			}()
			receiveDarwin(t, done)
			if !errors.Is(d.Err(), xpc.ErrClosed) {
				t.Fatalf("failed scan command cause = %v", d.Err())
			}
		})
	}
}
