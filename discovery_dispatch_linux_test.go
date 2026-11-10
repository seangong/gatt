package gatt

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux"
)

func TestDeviceDiscoveryBatchExceedsCallbackAllowance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		previous := runtime.GOMAXPROCS(1)
		defer runtime.GOMAXPROCS(previous)
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		defer d.Stop()
		const reports = 25
		seen := make(chan string, reports)
		d.peripheralDiscovered = func(p Peripheral, _ *Advertisement, _ int) { seen <- p.Name() }
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		// Model the independent HCI scan worker delivering one event's batch.
		dispatched := make(chan struct{})
		go func() {
			defer close(dispatched)
			for i := 0; i < reports; i++ {
				name := fmt.Sprintf("peer-%d", i)
				d.hci.AdvertisementHandler(&linux.PlatData{Data: append([]byte{byte(len(name) + 1), typeCompleteName}, name...)})
			}
		}()
		<-dispatched
		synctest.Wait()
		if len(seen) != reports {
			t.Fatalf("delivered %d of %d reports despite fast discovery callbacks", len(seen), reports)
		}
		found := make(map[string]bool, reports)
		for range reports {
			found[<-seen] = true
		}
		if len(found) != reports {
			t.Fatal("batch delivery duplicated a discovery report")
		}
	})
}

func TestDeviceDiscoveryCallbacksRetainConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		defer d.Stop()
		first, second := make(chan struct{}), make(chan struct{})
		result := make(chan error, 1)
		d.peripheralDiscovered = func(p Peripheral, _ *Advertisement, _ int) {
			if p.Name() == "first" {
				close(first)
				select {
				case <-second:
					result <- nil
				case <-time.After(time.Second):
					result <- fmt.Errorf("first callback waited for a serialized second callback")
				}
			} else {
				close(second)
			}
		}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		d.hci.AdvertisementHandler(&linux.PlatData{Data: []byte{6, typeCompleteName, 'f', 'i', 'r', 's', 't'}})
		<-first
		d.hci.AdvertisementHandler(&linux.PlatData{Data: []byte{7, typeCompleteName, 's', 'e', 'c', 'o', 'n', 'd'}})
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})
}

func TestDeviceStopReleasesQueuedDiscoveryWithoutWaitingForCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		defer d.Stop()
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		entered := make(chan struct{}, 16)
		d.peripheralDiscovered = func(Peripheral, *Advertisement, int) { entered <- struct{}{}; <-release }
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		for range cap(d.callbackSlots) {
			d.hci.AdvertisementHandler(&linux.PlatData{Data: []byte{2, typeCompleteName, 'n'}})
			<-entered
		}
		waiting := make(chan struct{})
		go func() {
			defer close(waiting)
			d.hci.AdvertisementHandler(&linux.PlatData{Data: []byte{2, typeCompleteName, 'n'}})
		}()
		synctest.Wait()
		select {
		case <-waiting:
			t.Fatal("discovery was discarded instead of waiting for the existing allowance")
		default:
		}
		if err := d.Stop(); err != nil {
			t.Fatal(err)
		}
		<-waiting
		if len(d.callbackSlots) != cap(d.callbackSlots) {
			t.Fatal("Stop released running callback allowance before handlers returned")
		}
		once.Do(func() { close(release) })
		synctest.Wait()
		if len(d.callbackSlots) != 0 || len(entered) != 0 {
			t.Fatal("queued discovery ran after Stop or returned callbacks retained allowance")
		}
	})
}

func TestDeviceDiscoveryCallbackCanStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		result := make(chan error, 1)
		d.peripheralDiscovered = func(Peripheral, *Advertisement, int) { result <- d.Stop() }
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		d.hci.AdvertisementHandler(&linux.PlatData{Data: []byte{2, typeCompleteName, 'n'}})
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(d.callbackSlots) != 0 {
			t.Fatal("discovery callback could not finish after reentering Stop")
		}
	})
}
