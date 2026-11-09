package gatt

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux"
)

func TestDeviceHCITerminationStopsDevice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("controller failed")
		terminated := make(chan struct{})
		var closes atomic.Int32
		d := &device{
			hci: &linux.HCI{}, hciDone: terminated,
			closeHCI: func() error { closes.Add(1); return cause },
		}
		states := make(chan State, 2)
		reentered := make(chan error, 1)
		if err := d.Init(func(_ Device, s State) {
			states <- s
			if s == StatePoweredOff {
				reentered <- d.Stop()
			}
		}); err != nil {
			t.Fatal(err)
		}
		go d.watchHCI()
		close(terminated)
		synctest.Wait()
		if d.ctx.Err() == nil || !d.stopping || d.state != StatePoweredOff {
			t.Fatal("HCI termination did not cancel and stop the device")
		}
		select {
		case <-d.stopDone:
		default:
			t.Fatal("device shutdown did not finish")
		}
		if len(states) != 2 || <-states != StatePoweredOn || <-states != StatePoweredOff {
			t.Fatal("state callbacks were not delivered once in order")
		}
		select {
		case err := <-reentered:
			if !errors.Is(err, cause) {
				t.Fatalf("reentrant Stop error = %v, want original cause", err)
			}
		default:
			t.Fatal("state callback could not reenter Stop")
		}
		if err := d.Stop(); !errors.Is(err, cause) || closes.Load() != 1 {
			t.Fatalf("repeated Stop = %v, HCI closes = %d", err, closes.Load())
		}
	})
}

func TestDeviceHCITerminationBeforeInit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		terminated := make(chan struct{})
		close(terminated)
		d := &device{hci: &linux.HCI{}, hciDone: terminated, closeHCI: func() error { return nil }}
		states := make(chan State, 2)
		// Do not schedule the observer until Init has checked the HCI signal.
		if err := d.Init(func(_ Device, s State) { states <- s }); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Init = %v, want closed pipe", err)
		}
		if d.initialized || len(states) != 0 {
			t.Fatal("Init published a powered-on state for an already terminated HCI")
		}
		go d.watchHCI()
		synctest.Wait()
		if err := d.Stop(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDeviceHCITerminationWaitsForProtocolWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		terminated := make(chan struct{})
		d := &device{hciDone: terminated, closeHCI: func() error { return nil }}
		d.initLifecycleLocked()
		d.workers.Add(1)
		go d.watchHCI()
		close(terminated)
		synctest.Wait()
		if d.ctx.Err() == nil {
			t.Fatal("HCI termination did not cancel protocol work")
		}
		select {
		case <-d.stopDone:
			t.Fatal("HCI termination bypassed the protocol worker join")
		default:
		}
		d.workers.Done()
		synctest.Wait()
		select {
		case <-d.stopDone:
		default:
			t.Fatal("HCI observer waited for itself during shutdown")
		}
	})
}

func TestDeviceHCITerminationDoesNotWaitForApplicationCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		terminated, release := make(chan struct{}), make(chan struct{})
		d := &device{hci: &linux.HCI{}, hciDone: terminated, closeHCI: func() error { return nil }}
		states := make(chan State, 2)
		if err := d.Init(func(_ Device, s State) {
			states <- s
			if s == StatePoweredOn {
				<-release
			}
		}); err != nil {
			t.Fatal(err)
		}
		go d.watchHCI()
		close(terminated)
		synctest.Wait()
		select {
		case <-d.stopDone:
		default:
			t.Fatal("HCI termination waited for a blocked application callback")
		}
		if len(states) != 1 || <-states != StatePoweredOn {
			t.Fatal("unexpected initial state callback")
		}
		close(release)
		synctest.Wait()
		if len(states) != 1 || <-states != StatePoweredOff {
			t.Fatal("powered-off callback did not follow the initial callback")
		}
	})
}

func TestDeviceHCITerminationPreservesStopContextWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		terminated, release := make(chan struct{}), make(chan struct{})
		cause := errors.New("controller failed while stopping")
		var closes atomic.Int32
		d := &device{hciDone: terminated, closeHCI: func() error {
			closes.Add(1)
			close(terminated)
			<-release
			return cause
		}}
		d.initLifecycleLocked()
		go d.watchHCI()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- d.StopContext(ctx) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation = %v", err)
		}
		select {
		case <-d.stopDone:
			t.Fatal("canceling a caller ended the shared HCI shutdown")
		default:
		}
		close(release)
		synctest.Wait()
		if err := d.Stop(); !errors.Is(err, cause) || closes.Load() != 1 {
			t.Fatalf("final Stop = %v, HCI closes = %d", err, closes.Load())
		}
	})
}
