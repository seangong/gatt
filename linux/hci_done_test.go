package linux

import (
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/evt"
)

func TestHCIDoneSignalsStartBeforeResourceRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, _ := newShutdownTestHCI(t, false)
		done := h.Done()
		if done != h.Done() {
			t.Fatal("Done returned different lifecycle signals")
		}
		select {
		case <-done:
			t.Fatal("Done closed before shutdown started")
		default:
		}
		release := make(chan struct{})
		if !h.startWorker(func() { <-release }) {
			t.Fatal("worker was not started")
		}
		defer close(release)
		cause := errors.New("controller unavailable")
		h.requestClose(cause)
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Done did not signal shutdown start")
		}
		select {
		case <-h.closeDone:
			t.Fatal("shutdown finished while a protocol worker was still running")
		default:
		}
		if h.Done() != done {
			t.Fatal("shutdown replaced Done")
		}
	})
}

func TestHCIDoneReportsTransportAndControllerFailure(t *testing.T) {
	for _, controller := range []bool{false, true} {
		t.Run(map[bool]string{false: "transport", true: "controller"}[controller], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				done := h.Done()
				cause := errors.New("USB controller removed")
				if controller {
					d.input <- shutdownTestEvent(evt.HardwareError, 0x42)
				} else {
					d.readError <- cause
				}
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("Done remained open after HCI failure")
				}
				err := h.Close()
				if controller && (err == nil || !strings.Contains(err.Error(), "0x42")) {
					t.Fatalf("Close lost hardware failure: %v", err)
				}
				if !controller && !errors.Is(err, cause) {
					t.Fatalf("Close lost read failure: %v", err)
				}
				if h.Done() != done || d.closeCount.Load() != 1 {
					t.Fatal("repeated observation or close changed the lifecycle")
				}
			})
		})
	}
}
