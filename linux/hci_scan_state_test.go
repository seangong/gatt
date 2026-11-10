package linux

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func TestHCIScanRejectionPreservesPreviousStateAndRetry(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			h.scanning = enabled
			result := make(chan error, 1)
			go func() { result <- h.SetScanEnable(!enabled, true) }()
			op := uint16(cmd.LESetScanEnable{}.Opcode())
			packet := shutdownTestCommand(t, d, op)
			if packet[4] != btoi(!enabled) || packet[5] != 0 {
				t.Fatalf("scan command = %x", packet)
			}
			shutdownTestCommandComplete(d, op, 0x0c)
			if err := pendingTestResult(t, result); err == nil {
				t.Fatal("scan rejection returned success")
			}
			h.connsmu.Lock()
			actual, closed := h.scanning, h.closed
			h.connsmu.Unlock()
			if actual != enabled || closed {
				t.Fatalf("rejected scan changed state: scanning=%v closed=%v, previously=%v", actual, closed, enabled)
			}
			go func() { result <- h.SetScanEnable(!enabled, false) }()
			packet = shutdownTestCommand(t, d, op)
			if packet[4] != btoi(!enabled) || packet[5] != 1 {
				t.Fatalf("scan retry command = %x", packet)
			}
			shutdownTestCommandComplete(d, op, 0)
			if err := pendingTestResult(t, result); err != nil {
				t.Fatal(err)
			}
			h.connsmu.Lock()
			actual = h.scanning
			h.connsmu.Unlock()
			if actual != !enabled {
				t.Fatalf("successful scan retry kept state %v", actual)
			}
			pendingTestNoCommand(t, d)
		})
	}
}

func TestHCIScanCommandsCommitInControllerOrder(t *testing.T) {
	for _, enable := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			h.scanning = !enable
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { first <- h.SetScanEnable(enable, false) }()
			op := uint16(cmd.LESetScanEnable{}.Opcode())
			if packet := shutdownTestCommand(t, d, op); packet[4] != btoi(enable) {
				t.Fatalf("first scan command = %x", packet)
			}
			go func() { second <- h.SetScanEnable(!enable, true) }()
			pendingTestNoCommand(t, d)
			shutdownTestCommandComplete(d, op, 0)
			if err := pendingTestResult(t, first); err != nil {
				t.Fatal(err)
			}
			if packet := shutdownTestCommand(t, d, op); packet[4] != btoi(!enable) {
				t.Fatalf("second scan command = %x", packet)
			}
			h.connsmu.Lock()
			actual := h.scanning
			h.connsmu.Unlock()
			if actual != enable {
				t.Fatalf("unconfirmed second command replaced the confirmed state: %v", actual)
			}
			shutdownTestCommandComplete(d, op, 0)
			if err := pendingTestResult(t, second); err != nil {
				t.Fatal(err)
			}
			h.connsmu.Lock()
			actual = h.scanning
			h.connsmu.Unlock()
			if actual != !enable {
				t.Fatalf("second scan result did not commit: %v", actual)
			}
		})
	}
}

func TestHCIShutdownDisablesScanAfterUnconfirmedResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		result, closed := make(chan error, 1), make(chan error, 1)
		go func() { result <- h.SetScanEnable(true, false) }()
		op := uint16(cmd.LESetScanEnable{}.Opcode())
		shutdownTestCommand(t, d, op)
		// The event envelope is complete, but a scan result without its status
		// cannot establish whether the controller applied the enable.
		d.input <- shutdownTestEvent(evt.CommandComplete, 1, byte(op), byte(op>>8))
		synctest.Wait()
		if err := pendingTestResult(t, result); err == nil {
			t.Fatal("missing scan status returned success")
		}
		go func() { closed <- h.Close() }()
		if packet := shutdownTestCommand(t, d, op); packet[4] != 0 {
			t.Fatalf("uncertain scan was not disabled: %x", packet)
		}
		shutdownTestCommandComplete(d, op, 0)
		if err := pendingTestResult(t, closed); err != nil {
			t.Fatal(err)
		}
		pendingTestNoCommand(t, d)
	})
}

func TestHCIShutdownOrdersAfterInflightScanAndCancelsQueuedScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		enabled, queued, closed := make(chan error, 1), make(chan error, 1), make(chan error, 1)
		go func() { enabled <- h.SetScanEnable(true, false) }()
		op := uint16(cmd.LESetScanEnable{}.Opcode())
		if packet := shutdownTestCommand(t, d, op); packet[4] != 1 {
			t.Fatalf("initial scan command = %x", packet)
		}
		go func() { queued <- h.SetScanEnable(false, true) }()
		synctest.Wait()
		go func() { closed <- h.Close() }()
		synctest.Wait()
		if err := pendingTestResult(t, queued); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("queued scan during shutdown = %v", err)
		}
		shutdownTestCommandComplete(d, op, 0)
		if err := pendingTestResult(t, enabled); err != nil {
			t.Fatalf("already submitted scan = %v", err)
		}
		if packet := shutdownTestCommand(t, d, op); packet[4] != 0 {
			t.Fatalf("shutdown did not disable the in-flight scan: %x", packet)
		}
		shutdownTestCommandComplete(d, op, 0)
		if err := pendingTestResult(t, closed); err != nil {
			t.Fatal(err)
		}
		pendingTestNoCommand(t, d)
	})
}

func TestHCIShutdownReleasesScanWaitingForCommandAllowance(t *testing.T) {
	for _, enable := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			d.input <- shutdownTestEvent(evt.CommandComplete, 0, 0, 0)
			synctest.Wait()
			result, closed := make(chan error, 1), make(chan error, 1)
			go func() { result <- h.SetScanEnable(enable, false) }()
			pendingTestNoCommand(t, d)
			go func() { closed <- h.Close() }()
			synctest.Wait()
			if err := pendingTestResult(t, result); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("unsent scan after shutdown = %v", err)
			}
			if enable {
				// Conservative cleanup still obeys the original deadline when a
				// controller withholds all further command allowance.
				time.Sleep(hciCloseTimeout)
				synctest.Wait()
			}
			err := pendingTestResult(t, closed)
			if enable && !errors.Is(err, context.DeadlineExceeded) || !enable && err != nil {
				t.Fatalf("zero-allowance scan shutdown (enable=%v) = %v", enable, err)
			}
			pendingTestNoCommand(t, d)
		})
	}
}
