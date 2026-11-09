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

func TestHCIShutdownWithExhaustedCommandCredits(t *testing.T) {
	for _, recoverCredits := range []bool{false, true} {
		name := "deadline"
		if recoverCredits {
			name = "recovery"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				reset := make(chan error, 1)
				go func() { _, err := h.SendRawCommand(cmd.Reset{}); reset <- err }()
				op := uint16(cmd.Reset{}.Opcode())
				shutdownTestCommand(t, d, op)
				d.input <- shutdownTestEvent(evt.CommandComplete, 0, byte(op), byte(op>>8), 0)
				synctest.Wait()
				if err := <-reset; err != nil {
					t.Fatal(err)
				}
				ordinary, queued, closed := make(chan error, 1), make(chan error, 1), make(chan error, 1)
				go func() { ordinary <- h.SetAdvertiseEnable(true) }()
				synctest.Wait()
				go func() { queued <- h.SetAdvertiseEnable(false) }()
				synctest.Wait()
				pendingTestNoCommand(t, d)
				go func() { closed <- h.Close() }()
				synctest.Wait()
				for _, result := range []<-chan error{ordinary, queued} {
					if err := pendingTestResult(t, result); !errors.Is(err, io.ErrClosedPipe) {
						t.Fatalf("ordinary advertising wait after shutdown = %v", err)
					}
				}
				pendingTestNoCommand(t, d) // Cleanup cannot invent controller credit.
				if recoverCredits {
					d.input <- shutdownTestEvent(evt.CommandStatus, 0, 1, 0, 0)
					op = uint16(cmd.LESetAdvertiseEnable{}.Opcode())
					if packet := shutdownTestCommand(t, d, op); packet[4] != 0 {
						t.Fatalf("shutdown advertising command = %x", packet)
					}
					shutdownTestCommandComplete(d, op, 0)
					if err := pendingTestResult(t, closed); err != nil {
						t.Fatal(err)
					}
				} else {
					time.Sleep(hciCloseTimeout)
					synctest.Wait()
					if err := pendingTestResult(t, closed); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("shutdown without command credits = %v", err)
					}
				}
				if d.closeCount.Load() != 1 {
					t.Fatal("shutdown did not close the transport exactly once")
				}
			})
		})
	}
}

func TestHCICancelPendingConnectionWaitsForCommandCredits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1}}
		ready := make(chan (<-chan ConnectionResult), 1)
		go func() { ready <- h.ConnectWithResult(pd) }()
		op := uint16(cmd.LECreateConn{}.Opcode())
		shutdownTestCommand(t, d, op)
		d.input <- shutdownTestEvent(evt.CommandStatus, 0, 0, byte(op), byte(op>>8))
		synctest.Wait()
		result := <-ready
		canceled := connectionIdentityCancel(h, pd)
		synctest.Wait()
		pendingTestNoCommand(t, d)
		if outcome := resultTestRead(t, result); outcome.Peripheral != pd || !errors.Is(outcome.Err, context.Canceled) {
			t.Fatalf("canceled connection result = %+v", outcome)
		}
		d.input <- shutdownTestEvent(evt.CommandComplete, 1, 0, 0)
		op = uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, op)
		shutdownTestCommandComplete(d, op, 0)
		pendingTestTerminal(d, 0x02)
		if err := pendingTestResult(t, canceled); err != nil {
			t.Fatal(err)
		}
		h.connsmu.Lock()
		closed, pending := h.closed, h.pending
		h.connsmu.Unlock()
		if closed || pending != nil {
			t.Fatal("command credit recovery closed HCI or retained a canceled attempt")
		}
	})
}
