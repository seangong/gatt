package linux

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func hardwareTestClosed(t *testing.T, h *HCI, d *shutdownTestTransport, want string) error {
	t.Helper()
	synctest.Wait()
	h.connsmu.Lock()
	closed, cause := h.closed, h.closeCause
	h.connsmu.Unlock()
	if !closed || cause == nil || !strings.Contains(cause.Error(), want) {
		t.Fatalf("hardware failure = closed %t, cause %v; want terminal error containing %q", closed, cause, want)
	}
	select {
	case <-h.closeDone:
	default:
		t.Fatal("hardware failure did not finish shutdown")
	}
	if err := h.Close(); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Close after hardware failure = %v, want %q", err, want)
	}
	if d.closeCount.Load() != 1 {
		t.Fatalf("transport closed %d times, want 1", d.closeCount.Load())
	}
	return cause
}

func TestHCIHardwareErrorReleasesCommandWaitsAndConnections(t *testing.T) {
	for _, code := range []byte{0, 0x42, 0xff} {
		for _, zeroCredit := range []bool{false, true} {
			name := fmt.Sprintf("code_%02x/in_flight", code)
			if zeroCredit {
				name = fmt.Sprintf("code_%02x/zero_credit", code)
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					h, d := newShutdownTestHCI(t, false)
					if zeroCredit {
						reset := make(chan error, 1)
						go func() { _, err := h.SendRawCommand(cmd.Reset{}); reset <- err }()
						op := uint16(cmd.Reset{}.Opcode())
						shutdownTestCommand(t, d, op)
						d.input <- shutdownTestEvent(evt.CommandComplete, 0, byte(op), byte(op>>8), 0)
						synctest.Wait()
						if err := <-reset; err != nil {
							t.Fatal(err)
						}
					}
					h.connsmu.Lock()
					connection := newConn(h, 1)
					h.conns[1] = connection
					h.connsmu.Unlock()
					ordinary := make(chan error, 1)
					go func() { _, err := h.SendRawCommand(cmd.LEReadBufferSize{}); ordinary <- err }()
					if zeroCredit {
						pendingTestNoCommand(t, d)
					} else {
						shutdownTestCommand(t, d, uint16(cmd.LEReadBufferSize{}.Opcode()))
					}
					queued := make(chan error, 1)
					go func() { _, err := h.SendRawCommand(cmd.Reset{}); queued <- err }()
					synctest.Wait()
					started := time.Now()
					d.input <- shutdownTestEvent(evt.HardwareError, code)
					want := fmt.Sprintf("0x%02x", code)
					cause := hardwareTestClosed(t, h, d, want)
					if elapsed := time.Since(started); elapsed >= hciGraceTimeout {
						t.Errorf("hardware failure waited for shutdown grace: %v", elapsed)
					}
					for _, result := range []<-chan error{ordinary, queued} {
						if err := pendingTestResult(t, result); err == nil {
							t.Error("command succeeded after hardware failure")
						}
					}
					select {
					case <-connection.Context().Done():
					default:
						t.Error("hardware failure left an active connection live")
					}
					// A later fault or repeated close must preserve the first cause.
					h.handlePacket(shutdownTestEvent(evt.HardwareError, code^0xff))
					synctest.Wait()
					h.connsmu.Lock()
					got := h.closeCause
					h.connsmu.Unlock()
					if got != cause || d.closeCount.Load() != 1 {
						t.Fatal("a repeated fault replaced the original cause or closed twice")
					}
				})
			})
		}
	}
}

func TestHCIHardwareErrorReleasesPendingConnectionAndCleanup(t *testing.T) {
	for _, mode := range []string{"before_create_status", "pending_connection", "cancel_command", "cancel_terminal_event"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				pd := &PlatData{Address: [6]byte{1}, AddressType: 1}
				ready := make(chan (<-chan ConnectionResult), 1)
				go func() { ready <- h.ConnectWithResult(pd) }()
				createOp := uint16(cmd.LECreateConn{}.Opcode())
				shutdownTestCommand(t, d, createOp)
				if mode != "before_create_status" {
					shutdownTestStatus(d, createOp)
				}
				var canceled <-chan error
				if strings.HasPrefix(mode, "cancel_") {
					canceled = connectionIdentityCancel(h, pd)
					cancelOp := uint16(cmd.LECreateConnCancel{}.Opcode())
					shutdownTestCommand(t, d, cancelOp)
					if mode == "cancel_terminal_event" {
						shutdownTestCommandComplete(d, cancelOp, 0)
					}
				}
				d.input <- shutdownTestEvent(evt.HardwareError, 0x42)
				hardwareTestClosed(t, h, d, "0x42")
				var result <-chan ConnectionResult
				select {
				case result = <-ready:
				default:
					t.Fatal("hardware failure blocked ConnectWithResult admission")
				}
				outcome := resultTestRead(t, result)
				if outcome.Peripheral != pd || outcome.Err == nil {
					t.Fatalf("pending connection outcome = %+v", outcome)
				}
				if canceled == nil {
					if !strings.Contains(outcome.Err.Error(), "0x42") || errors.Is(outcome.Err, context.Canceled) {
						t.Errorf("hardware failure did not reach the pending connection: %v", outcome.Err)
					}
				} else {
					if !errors.Is(outcome.Err, context.Canceled) {
						t.Errorf("explicit cancellation outcome was replaced: %v", outcome.Err)
					}
					if err := pendingTestResult(t, canceled); err == nil {
						t.Error("cancel cleanup succeeded without a healthy controller")
					}
				}
			})
		})
	}
}

func TestHCIHardwareErrorRejectsMalformedEvents(t *testing.T) {
	for _, tt := range []struct {
		name   string
		packet []byte
	}{
		{"empty_code", shutdownTestEvent(evt.HardwareError)},
		{"extra_code", shutdownTestEvent(evt.HardwareError, 0x42, 0x43)},
		{"short_header", []byte{byte(typEventPkt), evt.HardwareError}},
		{"truncated_code", []byte{byte(typEventPkt), evt.HardwareError, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				d.input <- tt.packet
				hardwareTestClosed(t, h, d, "hardware")
			})
		})
	}
}

func TestHCIHardwareErrorInterruptsGracefulShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.connsmu.Lock()
		h.scanning = true
		h.connsmu.Unlock()
		closed := make(chan error, 1)
		go func() { closed <- h.Close() }()
		shutdownTestCommand(t, d, uint16(cmd.LESetScanEnable{}.Opcode()))
		started := time.Now()
		d.input <- shutdownTestEvent(evt.HardwareError, 0x42)
		hardwareTestClosed(t, h, d, "0x42")
		if err := pendingTestResult(t, closed); err == nil || !strings.Contains(err.Error(), "0x42") {
			t.Errorf("graceful Close lost the hardware failure: %v", err)
		}
		if elapsed := time.Since(started); elapsed >= hciGraceTimeout {
			t.Errorf("hardware failure waited for the graceful-close deadline: %v", elapsed)
		}
	})
}

func TestHCIHardwareErrorInterruptsControllerInitialization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		ctx, cancel := context.WithTimeout(context.Background(), hciCloseTimeout)
		defer cancel()
		initialized := make(chan error, 1)
		go func() { initialized <- h.resetDevice(ctx) }()
		shutdownTestCommand(t, d, uint16(cmd.Reset{}.Opcode()))
		started := time.Now()
		d.input <- shutdownTestEvent(evt.HardwareError, 0x42)
		hardwareTestClosed(t, h, d, "0x42")
		if err := pendingTestResult(t, initialized); err == nil {
			t.Error("controller initialization succeeded after a hardware failure")
		}
		if elapsed := time.Since(started); elapsed >= hciCloseTimeout {
			t.Errorf("initialization waited for its deadline after hardware failure: %v", elapsed)
		}
	})
}
