package linux

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func pendingTestStart(t *testing.T, h *HCI, d *shutdownTestTransport, pd *PlatData, accept bool) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- h.Connect(pd) }()
	opcode := uint16(cmd.LECreateConn{}.Opcode())
	shutdownTestCommand(t, d, opcode)
	if accept {
		shutdownTestStatus(d, opcode)
		if err := <-result; err != nil {
			t.Fatalf("Connect() = %v", err)
		}
	}
	return result
}

func pendingTestTerminal(d *shutdownTestTransport, status byte) {
	// On failure all fields other than Status are invalid, including address,
	// address type and role. Cancellation must not filter on those fields.
	packet := make([]byte, 19)
	packet[0], packet[1] = byte(evt.LEConnectionComplete), status
	d.input <- shutdownTestEvent(evt.LEMeta, packet...)
	synctest.Wait()
}

func pendingTestResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	default:
		t.Fatal("operation did not finish")
		return nil
	}
}

func pendingTestBlocked(t *testing.T, result <-chan error) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-result:
		t.Fatalf("operation finished before terminal connection event: %v", err)
	default:
	}
}

func pendingTestNoCommand(t *testing.T, d *shutdownTestTransport) {
	t.Helper()
	synctest.Wait()
	select {
	case packet := <-d.writes:
		t.Fatalf("unexpected controller command: %x", packet)
	default:
	}
}

func pendingTestStillReserved(t *testing.T, h *HCI) {
	t.Helper()
	if err := h.Connect(&PlatData{Address: [6]byte{9, 8, 7, 6, 5, 4}}); err == nil || !strings.Contains(err.Error(), "already pending") {
		t.Fatalf("Connect() during cancellation = %v, want pending attempt", err)
	}
}

func TestHCICancelPendingConnectionWaitsForTerminalEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}, AddressType: 1}
		pendingTestStart(t, h, d, pd, true)
		result := connectionIdentityCancel(h, pd)
		opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		pendingTestBlocked(t, result)
		pendingTestStillReserved(t, h)
		pendingTestTerminal(d, 0x02)
		if err := pendingTestResult(t, result); err != nil {
			t.Fatal(err)
		}
		pendingTestStart(t, h, d, &PlatData{Address: [6]byte{6, 5, 4, 3, 2, 1}}, true)
	})
}

func TestHCICancelPendingConnectionBeforeCreateStatus(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		name := "accepted"
		if rejected {
			name = "rejected"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
				connect := pendingTestStart(t, h, d, pd, false)
				result := connectionIdentityCancel(h, pd)
				pendingTestBlocked(t, result)
				pendingTestNoCommand(t, d)
				opcode := uint16(cmd.LECreateConn{}.Opcode())
				status := byte(0)
				if rejected {
					status = 0x0c
				}
				d.input <- shutdownTestEvent(evt.CommandStatus, status, 1, byte(opcode), byte(opcode>>8))
				synctest.Wait()
				if err := pendingTestResult(t, connect); (err != nil) != rejected {
					t.Fatalf("Connect() = %v, rejected=%v", err, rejected)
				}
				if !rejected {
					opcode = uint16(cmd.LECreateConnCancel{}.Opcode())
					shutdownTestCommand(t, d, opcode)
					shutdownTestCommandComplete(d, opcode, 0)
					pendingTestTerminal(d, 0x02)
				}
				if err := pendingTestResult(t, result); err != nil {
					t.Fatal(err)
				}
				pendingTestNoCommand(t, d)
			})
		})
	}
}

func TestHCICancelPendingConnectionDeduplicatesConcurrentCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
		pendingTestStart(t, h, d, pd, true)
		first := connectionIdentityCancel(h, pd)
		opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		second := connectionIdentityCancel(h, pd)
		pendingTestBlocked(t, second)
		pendingTestNoCommand(t, d)
		shutdownTestCommandComplete(d, opcode, 0)
		pendingTestTerminal(d, 0x02)
		for _, result := range []<-chan error{first, second} {
			if err := pendingTestResult(t, result); err != nil {
				t.Fatal(err)
			}
		}
		pendingTestNoCommand(t, d)
	})
}

func TestHCICancelPendingConnectionSharesShutdown(t *testing.T) {
	for _, mode := range []string{"cancel first", "shutdown first", "queued cancel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
				pendingTestStart(t, h, d, pd, true)
				var ordinary <-chan error
				if mode == "queued cancel" {
					result := make(chan error, 1)
					ordinary = result
					go func() { _, err := h.SendRawCommand(cmd.Reset{}); result <- err }()
					shutdownTestCommand(t, d, uint16(cmd.Reset{}.Opcode()))
				}
				closed := make(chan error, 1)
				var canceled <-chan error
				if mode == "shutdown first" {
					go func() { closed <- h.Close() }()
					shutdownTestCommand(t, d, uint16(cmd.LECreateConnCancel{}.Opcode()))
					canceled = connectionIdentityCancel(h, pd)
				} else {
					canceled = connectionIdentityCancel(h, pd)
					synctest.Wait()
					if mode == "cancel first" {
						shutdownTestCommand(t, d, uint16(cmd.LECreateConnCancel{}.Opcode()))
					}
					go func() { closed <- h.Close() }()
				}
				pendingTestBlocked(t, canceled)
				if ordinary != nil {
					shutdownTestCommandComplete(d, uint16(cmd.Reset{}.Opcode()), 0)
					if err := pendingTestResult(t, ordinary); err != nil {
						t.Fatal(err)
					}
					shutdownTestCommand(t, d, uint16(cmd.LECreateConnCancel{}.Opcode()))
				}
				shutdownTestCommandComplete(d, uint16(cmd.LECreateConnCancel{}.Opcode()), 0)
				pendingTestTerminal(d, 0x02)
				for _, result := range []<-chan error{canceled, closed} {
					if err := pendingTestResult(t, result); err != nil {
						t.Fatal(err)
					}
				}
				pendingTestNoCommand(t, d)
			})
		})
	}
}

func TestHCICancelPendingConnectionHandlesSuccessfulConnectionRace(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "cancel written"
		if queued {
			name = "cancel queued"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				accepted := make(chan *PlatData, 1)
				h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
				pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
				pendingTestStart(t, h, d, pd, true)
				var ordinary <-chan error
				if queued {
					result := make(chan error, 1)
					ordinary = result
					go func() { _, err := h.SendRawCommand(cmd.Reset{}); result <- err }()
					shutdownTestCommand(t, d, uint16(cmd.Reset{}.Opcode()))
				}
				canceled := connectionIdentityCancel(h, pd)
				synctest.Wait()
				if !queued {
					shutdownTestCommand(t, d, uint16(cmd.LECreateConnCancel{}.Opcode()))
				}
				packet := make([]byte, 19)
				packet[0], packet[2], packet[12] = byte(evt.LEConnectionComplete), 1, 0x18
				for i := range pd.Address {
					packet[6+i] = pd.Address[5-i]
				}
				d.input <- shutdownTestEvent(evt.LEMeta, packet...)
				synctest.Wait()
				pendingTestStillReserved(t, h)
				h.connsmu.Lock()
				c := h.conns[1]
				h.connsmu.Unlock()
				if c == nil || c.Context().Err() == nil {
					t.Fatal("connection that raced cancellation was not stopped")
				}
				if ordinary != nil {
					shutdownTestCommandComplete(d, uint16(cmd.Reset{}.Opcode()), 0)
					if err := pendingTestResult(t, ordinary); err != nil {
						t.Fatal(err)
					}
					shutdownTestCommand(t, d, uint16(cmd.LECreateConnCancel{}.Opcode()))
				}
				shutdownTestCommandComplete(d, uint16(cmd.LECreateConnCancel{}.Opcode()), 0x0c)
				shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
				shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
				pendingTestBlocked(t, canceled)
				shutdownTestDisconnected(d, 1)
				if err := pendingTestResult(t, canceled); err != nil {
					t.Fatal(err)
				}
				shutdownTestCommand(t, d, uint16(cmd.LESetAdvertiseEnable{}.Opcode()))
				shutdownTestCommandComplete(d, uint16(cmd.LESetAdvertiseEnable{}.Opcode()), 0)
				select {
				case <-accepted:
					t.Fatal("canceled connection published Connected")
				default:
				}
				pendingTestStart(t, h, d, &PlatData{Address: [6]byte{6, 5, 4, 3, 2, 1}}, true)
			})
		})
	}
}

func TestHCICancelPendingConnectionPreservesAttemptIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		address := [6]byte{1, 2, 3, 4, 5, 6}
		old := &PlatData{Address: address}
		pendingTestStart(t, h, d, old, true)
		result := connectionIdentityCancel(h, old)
		opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		pendingTestTerminal(d, 0x02)
		if err := pendingTestResult(t, result); err != nil {
			t.Fatal(err)
		}

		// Reusing a connected callback object's stale Conn must not hide its
		// new pending attempt; another object for the same address is retired.
		replacement := &PlatData{Address: address, Conn: newConn(h, 1)}
		pendingTestStart(t, h, d, replacement, true)
		if err := h.CancelConnection(old); err != nil {
			t.Fatal(err)
		}
		pendingTestNoCommand(t, d)
		result = connectionIdentityCancel(h, replacement)
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		pendingTestTerminal(d, 0x02)
		if err := pendingTestResult(t, result); err != nil {
			t.Fatal(err)
		}
	})
}

func TestHCICancelPendingConnectionFailsClosedOnTimeout(t *testing.T) {
	for _, mode := range []string{"create status missing", "cancel response missing", "terminal event missing", "cancel write blocked", "command disallowed without event"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
				createPending := mode == "create status missing"
				connect := pendingTestStart(t, h, d, pd, !createPending)
				if mode == "cancel write blocked" {
					d.blockWrite = true
				}
				started := time.Now()
				result := connectionIdentityCancel(h, pd)
				if !createPending {
					opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
					shutdownTestCommand(t, d, opcode)
					if mode == "terminal event missing" {
						shutdownTestCommandComplete(d, opcode, 0)
					} else if mode == "command disallowed without event" {
						shutdownTestCommandComplete(d, opcode, 0x0c)
					}
				}
				if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("CancelConnection() = %v, want deadline exceeded", err)
				}
				if elapsed := time.Since(started); elapsed > hciCloseTimeout {
					t.Fatalf("cancellation took %s, exceeds budget %s", elapsed, hciCloseTimeout)
				}
				if err := h.Close(); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Close() after cancellation timeout = %v", err)
				}
				if createPending {
					if err := <-connect; !errors.Is(err, io.ErrClosedPipe) {
						t.Fatalf("Connect() interrupted by cancellation = %v", err)
					}
				}
				if got := d.closeCount.Load(); got != 1 {
					t.Fatalf("physical transport closes = %d, want 1", got)
				}
			})
		})
	}
}

func TestHCICancelPendingConnectionRejectsFailedCancelStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
		pendingTestStart(t, h, d, pd, true)
		result := connectionIdentityCancel(h, pd)
		opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0x01)
		if err := pendingTestResult(t, result); err == nil || !strings.Contains(err.Error(), "status 0x01") {
			t.Fatalf("CancelConnection() = %v, want controller status", err)
		}
		if err := h.Close(); err == nil || !strings.Contains(err.Error(), "status 0x01") {
			t.Fatalf("Close() = %v, want original cancellation error", err)
		}
	})
}

func TestHCICancelPendingConnectionRespectsEarlierShutdownDeadline(t *testing.T) {
	for _, blockWrite := range []bool{false, true} {
		name := "terminal event missing"
		if blockWrite {
			name = "cancel write blocked"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
				pendingTestStart(t, h, d, pd, true)
				d.blockWrite = blockWrite
				canceled := connectionIdentityCancel(h, pd)
				opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
				shutdownTestCommand(t, d, opcode)
				if !blockWrite {
					shutdownTestCommandComplete(d, opcode, 0)
				}
				started := time.Now()
				closed := make(chan error, 1)
				go func() { closed <- h.Close() }()
				if err := <-closed; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Close() waiting for an ordinary cancellation = %v", err)
				}
				if elapsed := time.Since(started); elapsed > hciGraceTimeout {
					t.Fatalf("shutdown took %s, exceeds its own budget %s", elapsed, hciGraceTimeout)
				}
				synctest.Wait()
				if err := pendingTestResult(t, canceled); !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("ordinary cancellation interrupted by shutdown = %v", err)
				}
				pendingTestNoCommand(t, d)
			})
		})
	}
}

func TestHCICancelPendingConnectionReportsTransportFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}
		pendingTestStart(t, h, d, pd, true)
		result := connectionIdentityCancel(h, pd)
		opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		d.readError <- io.EOF
		synctest.Wait()
		if err := pendingTestResult(t, result); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("CancelConnection() after transport EOF = %v", err)
		}
		if err := h.Close(); !errors.Is(err, io.EOF) {
			t.Fatalf("Close() after transport EOF = %v", err)
		}
	})
}

func TestHCIShutdownRejectsUnsentCreateWithoutCancelFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		ordinary := make(chan error, 1)
		go func() { _, err := h.SendRawCommand(cmd.Reset{}); ordinary <- err }()
		shutdownTestCommand(t, d, uint16(cmd.Reset{}.Opcode()))
		connect := make(chan error, 1)
		go func() { connect <- h.Connect(&PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}}) }()
		synctest.Wait()
		closed := make(chan error, 1)
		go func() { closed <- h.Close() }()
		synctest.Wait()
		if err := pendingTestResult(t, connect); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("unsent Create during shutdown = %v", err)
		}
		if err := pendingTestResult(t, closed); err != nil {
			t.Fatalf("Close() with no active controller attempt = %v", err)
		}
		if err := pendingTestResult(t, ordinary); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("ordinary command after shutdown = %v", err)
		}
		pendingTestNoCommand(t, d)
	})
}
