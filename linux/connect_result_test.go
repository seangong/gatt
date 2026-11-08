package linux

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func resultTestStart(t *testing.T, h *HCI, d *shutdownTestTransport, pd *PlatData, status byte) <-chan ConnectionResult {
	t.Helper()
	ready := make(chan (<-chan ConnectionResult), 1)
	go func() { ready <- h.ConnectWithResult(pd) }()
	op := uint16(cmd.LECreateConn{}.Opcode())
	shutdownTestCommand(t, d, op)
	d.input <- shutdownTestEvent(evt.CommandStatus, status, 1, byte(op), byte(op>>8))
	synctest.Wait()
	return <-ready
}

func resultTestRead(t *testing.T, result <-chan ConnectionResult) ConnectionResult {
	t.Helper()
	synctest.Wait()
	select {
	case r, ok := <-result:
		if !ok {
			t.Fatal("connection result closed without an outcome")
		}
		if _, open := <-result; open {
			t.Fatal("connection result contains more than one outcome")
		}
		return r
	default:
		t.Fatal("connection outcome was not published")
		return ConnectionResult{}
	}
}

func resultTestPhysical(d *shutdownTestTransport, pd *PlatData) {
	packet := make([]byte, 19)
	packet[0], packet[2], packet[12] = byte(evt.LEConnectionComplete), 1, 0x18
	for i := range pd.Address {
		packet[6+i] = pd.Address[5-i]
	}
	d.input <- shutdownTestEvent(evt.LEMeta, packet...)
	synctest.Wait()
}

func TestConnectResultReportsRejectedAndFailedAttempts(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			pd := &PlatData{Address: [6]byte{1}}
			status := byte(0)
			if rejected {
				status = 0x0c
			}
			result := resultTestStart(t, h, d, pd, status)
			if !rejected {
				pendingTestTerminal(d, 0x3e)
			}
			r := resultTestRead(t, result)
			if r.Peripheral != pd || r.Err == nil || errors.Is(r.Err, context.Canceled) {
				t.Fatalf("connection failure = %+v", r)
			}
			h.connsmu.Lock()
			closed, pending := h.closed, h.pending
			h.connsmu.Unlock()
			if closed || pending != nil {
				t.Fatal("ordinary connection rejection retained an attempt or closed HCI")
			}
			pendingTestNoCommand(t, d)
		})
	}
}

func TestConnectResultBusyDoesNotReplaceOriginalAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		first, second := &PlatData{Address: [6]byte{1}}, &PlatData{Address: [6]byte{2}}
		original := resultTestStart(t, h, d, first, 0)
		r := resultTestRead(t, h.ConnectWithResult(second))
		if r.Peripheral != second || r.Err == nil || !strings.Contains(r.Err.Error(), "already pending") {
			t.Fatalf("busy outcome = %+v", r)
		}
		pendingTestTerminal(d, 0x3e)
		r = resultTestRead(t, original)
		if r.Peripheral != first || r.Err == nil {
			t.Fatalf("original outcome = %+v", r)
		}
	})
}

func TestConnectResultCancellationKeepsCleanupSuccessful(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			pd := &PlatData{Address: [6]byte{1}}
			ready := make(chan (<-chan ConnectionResult), 1)
			go func() { ready <- h.ConnectWithResult(pd) }()
			op := uint16(cmd.LECreateConn{}.Opcode())
			shutdownTestCommand(t, d, op)
			canceled := connectionIdentityCancel(h, pd)
			synctest.Wait()
			status := byte(0)
			if rejected {
				status = 0x0c
			}
			d.input <- shutdownTestEvent(evt.CommandStatus, status, 1, byte(op), byte(op>>8))
			synctest.Wait()
			result := <-ready
			if !rejected {
				op = uint16(cmd.LECreateConnCancel{}.Opcode())
				shutdownTestCommand(t, d, op)
				shutdownTestCommandComplete(d, op, 0)
				pendingTestTerminal(d, 0x02)
			}
			if err := pendingTestResult(t, canceled); err != nil {
				t.Fatalf("expected cancellation became a cleanup error: %v", err)
			}
			if r := resultTestRead(t, result); r.Peripheral != pd || !errors.Is(r.Err, context.Canceled) {
				t.Fatalf("canceled outcome = %+v", r)
			}
			h.connsmu.Lock()
			closed := h.closed
			h.connsmu.Unlock()
			if closed {
				t.Fatal("expected cancellation closed the HCI")
			}
		})
	}
}

func TestConnectResultSurvivesClosedCalloutGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1}}
		result := resultTestStart(t, h, d, pd, 0)
		for i := 0; i < cap(h.callbacks); i++ {
			h.callbacks <- struct{}{}
		}
		want := errors.New("controller transport failed")
		d.readError <- want
		r := resultTestRead(t, result)
		if r.Peripheral != pd || !errors.Is(r.Err, want) {
			t.Fatalf("transport outcome = %+v", r)
		}
		if err := h.Close(); !errors.Is(err, want) {
			t.Fatalf("HCI terminal error = %v", err)
		}
	})
}

func TestConnectResultDoesNotDuplicateLegacySuccessHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.maxConn, h.adv = 1, true // No setup command is needed at capacity.
		accepted := make(chan *PlatData, 1)
		h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
		pd := &PlatData{Address: [6]byte{1}}
		result := resultTestStart(t, h, d, pd, 0)
		resultTestPhysical(d, pd)
		r := resultTestRead(t, result)
		if r.Err != nil || r.Peripheral == nil || r.Peripheral.Conn == nil {
			t.Fatalf("successful outcome = %+v", r)
		}
		select {
		case <-accepted:
			t.Fatal("result API also invoked the legacy handler")
		default:
		}
	})
}

func TestConnectResultEarlySuccessIsNotReplacedByLateCommandError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.maxConn, h.adv = 1, true
		pd := &PlatData{Address: [6]byte{1}}
		ready := make(chan (<-chan ConnectionResult), 1)
		go func() { ready <- h.ConnectWithResult(pd) }()
		op := uint16(cmd.LECreateConn{}.Opcode())
		shutdownTestCommand(t, d, op)
		resultTestPhysical(d, pd)
		d.input <- shutdownTestEvent(evt.CommandStatus, 0x0c, 1, byte(op), byte(op>>8))
		synctest.Wait()
		r := resultTestRead(t, <-ready)
		if r.Err != nil || r.Peripheral.Conn == nil || r.Peripheral.Conn.(*conn).Context().Err() != nil {
			t.Fatalf("early success was overwritten: %+v", r)
		}
	})
}

func TestConnectResultSetupFailureAndEarlyTermination(t *testing.T) {
	for _, mode := range []string{"setup-error", "remote-disconnect", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				h.advmu.Lock()
				h.adv = true
				h.advmu.Unlock()
				pd := &PlatData{Address: [6]byte{1}}
				result := resultTestStart(t, h, d, pd, 0)
				resultTestPhysical(d, pd)
				h.connsmu.Lock()
				physical := h.conns[1]
				h.connsmu.Unlock()
				op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
				shutdownTestCommand(t, d, op)
				var canceled <-chan error
				switch mode {
				case "remote-disconnect":
					shutdownTestDisconnected(d, 1)
				case "cancel":
					canceled = connectionIdentityCancel(h, pd)
					synctest.Wait()
				}
				status := byte(0)
				if mode == "setup-error" {
					status = 0x0c
				}
				shutdownTestCommandComplete(d, op, status)
				r := resultTestRead(t, result)
				if r.Peripheral != pd || r.Err == nil || (mode == "cancel") != errors.Is(r.Err, context.Canceled) {
					t.Fatalf("%s outcome = %+v", mode, r)
				}
				if physical.ConnectionCanceled() != (mode == "cancel") {
					t.Fatalf("%s was assigned the wrong cancellation intent", mode)
				}
				if mode != "remote-disconnect" {
					shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
					shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
					shutdownTestDisconnected(d, 1)
				}
				shutdownTestCommand(t, d, op)
				shutdownTestCommandComplete(d, op, 0)
				if canceled != nil {
					if err := pendingTestResult(t, canceled); err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}

func TestConnectResultPublishedSuccessRetainsCancellationIntent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.maxConn, h.adv = 1, true
		pd := &PlatData{Address: [6]byte{1}}
		result := resultTestStart(t, h, d, pd, 0)
		resultTestPhysical(d, pd)
		r := resultTestRead(t, result)
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		physical := r.Peripheral.Conn.(*conn)
		canceled := connectionIdentityCancel(h, pd)
		op := uint16(cmd.Disconnect{}.Opcode())
		shutdownTestCommand(t, d, op)
		if !physical.ConnectionCanceled() {
			t.Fatal("cancellation after setup was indistinguishable from remote closure")
		}
		shutdownTestStatus(d, op)
		shutdownTestDisconnected(d, 1)
		if err := pendingTestResult(t, canceled); err != nil {
			t.Fatal(err)
		}
		op = uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d, op)
		shutdownTestCommandComplete(d, op, 0)
	})
}

func TestConnectResultAbortNilDoesNotPublishSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1}}
		result := resultTestStart(t, h, d, pd, 0)
		resultTestPhysical(d, pd)
		op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d, op)
		h.connsmu.Lock()
		physical := h.conns[1]
		h.connsmu.Unlock()
		physical.Abort(nil)
		if r := resultTestRead(t, result); !errors.Is(r.Err, io.ErrClosedPipe) {
			t.Fatalf("Abort(nil) outcome = %+v", r)
		}
		shutdownTestCommandComplete(d, op, 0)
		op = uint16(cmd.Disconnect{}.Opcode())
		shutdownTestCommand(t, d, op)
		shutdownTestStatus(d, op)
		shutdownTestDisconnected(d, 1)
		op = uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d, op)
		shutdownTestCommandComplete(d, op, 0)
	})
}

func TestConnectResultNormalShutdownIsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1}}
		result := resultTestStart(t, h, d, pd, 0)
		closed := make(chan error, 1)
		go func() { closed <- h.Close() }()
		op := uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, op)
		shutdownTestCommandComplete(d, op, 0)
		pendingTestTerminal(d, 0x02)
		if err := pendingTestResult(t, closed); err != nil {
			t.Fatal(err)
		}
		if r := resultTestRead(t, result); !errors.Is(r.Err, context.Canceled) {
			t.Fatalf("normal shutdown outcome = %+v", r)
		}
		if r := resultTestRead(t, h.ConnectWithResult(pd)); !errors.Is(r.Err, io.ErrClosedPipe) {
			t.Fatalf("connect after shutdown = %+v", r)
		}
	})
}
