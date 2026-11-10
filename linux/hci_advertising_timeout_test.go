package linux

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestHCIDirectedAdvertisingTimeoutPreservesOutgoingConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.maxConn, h.adv = 1, true
		pd := &PlatData{Address: [6]byte{1, 2, 3, 4, 5, 6}, Name: "requested", Data: []byte("snapshot")}
		result := resultTestStart(t, h, d, pd, 0)
		h.connsmu.Lock()
		original := h.pending
		h.connsmu.Unlock()

		pendingTestTerminal(d, 0x3c)
		select {
		case got := <-result:
			t.Fatalf("advertising timeout completed outgoing connection: %+v", got)
		default:
		}
		h.connsmu.Lock()
		retained := h.pending == original
		h.connsmu.Unlock()
		if !retained {
			t.Fatal("advertising timeout released the outgoing attempt")
		}
		pendingTestStillReserved(t, h)

		resultTestPhysical(d, pd)
		got := resultTestRead(t, result)
		if got.Err != nil || got.Peripheral == nil || got.Peripheral.Conn == nil {
			t.Fatalf("outgoing success after advertising timeout = %+v", got)
		}
		c := got.Peripheral.Conn.(*conn)
		if c.origin != pd || got.Peripheral.Name != pd.Name || !bytes.Equal(got.Peripheral.Data, pd.Data) {
			t.Fatalf("outgoing success lost original request: origin=%p want=%p peripheral=%+v", c.origin, pd, got.Peripheral)
		}
		pendingTestNoCommand(t, d)
	})
}

func TestHCIDirectedAdvertisingTimeoutDoesNotCompleteCancellation(t *testing.T) {
	for _, afterCancelComplete := range []bool{false, true} {
		name := "before_cancel_complete"
		if afterCancelComplete {
			name = "after_cancel_complete"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				pd := &PlatData{Address: [6]byte{1}}
				result := resultTestStart(t, h, d, pd, 0)
				h.connsmu.Lock()
				original := h.pending
				h.connsmu.Unlock()
				canceled := connectionIdentityCancel(h, pd)
				opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
				shutdownTestCommand(t, d, opcode)
				if afterCancelComplete {
					shutdownTestCommandComplete(d, opcode, 0)
				}

				pendingTestTerminal(d, 0x3c)
				select {
				case <-original.done:
					t.Fatal("advertising timeout ended the outgoing physical attempt")
				default:
				}
				if !afterCancelComplete {
					shutdownTestCommandComplete(d, opcode, 0)
				}
				pendingTestBlocked(t, canceled)
				pendingTestStillReserved(t, h)
				pendingTestTerminal(d, 0x02)
				if err := pendingTestResult(t, canceled); err != nil {
					t.Fatalf("real cancellation event = %v", err)
				}
				if got := resultTestRead(t, result); got.Peripheral != pd || !errors.Is(got.Err, context.Canceled) {
					t.Fatalf("canceled result = %+v", got)
				}
				h.connsmu.Lock()
				closed, pending := h.closed, h.pending
				h.connsmu.Unlock()
				if closed || pending != nil {
					t.Fatalf("real cancellation left closed=%v pending=%v", closed, pending != nil)
				}
				pendingTestNoCommand(t, d)
			})
		})
	}
}

func TestHCIDirectedAdvertisingTimeoutWithoutOutgoingConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		accepted := make(chan *PlatData, 1)
		h.AcceptMasterHandler = func(pd *PlatData) { accepted <- pd }
		h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
		pendingTestTerminal(d, 0x3c)
		h.connsmu.Lock()
		unchanged := !h.closed && h.pending == nil && len(h.conns) == 0
		h.connsmu.Unlock()
		if !unchanged {
			t.Fatal("advertising timeout changed the connection lifecycle")
		}
		select {
		case <-accepted:
			t.Fatal("advertising timeout invoked a connection callback")
		default:
		}
		pendingTestNoCommand(t, d)

		pd := &PlatData{Address: [6]byte{1}, AddressType: 1}
		result := resultTestStart(t, h, d, pd, 0)
		// A real failed Create Connection still has invalid address and role
		// fields. Its status alone must finish the pending outgoing attempt.
		pendingTestTerminal(d, 0x3e)
		got := resultTestRead(t, result)
		if got.Peripheral != pd || got.Err == nil || !strings.Contains(got.Err.Error(), "0x3e") {
			t.Fatalf("real outgoing failure after advertising timeout = %+v", got)
		}
	})
}
