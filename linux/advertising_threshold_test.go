package linux

import (
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestAdvertisingRemainsOffBeyondConnectionThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.maxConn, h.adv = 1, true
		incoming := make(chan *PlatData, 1)
		h.AcceptMasterHandler = func(pd *PlatData) { incoming <- pd }
		connectionNameComplete(d, &PlatData{Address: [6]byte{1}}, 1, 1)
		select {
		case <-incoming:
		default:
			t.Fatal("incoming connection was not published")
		}
		pendingTestNoCommand(t, d)
		// Outgoing Connect is permitted while an incoming link is already live.
		source := &PlatData{Address: [6]byte{2}}
		result := resultTestStart(t, h, d, source, 0)
		connectionNameComplete(d, source, 2, 0)
		select {
		case packet := <-d.writes:
			t.Errorf("advertising enabled beyond maxConn=1: %x", packet)
			shutdownTestCommandComplete(d, uint16(cmd.LESetAdvertiseEnable{}.Opcode()), 0)
		default:
		}
		if got := resultTestRead(t, result); got.Err != nil || got.Peripheral.Conn == nil {
			t.Fatalf("outgoing connection changed its admission behavior: %+v", got)
		}
		// Returning below the threshold must still restore requested advertising.
		shutdownTestDisconnected(d, 2)
		pendingTestNoCommand(t, d)
		shutdownTestDisconnected(d, 1)
		raw := shutdownTestCommand(t, d, uint16(cmd.LESetAdvertiseEnable{}.Opcode()))
		if len(raw) != 5 || raw[4] != 1 {
			t.Fatalf("advertising restoration below threshold = %x", raw)
		}
		shutdownTestCommandComplete(d, uint16(cmd.LESetAdvertiseEnable{}.Opcode()), 0)
	})
}
