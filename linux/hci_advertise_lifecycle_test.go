package linux

import (
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestHCIStoppedAdvertisingIsNotRestoredAfterDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.connsmu.Lock()
		h.conns[1] = newConn(h, 1)
		h.connsmu.Unlock()
		stopped := make(chan error, 1)
		go func() { stopped <- h.SetAdvertiseEnable(false) }()
		op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		if packet := shutdownTestCommand(t, d, op); packet[4] != 0 {
			t.Fatalf("stop advertising command = %x", packet)
		}
		shutdownTestCommandComplete(d, op, 0)
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		shutdownTestDisconnected(d, 1)
		pendingTestNoCommand(t, d)
	})
}

func TestHCIClientConnectionDoesNotStartAdvertising(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		pd := &PlatData{Address: [6]byte{1}}
		result := resultTestStart(t, h, d, pd, 0)
		resultTestPhysical(d, pd)
		pendingTestNoCommand(t, d)
		if r := resultTestRead(t, result); r.Err != nil || r.Peripheral.Conn == nil {
			t.Fatalf("client connection = %+v", r)
		}
	})
}

func TestHCIAdvertisingStopOrdersWithAutomaticRestore(t *testing.T) {
	for _, restoreFirst := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			h.advmu.Lock()
			h.adv = true
			h.advmu.Unlock()
			first, second := make(chan error, 1), make(chan error, 1)
			if restoreFirst {
				go func() { first <- h.setAdvertiseEnable(true) }()
			} else {
				go func() { first <- h.SetAdvertiseEnable(false) }()
			}
			op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
			packet := shutdownTestCommand(t, d, op)
			if packet[4] != btoi(restoreFirst) {
				t.Fatalf("first advertising command = %x", packet)
			}
			if restoreFirst {
				go func() { second <- h.SetAdvertiseEnable(false) }()
			} else {
				go func() { second <- h.setAdvertiseEnable(true) }()
			}
			synctest.Wait()
			shutdownTestCommandComplete(d, op, 0)
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			if restoreFirst {
				if packet := shutdownTestCommand(t, d, op); packet[4] != 0 {
					t.Fatalf("stop following restore = %x", packet)
				}
				shutdownTestCommandComplete(d, op, 0)
			}
			if err := <-second; err != nil {
				t.Fatal(err)
			}
			pendingTestNoCommand(t, d)
		})
	}
}

func TestHCIAdvertisingConfigurationFinishesBeforeStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.advmu.Lock()
		h.adv = true
		h.advmu.Unlock()
		configured, stopped := make(chan error, 1), make(chan error, 1)
		go func() { configured <- h.SendCmdWithAdvOff(cmd.LESetAdvertisingParameters{}) }()
		op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		if packet := shutdownTestCommand(t, d, op); packet[4] != 0 {
			t.Fatalf("configuration pause = %x", packet)
		}
		go func() { stopped <- h.SetAdvertiseEnable(false) }()
		synctest.Wait()
		shutdownTestCommandComplete(d, op, 0)
		configOp := uint16(cmd.LESetAdvertisingParameters{}.Opcode())
		shutdownTestCommand(t, d, configOp)
		shutdownTestCommandComplete(d, configOp, 0x12)
		if packet := shutdownTestCommand(t, d, op); packet[4] != 1 {
			t.Fatalf("restore after failed configuration = %x", packet)
		}
		shutdownTestCommandComplete(d, op, 0)
		if err := <-configured; err == nil {
			t.Fatal("configuration failure was ignored")
		}
		if packet := shutdownTestCommand(t, d, op); packet[4] != 0 {
			t.Fatalf("stop after configuration = %x", packet)
		}
		shutdownTestCommandComplete(d, op, 0)
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		pendingTestNoCommand(t, d)
	})
}

func TestHCIShutdownReleasesQueuedAdvertisingOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, true)
		first, queued, closed := make(chan error, 1), make(chan error, 1), make(chan error, 1)
		go func() { first <- h.SetAdvertiseEnable(true) }()
		shutdownTestCommand(t, d, uint16(cmd.LESetAdvertiseEnable{}.Opcode()))
		go func() { queued <- h.SetAdvertiseEnable(false) }()
		synctest.Wait()
		go func() { closed <- h.Close() }()
		synctest.Wait()
		select {
		case err := <-queued:
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("queued advertising operation = %v", err)
			}
		default:
			t.Fatal("shutdown did not cancel queued advertising operation")
		}
		time.Sleep(hciCloseTimeout)
		synctest.Wait()
		if err := <-closed; err == nil {
			t.Fatal("blocked transport shutdown returned success")
		}
		if err := <-first; err == nil {
			t.Fatal("interrupted advertising write returned success")
		}
	})
}
