package linux

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func TestHCIMalformedCommandEventsReleasePendingAndQueuedWork(t *testing.T) {
	for _, tt := range []struct {
		name   string
		packet []byte
	}{
		{"status without parameters", shutdownTestEvent(evt.CommandStatus)},
		{"status with truncated opcode", shutdownTestEvent(evt.CommandStatus, 0, 1, 3)},
		{"status with extra parameter", shutdownTestEvent(evt.CommandStatus, 0, 1, 3, 12, 0)},
		{"complete without parameters", shutdownTestEvent(evt.CommandComplete)},
		{"complete with truncated opcode", shutdownTestEvent(evt.CommandComplete, 1, 3)},
		{"status without length", []byte{byte(typEventPkt), evt.CommandStatus}},
		{"complete with wrong length", []byte{byte(typEventPkt), evt.CommandComplete, 4, 1, 3}},
	} {
		for _, zeroCredit := range []bool{false, true} {
			name := tt.name + "/in flight"
			if zeroCredit {
				name = tt.name + "/zero credit"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					h, d := newShutdownTestHCI(t, false)
					if zeroCredit {
						d.input <- shutdownTestEvent(evt.CommandComplete, 0, 0, 0)
						synctest.Wait()
					}
					connection := newConn(h, 1)
					h.conns[1] = connection
					first, queued := make(chan error, 1), make(chan error, 1)
					go func() { _, err := h.SendRawCommand(cmd.Reset{}); first <- err }()
					if !zeroCredit {
						shutdownTestCommand(t, d, uint16(cmd.Reset{}.Opcode()))
					}
					go func() { _, err := h.SendRawCommand(cmd.LEReadBufferSize{}); queued <- err }()
					synctest.Wait()
					done := h.Done()
					d.input <- tt.packet
					synctest.Wait()
					select {
					case <-done:
					default:
						t.Fatal("malformed command event left HCI live")
					}
					cause := hardwareTestClosed(t, h, d, "command")
					for _, result := range []<-chan error{first, queued} {
						if err := pendingTestResult(t, result); err == nil {
							t.Fatal("command succeeded after a malformed command event")
						}
					}
					select {
					case <-connection.Context().Done():
					default:
						t.Fatal("malformed command event left a connection live")
					}
					if len(d.writes) != 0 {
						t.Fatal("queued work reached the transport after a malformed event")
					}
					h.handlePacket(shutdownTestEvent(evt.HardwareError, 0x42))
					if h.closeCause != cause {
						t.Fatal("a later failure replaced the command event error")
					}
				})
			})
		}
	}
}

func TestHCIMalformedCommandEventFinishesPendingConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		source := &PlatData{Address: [6]byte{1}, AddressType: 1}
		result := resultTestStart(t, h, d, source, 0)
		d.input <- shutdownTestEvent(evt.CommandStatus, 0, 1, 3)
		hardwareTestClosed(t, h, d, "command")
		got := resultTestRead(t, result)
		if got.Peripheral != source || got.Err == nil || !strings.Contains(got.Err.Error(), "command") {
			t.Fatalf("pending connection lost its source or fault: %+v", got)
		}
	})
}

func TestHCIMalformedCommandEventInterruptsGracefulClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.scanning = true
		closed := make(chan error, 1)
		go func() { closed <- h.Close() }()
		shutdownTestCommand(t, d, uint16(cmd.LESetScanEnable{}.Opcode()))
		started := time.Now()
		d.input <- shutdownTestEvent(evt.CommandComplete, 1, 12)
		hardwareTestClosed(t, h, d, "command")
		if err := pendingTestResult(t, closed); err == nil || !strings.Contains(err.Error(), "command") {
			t.Fatalf("Close lost the command event error: %v", err)
		}
		if time.Since(started) >= hciGraceTimeout {
			t.Fatal("malformed command event waited for the shutdown deadline")
		}
	})
}

func TestHCIClosedCommandProcessorDoesNotCreateShutdownFault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.c.Close()
		// Closing the transport also closes Cmd before publishing transportClosed.
		// An already received valid response in that interval is not a parse error.
		for _, packet := range [][]byte{
			shutdownTestEvent(evt.CommandStatus, 0, 1, 3, 12),
			shutdownTestEvent(evt.CommandComplete, 1, 3, 12, 0),
		} {
			h.handlePacket(packet)
		}
		if h.closeCause != nil {
			t.Fatalf("closed command delivery created a fault: %v", h.closeCause)
		}
		if err := h.Close(); err != nil {
			t.Fatalf("normal Close = %v", err)
		}
		if !errors.Is(h.c.HandleStatus([]byte{0, 1, 3, 12}), io.ErrClosedPipe) {
			t.Fatal("closed command processor accepted a late response")
		}
		if d.closeCount.Load() != 1 {
			t.Fatal("normal Close did not release the transport exactly once")
		}
	})
}
