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

func TestHCIShutdownPreservesQueuedDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		c := newConn(h, 1)
		h.connsmu.Lock()
		h.conns[c.attr] = c
		h.connsmu.Unlock()

		ordinaryResult := make(chan error, 1)
		go func() {
			_, err := h.SendRawCommand(cmd.LEReadBufferSize{})
			ordinaryResult <- err
		}()
		opcode := uint16(cmd.LEReadBufferSize{}.Opcode())
		shutdownTestCommand(t, d, opcode)

		connResult := make(chan error, 1)
		go func() { connResult <- c.Close() }()
		synctest.Wait() // Disconnect is queued behind the ordinary command.

		h.requestClose(nil)
		hciResult := make(chan error, 1)
		go func() { hciResult <- h.Close() }()
		if _, err := h.SendRawCommand(cmd.LEReadBufferSize{}); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("new ordinary command during shutdown = %v, want closed pipe", err)
		}
		synctest.Wait()
		select {
		case err := <-hciResult:
			t.Fatalf("HCI.Close() abandoned the queued Disconnect before the ordinary response: %v", err)
		default:
		}

		d.input <- shutdownTestEvent(evt.CommandComplete, 1, byte(opcode), byte(opcode>>8), 0, 27, 0, 14)
		synctest.Wait()
		if err := <-ordinaryResult; err != nil {
			t.Fatalf("in-flight ordinary command = %v, want nil", err)
		}
		packet := shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
		if len(packet) != 7 || packet[4] != byte(c.attr) || packet[5] != byte(c.attr>>8) {
			t.Fatalf("Disconnect packet = %x, want handle 0x%04x", packet, c.attr)
		}
		shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
		for _, result := range []struct {
			name string
			ch   <-chan error
		}{
			{"connection", connResult},
			{"HCI", hciResult},
		} {
			select {
			case err := <-result.ch:
				t.Fatalf("%s Close() completed on Command Status alone: %v", result.name, err)
			default:
			}
		}
		select {
		case packet := <-d.writes:
			t.Fatalf("extra command after accepting Disconnect: %x", packet)
		default:
		}

		shutdownTestDisconnected(d, c.attr)
		for _, result := range []struct {
			name string
			ch   <-chan error
		}{
			{"connection", connResult},
			{"HCI", hciResult},
		} {
			select {
			case err := <-result.ch:
				if err != nil {
					t.Errorf("%s Close() after DisconnectionComplete = %v, want nil", result.name, err)
				}
			default:
				t.Errorf("%s Close() did not finish after DisconnectionComplete", result.name)
			}
		}
		select {
		case packet := <-d.writes:
			t.Fatalf("extra command after shutdown completed: %x", packet)
		default:
		}
		if got := d.closeCount.Load(); got != 1 {
			t.Errorf("transport closes = %d, want 1", got)
		}
	})
}

func TestHCIShutdownTimesOutQueuedDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		c := newConn(h, 1)
		h.connsmu.Lock()
		h.conns[c.attr] = c
		h.connsmu.Unlock()

		ordinaryResult := make(chan error, 1)
		go func() {
			_, err := h.SendRawCommand(cmd.LEReadBufferSize{})
			ordinaryResult <- err
		}()
		shutdownTestCommand(t, d, uint16(cmd.LEReadBufferSize{}.Opcode()))
		connResult := make(chan error, 1)
		go func() { connResult <- c.Close() }()
		synctest.Wait()

		started := time.Now()
		h.requestClose(nil)
		hciResult := make(chan error, 1)
		go func() { hciResult <- h.Close() }()
		select {
		case err := <-hciResult:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("HCI.Close() with an unanswered command = %v, want deadline exceeded", err)
			}
		case <-time.After(hciCloseTimeout):
			t.Fatal("HCI.Close() exceeded the shared shutdown budget")
		}
		if elapsed := time.Since(started); elapsed > hciCloseTimeout {
			t.Errorf("shutdown took %s, exceeds total budget %s", elapsed, hciCloseTimeout)
		}
		synctest.Wait()
		for _, result := range []struct {
			name string
			ch   <-chan error
		}{
			{"in-flight ordinary command", ordinaryResult},
			{"queued connection close", connResult},
		} {
			select {
			case err := <-result.ch:
				if err == nil {
					t.Errorf("%s returned success after forced shutdown", result.name)
				}
			default:
				t.Errorf("%s remained blocked after shutdown", result.name)
			}
		}
		h.connsmu.Lock()
		remaining, closed := len(h.conns), h.transportClosed
		h.connsmu.Unlock()
		if remaining != 0 || !closed {
			t.Errorf("shutdown left connections=%d, transportClosed=%t", remaining, closed)
		}
		select {
		case <-h.readerDone:
		default:
			t.Error("shutdown did not join its reader")
		}
		if got := d.closeCount.Load(); got != 1 {
			t.Errorf("transport closes = %d, want 1", got)
		}
		select {
		case packet := <-d.writes:
			t.Fatalf("queued Disconnect was written without completing the ordinary command: %x", packet)
		default:
		}
	})
}
