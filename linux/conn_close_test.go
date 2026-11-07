package linux

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func TestConnCloseFailureShutsDownHCI(t *testing.T) {
	for _, tt := range []struct {
		name       string
		blockWrite bool
		status     bool
		statusCode byte
	}{
		{name: "missing command status"},
		{name: "missing disconnection complete", status: true},
		{name: "blocked disconnect write", blockWrite: true},
		{name: "rejected disconnect", status: true, statusCode: 0x0c},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, tt.blockWrite)
				c := newConn(h, 1)
				h.connsmu.Lock()
				h.conns[c.attr] = c
				h.connsmu.Unlock()
				result := make(chan error, 1)
				go func() { result <- c.Close() }()
				shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
				if tt.status {
					opcode := uint16(cmd.Disconnect{}.Opcode())
					d.input <- shutdownTestEvent(evt.CommandStatus, tt.statusCode, 1, byte(opcode), byte(opcode>>8))
					synctest.Wait()
				}
				if tt.statusCode == 0 {
					select {
					case err := <-result:
						t.Fatalf("Close() completed before its deadline: %v", err)
					default:
					}
					time.Sleep(hciCloseTimeout)
					synctest.Wait()
				}
				var closeErr error
				select {
				case err := <-result:
					closeErr = err
					if tt.statusCode != 0 {
						if err == nil || errors.Is(err, context.DeadlineExceeded) {
							t.Fatalf("Close() after rejected status = %v, want controller rejection", err)
						}
					} else if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("Close() = %v, want deadline exceeded", err)
					}
				default:
					t.Fatal("Close() did not interrupt its own pending command or transport Write")
				}
				select {
				case <-h.closeDone:
				default:
					t.Fatal("connection failure left the HCI transport and core workers running")
				}
				if err := h.Close(); tt.statusCode != 0 {
					if !errors.Is(err, closeErr) {
						t.Errorf("HCI.Close() = %v, lost controller rejection %v", err, closeErr)
					}
				} else if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("HCI.Close() lost the connection timeout: %v", err)
				}
				if count := d.closeCount.Load(); count != 1 {
					t.Errorf("transport Close calls = %d, want 1", count)
				}
				select {
				case <-h.readerDone:
				default:
					t.Error("HCI shutdown did not join its reader")
				}
				workerDone := make(chan struct{})
				go func() { h.c.Wait(); close(workerDone) }()
				synctest.Wait()
				select {
				case <-workerDone:
				default:
					t.Error("HCI shutdown did not join the command event worker")
				}
				h.connsmu.Lock()
				remaining, closed := len(h.conns), h.transportClosed
				h.connsmu.Unlock()
				if remaining != 0 || !closed {
					t.Errorf("HCI shutdown left connections=%d, transportClosed=%t", remaining, closed)
				}
				if h.startWorker(func() { t.Error("core task ran after HCI shutdown") }) {
					t.Error("closed HCI admitted another core task")
				}
			})
		})
	}
}

func TestConnCloseCompletionKeepsHCIAvailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		c := newConn(h, 1)
		h.connsmu.Lock()
		h.conns[c.attr] = c
		h.connsmu.Unlock()
		result := make(chan error, 1)
		go func() { result <- c.Close() }()
		shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
		shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
		shutdownTestDisconnected(d, c.attr)
		if err := <-result; err != nil {
			t.Fatalf("Close() after physical disconnection = %v", err)
		}
		// Complete the normal automatic advertisement update after disconnect.
		opcode := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		d.input <- shutdownTestEvent(evt.CommandComplete, 1, byte(opcode), byte(opcode>>8), 0)
		synctest.Wait()
		time.Sleep(hciCloseTimeout + time.Second)
		synctest.Wait()
		h.connsmu.Lock()
		closed := h.closed
		h.connsmu.Unlock()
		if closed || d.closeCount.Load() != 0 {
			t.Fatal("completed connection Close left a watchdog that shut down the HCI")
		}
		go func() { _, err := h.SendRawCommand(cmd.Reset{}); result <- err }()
		opcode = uint16(cmd.Reset{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		d.input <- shutdownTestEvent(evt.CommandComplete, 1, byte(opcode), byte(opcode>>8), 0)
		synctest.Wait()
		if err := <-result; err != nil {
			t.Fatalf("HCI command after connection Close = %v", err)
		}
	})
}
