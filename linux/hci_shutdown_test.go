package linux

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

type shutdownTestTransport struct {
	input      chan []byte
	readError  chan error
	writes     chan []byte
	closed     chan struct{}
	blockWrite bool
	aclError   error
	closeOnce  sync.Once
	closeCount atomic.Int32
}

func (d *shutdownTestTransport) Read(b []byte) (int, error) {
	select {
	case packet, ok := <-d.input:
		if !ok {
			return 0, io.EOF
		}
		return copy(b, packet), nil
	case <-d.closed:
		return 0, io.EOF
	case err := <-d.readError:
		return 0, err
	}
}

func (d *shutdownTestTransport) Write(b []byte) (int, error) {
	select {
	case <-d.closed:
		return 0, io.ErrClosedPipe
	case d.writes <- append([]byte(nil), b...):
	}
	if d.blockWrite {
		<-d.closed
		return 0, io.ErrClosedPipe
	}
	if len(b) != 0 && b[0] == byte(typACLDataPkt) && d.aclError != nil {
		return 0, d.aclError
	}
	return len(b), nil
}

func (d *shutdownTestTransport) Close() error {
	d.closeOnce.Do(func() {
		d.closeCount.Add(1)
		close(d.closed)
	})
	return nil
}

func newShutdownTestHCI(t *testing.T, blockWrite bool) (*HCI, *shutdownTestTransport) {
	t.Helper()
	d := &shutdownTestTransport{
		input: make(chan []byte, 16), readError: make(chan error, 1), writes: make(chan []byte, 16),
		closed: make(chan struct{}), blockWrite: blockWrite,
	}
	h := &HCI{
		d: d, c: cmd.NewCmd(d), e: evt.NewEvt(),
		connsmu: &sync.Mutex{}, conns: make(map[uint16]*conn),
		plistmu: &sync.Mutex{}, plist: make(map[bdaddr]*PlatData),
		advmu: &sync.Mutex{}, maxConn: 4,
		bufSize: 27, bufCnt: make(chan struct{}, 14),
	}
	h.e.HandleEvent(evt.CommandStatus, evt.HandlerFunc(h.c.HandleStatus))
	h.e.HandleEvent(evt.CommandComplete, evt.HandlerFunc(h.c.HandleComplete))
	h.e.HandleEvent(evt.DisconnectionComplete, evt.HandlerFunc(h.handleDisconnectionComplete))
	h.e.HandleEvent(evt.LEMeta, evt.HandlerFunc(h.handleLEMeta))
	go h.mainLoop()
	synctest.Wait()
	t.Cleanup(func() {
		// Release the controlled transport even when an assertion fails before
		// shutdown can interrupt it, then wait for all owned tasks to exit.
		d.Close()
		<-h.requestClose(io.ErrClosedPipe)
	})
	return h, d
}

func shutdownTestEvent(code byte, payload ...byte) []byte {
	return append([]byte{byte(typEventPkt), code, byte(len(payload))}, payload...)
}

func shutdownTestStatus(d *shutdownTestTransport, opcode uint16) {
	d.input <- shutdownTestEvent(evt.CommandStatus, 0, 1, byte(opcode), byte(opcode>>8))
	synctest.Wait()
}

func shutdownTestDisconnected(d *shutdownTestTransport, handle uint16) {
	d.input <- shutdownTestEvent(evt.DisconnectionComplete, 0, byte(handle), byte(handle>>8), 0x13)
	synctest.Wait()
}

func shutdownTestCommand(t *testing.T, d *shutdownTestTransport, opcode uint16) []byte {
	t.Helper()
	select {
	case packet := <-d.writes:
		if len(packet) < 4 || packet[0] != 1 || uint16(packet[1])|uint16(packet[2])<<8 != opcode {
			t.Fatalf("command = %x, want opcode 0x%04x", packet, opcode)
		}
		return packet
	case <-time.After(hciCloseTimeout):
		t.Fatalf("command 0x%04x was not sent", opcode)
		return nil
	}
}

func TestHCIShutdownInterruptsBlockedTransportWrite(t *testing.T) {
	for _, pendingCommand := range []bool{false, true} {
		name := "disconnect write"
		if pendingCommand {
			name = "ordinary command write"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, true)
				c := newConn(h, 1)
				h.conns[c.attr] = c
				workerDone := make(chan struct{})
				workerResult := make(chan error, 1)
				if pendingCommand {
					if !h.startWorker(func() {
						defer close(workerDone)
						_, err := h.c.Send(cmd.Reset{})
						workerResult <- err
					}) {
						t.Fatal("ordinary command worker was not started")
					}
					shutdownTestCommand(t, d, uint16(cmd.Reset{}.Opcode()))
				}
				result := make(chan error, 1)
				started := time.Now()
				go func() { result <- h.Close() }()
				if !pendingCommand {
					shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
				}
				time.Sleep(hciCloseTimeout)
				synctest.Wait()
				select {
				case err := <-result:
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("Close() = %v, want deadline exceeded", err)
					}
				default:
					t.Error("Close() did not interrupt the blocked transport Write")
					// Rescue a broken implementation before the bubble exits.
					d.Close()
					synctest.Wait()
				}
				if elapsed := time.Since(started); elapsed > hciCloseTimeout {
					t.Errorf("shutdown took %s, exceeds total budget %s", elapsed, hciCloseTimeout)
				}
				if pendingCommand {
					select {
					case <-workerDone:
					default:
						t.Error("Close() returned before the command worker exited")
					}
					select {
					case err := <-workerResult:
						if !errors.Is(err, io.ErrClosedPipe) {
							t.Errorf("blocked command result = %v, want closed pipe", err)
						}
					default:
						t.Error("command worker did not publish its result")
					}
				}
				select {
				case <-h.readerDone:
				default:
					t.Error("Close() returned before the HCI reader exited")
				}
				if got := d.closeCount.Load(); got != 1 {
					t.Errorf("physical transport closes = %d, want 1", got)
				}
			})
		})
	}
}

func TestHCIShutdownRequiresDisconnectionComplete(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "status without complete"
		if complete {
			name = "status and complete"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				c := newConn(h, 1)
				h.conns[c.attr] = c
				result := make(chan error, 1)
				go func() { result <- h.Close() }()
				shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
				shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
				select {
				case err := <-result:
					t.Fatalf("Close() finished on Command Status alone: %v", err)
				default:
				}
				if complete {
					shutdownTestDisconnected(d, c.attr)
				} else {
					time.Sleep(hciCloseTimeout)
					synctest.Wait()
				}
				select {
				case err := <-result:
					if complete && err != nil {
						t.Errorf("Close() after complete = %v, want nil", err)
					}
					if !complete && !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("Close() without complete = %v, want deadline exceeded", err)
					}
				default:
					t.Fatal("Close() did not finish after link termination or deadline")
				}
				select {
				case <-c.linkDone:
				default:
					t.Error("link was not terminated during shutdown")
				}
			})
		})
	}
}

func TestHCIShutdownSharesDeadlineAcrossConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		for handle := uint16(1); handle <= 2; handle++ {
			h.conns[handle] = newConn(h, handle)
		}
		result := make(chan error, 1)
		started := time.Now()
		go func() { result <- h.Close() }()
		first := shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
		shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
		time.Sleep(hciGraceTimeout / 2)
		shutdownTestDisconnected(d, uint16(first[4])|uint16(first[5])<<8)
		second := shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
		if first[4] == second[4] && first[5] == second[5] {
			t.Fatalf("second disconnect reused the first handle: %x", second)
		}
		shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
		err := <-result // The second link never sends DisconnectionComplete.
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Close() = %v, want deadline exceeded", err)
		}
		if elapsed := time.Since(started); elapsed > hciCloseTimeout {
			t.Errorf("shutdown restarted the deadline for the second connection: %s", elapsed)
		}
	})
}

func TestHCIShutdownCleansUpLateConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		accepted := make(chan struct{}, 1)
		h.AcceptMasterHandler = func(*PlatData) { accepted <- struct{}{} }
		first := newConn(h, 1)
		h.conns[first.attr] = first
		result := make(chan error, 1)
		go func() { result <- h.Close() }()
		shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
		shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))

		connection := make([]byte, 19)
		connection[0], connection[2], connection[4], connection[12] = 1, 2, 1, 0x18
		d.input <- shutdownTestEvent(evt.LEMeta, connection...)
		synctest.Wait()
		h.connsmu.Lock()
		late := h.conns[2]
		h.connsmu.Unlock()
		if late == nil {
			t.Fatal("late connection was not registered for cleanup")
		}
		select {
		case <-late.done:
		default:
			t.Fatal("late connection was allowed to perform application IO")
		}
		shutdownTestDisconnected(d, first.attr)
		packet := shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
		if packet[4] != 2 || packet[5] != 0 {
			t.Fatalf("late disconnect command = %x, want handle 2", packet)
		}
		shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
		shutdownTestDisconnected(d, 2)
		if err := <-result; err != nil {
			t.Fatalf("Close() after late connection cleanup = %v", err)
		}
		select {
		case <-accepted:
			t.Fatal("late connection fired Connected during shutdown")
		default:
		}
	})
}

func TestHCIShutdownFromReaderOrWorker(t *testing.T) {
	for _, source := range []string{"reader", "worker"} {
		t.Run(source, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				h.conns[1] = newConn(h, 1)
				wantErr := io.EOF
				workerDone := make(chan struct{})
				if source == "reader" {
					close(d.input)
				} else {
					wantErr = errors.New("connection worker failed")
					if !h.startWorker(func() {
						defer close(workerDone)
						h.requestClose(wantErr)
					}) {
						t.Fatal("failure worker was not started")
					}
				}
				synctest.Wait()
				if err := h.Close(); !errors.Is(err, wantErr) {
					t.Fatalf("Close() after %s error = %v, want %v", source, err, wantErr)
				}
				select {
				case <-h.readerDone:
				default:
					t.Error("reader was not joined")
				}
				if source == "worker" {
					select {
					case <-workerDone:
					default:
						t.Error("failure worker waited on its own shutdown")
					}
				}
				if len(h.conns) != 0 {
					t.Errorf("shutdown left %d registered connections", len(h.conns))
				}
			})
		})
	}
}

func TestHCIShutdownReportsControlCommandFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.scanning = true
		result := make(chan error, 1)
		go func() { result <- h.Close() }()
		opcode := uint16(cmd.LESetScanEnable{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		d.input <- shutdownTestEvent(evt.CommandComplete, 1, byte(opcode), byte(opcode>>8), 0x0c)
		synctest.Wait()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("Close() ignored rejected stop-scan command")
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Close() reported a deadline instead of the controller rejection: %v", err)
			}
		default:
			t.Fatal("Close() remained blocked after the controller rejected stop-scan")
		}
	})
}

type deadlineReadFailureTransport struct{ *shutdownTestTransport }

func (d *deadlineReadFailureTransport) SetDeadline(time.Time) error {
	// Deliver the poller's timeout before the shutdown context timer runs.
	d.readError <- os.ErrDeadlineExceeded
	return nil
}

func TestHCIShutdownPreservesTransportDeadlineCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := &shutdownTestTransport{
			input: make(chan []byte, 4), readError: make(chan error, 1),
			writes: make(chan []byte, 4), closed: make(chan struct{}),
		}
		d := &deadlineReadFailureTransport{base}
		h := &HCI{
			d: d, c: cmd.NewCmd(d), e: evt.NewEvt(),
			connsmu: &sync.Mutex{}, conns: make(map[uint16]*conn),
		}
		go h.mainLoop()
		synctest.Wait()
		defer func() {
			d.Close()
			<-h.requestClose(io.ErrClosedPipe)
		}()
		h.conns[1] = newConn(h, 1)
		if err := h.Close(); !errors.Is(err, os.ErrDeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close() = %v, want transport and context deadline causes", err)
		}
	})
}

func shutdownTestCommandComplete(d *shutdownTestTransport, opcode uint16, status byte) {
	d.input <- shutdownTestEvent(evt.CommandComplete, 1, byte(opcode), byte(opcode>>8), status)
	synctest.Wait()
}

func TestHCIConnectionRejectsExhaustedCallbackCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		release := make(chan struct{})
		defer close(release)
		for i := 0; i < cap(h.callbacks); i++ {
			if !h.callout(func() { <-release }) {
				t.Fatal("callback slot was unexpectedly unavailable")
			}
		}
		accepted := make(chan struct{}, 1)
		h.AcceptMasterHandler = func(*PlatData) { accepted <- struct{}{} }
		connection := make([]byte, 19)
		connection[0], connection[2], connection[4], connection[12] = 1, 1, 1, 0x18
		d.input <- shutdownTestEvent(evt.LEMeta, connection...)
		advertisingOpcode := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d, advertisingOpcode)
		shutdownTestCommandComplete(d, advertisingOpcode, 0)
		packet := shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
		if packet[4] != 1 || packet[5] != 0 {
			t.Fatalf("rejected connection cleanup = %x, want handle 1", packet)
		}
		shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
		shutdownTestDisconnected(d, 1)
		select {
		case <-accepted:
			t.Fatal("Connected ran after callback capacity was exhausted")
		default:
		}
		h.connsmu.Lock()
		remaining := len(h.conns)
		h.connsmu.Unlock()
		if remaining != 0 {
			t.Fatalf("rejected connection remained registered: %d", remaining)
		}
	})
}

func TestHCIConnectionInitializationFailureCleansUp(t *testing.T) {
	for _, stage := range []string{"advertising", "connection parameters"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				if stage == "connection parameters" {
					d.aclError = errors.New("connection parameter transport failed")
				}
				accepted := make(chan struct{}, 1)
				h.AcceptMasterHandler = func(*PlatData) { accepted <- struct{}{} }
				connection := make([]byte, 19)
				connection[0], connection[2], connection[4], connection[12] = 1, 1, 1, 0x18
				if stage == "connection parameters" {
					connection[12] = 0x19 // Request a connection parameter update.
				}
				d.input <- shutdownTestEvent(evt.LEMeta, connection...)
				opcode := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
				shutdownTestCommand(t, d, opcode)
				status := byte(0x0c)
				if stage == "connection parameters" {
					status = 0
				}
				shutdownTestCommandComplete(d, opcode, status)
				if stage == "connection parameters" {
					select {
					case packet := <-d.writes:
						if len(packet) == 0 || packet[0] != byte(typACLDataPkt) {
							t.Fatalf("connection parameter request = %x, want ACL packet", packet)
						}
					case <-time.After(hciCloseTimeout):
						t.Fatal("connection parameter update was not attempted")
					}
					if err := h.Close(); !errors.Is(err, d.aclError) {
						t.Fatalf("Close() after transport failure = %v, want original write error", err)
					}
				} else {
					shutdownTestCommand(t, d, uint16(cmd.Disconnect{}.Opcode()))
					shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
					shutdownTestDisconnected(d, 1)
				}
				select {
				case <-accepted:
					t.Fatal("failed connection initialization fired Connected")
				default:
				}
				h.connsmu.Lock()
				remaining := len(h.conns)
				h.connsmu.Unlock()
				if remaining != 0 {
					t.Fatalf("failed connection remained registered: %d", remaining)
				}
			})
		})
	}
}

func TestHCIShutdownStopsControllerActivityAndRejectsNewCommands(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.connsmu.Lock()
		h.scanning = true
		h.pending = newConnectionAttempt(&PlatData{})
		h.pending.created()
		h.connsmu.Unlock()
		h.advmu.Lock()
		h.adv = true
		h.advmu.Unlock()
		h.requestClose(nil)
		if _, err := h.SendRawCommand(cmd.Reset{}); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("new command during shutdown = %v, want closed pipe", err)
		}
		result := make(chan error, 1)
		go func() { result <- h.Close() }()
		for _, cp := range []cmd.CmdParam{cmd.LESetScanEnable{}, cmd.LESetAdvertiseEnable{}, cmd.LECreateConnCancel{}} {
			op := uint16(cp.Opcode())
			shutdownTestCommand(t, d, op)
			d.input <- shutdownTestEvent(evt.CommandComplete, 1, byte(op), byte(op>>8), 0)
		}
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("Close completed while connection establishment was still pending: %v", err)
		default:
		}
		// LE Create Connection Cancel is followed by a failed completion of
		// the original connection attempt. It must wake the shared shutdown.
		failedConnection := make([]byte, 19)
		failedConnection[0], failedConnection[1] = byte(evt.LEConnectionComplete), 0x02
		d.input <- shutdownTestEvent(evt.LEMeta, failedConnection...)
		synctest.Wait()
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("Close did not finish after the pending connection was canceled")
		}
		select {
		case packet := <-d.writes:
			t.Fatalf("unexpected command during shutdown: %x", packet)
		default:
		}
	})
}
