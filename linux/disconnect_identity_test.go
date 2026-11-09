package linux

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func TestUnsentDisconnectDoesNotTargetReusedHandle(t *testing.T) {
	for _, zeroCredit := range []bool{false, true} {
		name := "queued"
		if zeroCredit {
			name = "zero_credit"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				old := newConn(h, 1)
				h.conns[1] = old
				ordinary := uint16(cmd.LEReadBufferSize{}.Opcode())
				busy := make(chan error, 1)
				if zeroCredit {
					d.input <- shutdownTestEvent(evt.CommandComplete, 0, 0, 0)
					synctest.Wait()
				} else {
					go func() { _, err := h.SendRawCommand(cmd.LEReadBufferSize{}); busy <- err }()
					shutdownTestCommand(t, d, ordinary)
				}
				closing := make(chan error, 1)
				go func() { closing <- old.Close() }()
				synctest.Wait()
				shutdownTestDisconnected(d, 1)
				accepted := make(chan *PlatData, 1)
				h.AcceptMasterHandler = func(pd *PlatData) { accepted <- pd }
				connectionNameComplete(d, &PlatData{Address: [6]byte{2}, AddressType: 1}, 1, 1)
				fresh := (<-accepted).Conn.(*conn)
				closed := false
				select {
				case err := <-closing:
					closed = true
					if err != nil {
						t.Errorf("Close after physical disconnection = %v", err)
					}
				default:
					t.Error("retired connection is still waiting to send its obsolete Disconnect")
				}
				if zeroCredit {
					d.input <- shutdownTestEvent(evt.CommandComplete, 1, 0, 0)
					synctest.Wait()
				} else {
					shutdownTestCommandComplete(d, ordinary, 0)
					if err := <-busy; err != nil {
						t.Fatal(err)
					}
				}
				select {
				case packet := <-d.writes:
					t.Errorf("retired connection sent a command targeting the new live handle: %x", packet)
					// Let the old implementation finish after demonstrating the bug.
					shutdownTestStatus(d, uint16(cmd.Disconnect{}.Opcode()))
				default:
				}
				if !closed {
					if err := <-closing; err != nil {
						t.Errorf("retired Close = %v", err)
					}
				}
				select {
				case <-fresh.Context().Done():
					t.Fatal("retiring the old connection stopped its replacement")
				default:
				}
			})
		})
	}
}

func TestDisconnectPollRetryCannotTargetReusedHandle(t *testing.T) {
	for _, zeroCreditUpdate := range []bool{false, true} {
		t.Run(fmt.Sprintf("zero_credit_update_%t", zeroCreditUpdate), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newSubmissionTestHCI(t, true)
				old := newConn(h, 1)
				h.conns[1] = old
				closing := make(chan error, 1)
				go func() { closing <- old.Close() }()
				<-d.beforeRetry
				if zeroCreditUpdate {
					d.input <- shutdownTestEvent(evt.CommandComplete, 0, 0, 0)
					synctest.Wait()
				}
				shutdownTestDisconnected(d.shutdownTestTransport, 1)
				accepted := make(chan *PlatData, 1)
				h.AcceptMasterHandler = func(pd *PlatData) { accepted <- pd }
				connectionNameComplete(d.shutdownTestTransport, &PlatData{Address: [6]byte{2}, AddressType: 1}, 1, 1)
				fresh := (<-accepted).Conn.(*conn)
				close(d.resume)
				if err := <-closing; err != nil {
					t.Fatalf("Close after poll retirement = %v", err)
				}
				pendingTestNoCommand(t, d.shutdownTestTransport)
				select {
				case <-fresh.Context().Done():
					t.Fatal("retired poll retry stopped its replacement")
				default:
				}
				// A retired reservation refunds its credit, except when a newer
				// absolute allowance has superseded that reservation.
				result := make(chan error, 1)
				go func() { _, err := h.SendRawCommand(cmd.Reset{}); result <- err }()
				if zeroCreditUpdate {
					pendingTestNoCommand(t, d.shutdownTestTransport)
					d.input <- shutdownTestEvent(evt.CommandComplete, 1, 0, 0)
					synctest.Wait()
				}
				opcode := uint16(cmd.Reset{}.Opcode())
				shutdownTestCommand(t, d.shutdownTestTransport, opcode)
				shutdownTestCommandComplete(d.shutdownTestTransport, opcode, 0)
				if err := <-result; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestSubmittedDisconnectStillWaitsForItsCommandStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		old := newConn(h, 1)
		h.conns[1] = old
		closing := make(chan error, 1)
		go func() { closing <- old.Close() }()
		opcode := uint16(cmd.Disconnect{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		// The physical event may win the race with the command status. Since
		// the packet is submitted, retiring its owner must not cancel the Cmd.
		shutdownTestDisconnected(d, 1)
		select {
		case err := <-closing:
			t.Fatalf("submitted Disconnect stopped waiting for its status: %v", err)
		default:
		}
		shutdownTestStatus(d, opcode)
		if err := <-closing; err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() { _, err := h.SendRawCommand(cmd.Reset{}); result <- err }()
		opcode = uint16(cmd.Reset{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		if err := <-result; err != nil {
			t.Fatalf("owner retirement stopped the submitted command processor: %v", err)
		}
	})
}

type wrappedRetiredDisconnectTransport struct {
	*shutdownTestTransport
	retire func()
}

func (d *wrappedRetiredDisconnectTransport) writeACL(packet []byte, submit aclSubmitFunc) (int, error) {
	d.retire()
	n, err := submit(func() (int, error) { return d.Write(packet) })
	if err != nil {
		return n, os.NewSyscallError("write", err)
	}
	return n, nil
}

func TestDisconnectRetirementSurvivesNativeErrorWrapping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := &shutdownTestTransport{writes: make(chan []byte, 2), closed: make(chan struct{})}
		d := &wrappedRetiredDisconnectTransport{shutdownTestTransport: base}
		h := &HCI{d: d, c: cmd.NewCmd(d), connsmu: &sync.Mutex{}, conns: make(map[uint16]*conn)}
		old := newConn(h, 1)
		h.conns[1] = old
		d.retire = func() {
			if err := h.handleDisconnectionComplete([]byte{0, 1, 0, 0x13}); err != nil {
				t.Error(err)
			}
		}
		defer h.Close()
		if err := old.Close(); err != nil {
			t.Fatalf("native-wrapped retirement was reported as a close failure: %v", err)
		}
		if len(base.writes) != 0 {
			t.Fatal("retired command reached its transport")
		}
		h.connsmu.Lock()
		closed := h.closed
		h.connsmu.Unlock()
		if closed {
			t.Fatal("a wrapped retirement stopped the whole controller")
		}
	})
}
