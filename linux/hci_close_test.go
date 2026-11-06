package linux

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
)

type pendingDisconnectTestDevice struct {
	commands   chan []byte
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func (*pendingDisconnectTestDevice) Read([]byte) (int, error) { return 0, io.EOF }

func (d *pendingDisconnectTestDevice) Write(b []byte) (int, error) {
	d.commands <- append([]byte(nil), b...)
	return len(b), nil
}

func (d *pendingDisconnectTestDevice) Close() error {
	d.closeCalls.Add(1)
	d.closeOnce.Do(func() { close(d.closed) })
	return nil
}

func TestHCICloseWaitsForOverflowDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &pendingDisconnectTestDevice{
			commands: make(chan []byte, 4),
			closed:   make(chan struct{}),
		}
		h := &HCI{
			d:       d,
			c:       cmd.NewCmd(d),
			connsmu: &sync.Mutex{},
			conns:   make(map[uint16]*conn),
		}
		c := newConn(h, 1)
		h.conns[c.attr] = c
		closeStarted := false
		closeFinished := make(chan struct{})
		defer func() {
			// Cancel a pending Disconnect even if an assertion fails before
			// its controller status is delivered.
			h.c.Close()
			synctest.Wait()
			if closeStarted {
				select {
				case <-closeFinished:
				default:
					t.Error("HCI.Close did not exit after command cancellation")
				}
			}
			select {
			case <-d.closed:
			default:
				d.Close()
			}
		}()
		for i := 0; i < cap(c.aclc); i++ {
			if err := h.handleL2CAP(aclPacketForTest(c.attr, 0x02, 3, 0, 4, 0, 0x52, byte(i), 0)); err != nil {
				t.Fatal(err)
			}
		}
		if err := h.handleL2CAP(aclPacketForTest(c.attr, 0x02, 3, 0, 4, 0, 0x52, 0xff, 0)); err == nil {
			t.Fatal("receive queue overflow was not reported")
		}
		synctest.Wait()
		select {
		case packet := <-d.commands:
			if want := []byte{1, 6, 4, 3, 1, 0, 0x13}; !bytes.Equal(packet, want) {
				t.Fatalf("overflow disconnect command = %x, want %x", packet, want)
			}
		default:
			t.Fatal("overflow did not send a disconnect command")
		}
		closeResult := make(chan error, 1)
		closeStarted = true
		go func() {
			defer close(closeFinished)
			closeResult <- h.Close()
		}()
		synctest.Wait()
		h.connsmu.Lock()
		closing := h.closed
		h.connsmu.Unlock()
		if !closing {
			t.Fatal("HCI.Close did not enter shutdown")
		}
		select {
		case <-d.closed:
			t.Fatal("HCI.Close closed the device before Disconnect completed")
		case err := <-closeResult:
			t.Fatalf("HCI.Close returned before Disconnect completed: %v", err)
		default:
		}
		if err := h.c.HandleStatus([]byte{0, 1, 6, 4}); err != nil {
			t.Fatalf("deliver Disconnect status: %v", err)
		}
		synctest.Wait()
		select {
		case err := <-closeResult:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("HCI.Close did not finish after Disconnect status")
		}
		select {
		case <-d.closed:
		default:
			t.Fatal("HCI.Close did not close the device")
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		if got := d.closeCalls.Load(); got != 1 {
			t.Errorf("device Close calls = %d, want 1", got)
		}
		select {
		case packet := <-d.commands:
			t.Fatalf("duplicate Disconnect command = %x", packet)
		default:
		}
	})
}
