package linux

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func completedACL(handle uint16, count byte) []byte {
	return []byte{1, byte(handle), byte(handle >> 8), count, 0}
}

func TestACLDisconnectReturnsOnlyConnectionCredits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		first, second := newConn(h, 1), newConn(h, 2)
		h.conns[1], h.conns[2] = first, second
		for i := 0; i < cap(h.bufCnt); i++ {
			c := first
			if i%2 != 0 {
				c = second
			}
			if _, err := c.Write([]byte{0x13}); err != nil {
				t.Fatal(err)
			}
			<-d.writes
		}
		if err := h.handleNumberOfCompletedPkts(completedACL(1, 2)); err != nil {
			t.Fatal(err)
		}
		shutdownTestDisconnected(d, 1)
		op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d, op)
		shutdownTestCommandComplete(d, op, 0)
		if got := len(h.bufCnt); got != 7 {
			t.Fatalf("credits after disconnect = %d, want second connection's 7", got)
		}
		if err := h.handleNumberOfCompletedPkts(completedACL(2, 7)); err != nil {
			t.Fatal(err)
		}
		if len(h.bufCnt) != 0 {
			t.Fatal("completed connection retained credits")
		}
		fresh := newConn(h, 3)
		h.connsmu.Lock()
		h.conns[3] = fresh
		h.connsmu.Unlock()
		if _, err := fresh.Write([]byte{0x13}); err != nil {
			t.Fatal(err)
		}
		<-d.writes
	})
}

type submissionTestTransport struct {
	*shutdownTestTransport
	first        sync.Once
	poll         bool
	beforeRetry  chan struct{}
	resume       chan struct{}
	accepted     chan struct{}
	pauseAttempt bool
}

type shortACLTestTransport struct{ *flowTestDevice }

func (*shortACLTestTransport) writeACL(b []byte, submit aclSubmitFunc) (int, error) {
	return submit(func() (int, error) { return len(b) - 1, nil })
}

func TestACLSubmissionFailureClosesHCI(t *testing.T) {
	for _, short := range []bool{false, true} {
		d := &flowTestDevice{input: make(chan []byte, 4), done: make(chan struct{})}
		var transport io.ReadWriteCloser = struct{ io.ReadWriteCloser }{d}
		if short {
			transport = &shortACLTestTransport{d}
		}
		h := &HCI{
			d: transport, c: cmd.NewCmd(transport), bufSize: 27, bufCnt: make(chan struct{}, 1),
			connsmu: &sync.Mutex{}, conns: make(map[uint16]*conn),
		}
		c := newConn(h, 1)
		h.conns[1] = c
		if _, err := c.Write([]byte{0x13}); err == nil || short && !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short=%t: submission error = %v", short, err)
		}
		if err := h.Close(); err == nil {
			t.Fatal("submission failure lost its HCI close cause")
		}
		if len(h.bufCnt) != 0 {
			t.Fatal("failed submission retained a credit")
		}
	}
}

func (d *submissionTestTransport) writeACL(b []byte, submit aclSubmitFunc) (int, error) {
	first := false
	d.first.Do(func() { first = true })
	if first && d.poll {
		_, err := submit(func() (int, error) { return 0, syscall.EAGAIN })
		if !errors.Is(err, syscall.EAGAIN) {
			return 0, err
		}
		close(d.beforeRetry)
		select {
		case <-d.resume:
		case <-d.closed:
			return 0, io.ErrClosedPipe
		}
	}
	n, err := submit(func() (int, error) {
		n, err := d.Write(b)
		if first && d.pauseAttempt {
			// Pause after the native attempt accepted a packet but before its
			// result is published, modelling a scheduling pause at that boundary.
			close(d.accepted)
			<-d.resume
		}
		return n, err
	})
	if first && !d.poll && !d.pauseAttempt {
		close(d.accepted)
		select {
		case <-d.resume:
		case <-d.closed:
			return n, io.ErrClosedPipe
		}
	}
	return n, err
}

func newSubmissionTestHCI(t *testing.T, poll bool) (*HCI, *submissionTestTransport) {
	t.Helper()
	d := &submissionTestTransport{
		shutdownTestTransport: &shutdownTestTransport{
			input: make(chan []byte, 16), readError: make(chan error, 1),
			writes: make(chan []byte, 16), closed: make(chan struct{}),
		},
		poll: poll, beforeRetry: make(chan struct{}), resume: make(chan struct{}), accepted: make(chan struct{}),
	}
	h := &HCI{
		d: d, c: cmd.NewCmd(d), e: evt.NewEvt(), maxConn: 4,
		connsmu: &sync.Mutex{}, conns: make(map[uint16]*conn),
		plistmu: &sync.Mutex{}, plist: make(map[bdaddr]*PlatData), advmu: &sync.Mutex{},
		bufSize: 27, bufCnt: make(chan struct{}, 14),
	}
	h.e.HandleEvent(evt.CommandComplete, evt.HandlerFunc(h.c.HandleComplete))
	h.e.HandleEvent(evt.NumberOfCompletedPkts, evt.HandlerFunc(h.handleNumberOfCompletedPkts))
	h.e.HandleEvent(evt.DisconnectionComplete, evt.HandlerFunc(h.handleDisconnectionComplete))
	h.e.HandleEvent(evt.LEMeta, evt.HandlerFunc(h.handleLEMeta))
	go h.mainLoop()
	synctest.Wait()
	t.Cleanup(func() { d.Close(); <-h.requestClose(io.ErrClosedPipe) })
	return h, d
}

func TestACLRetiredPollRetryCannotWriteToReusedHandle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newSubmissionTestHCI(t, true)
		old := newConn(h, 1)
		h.conns[1] = old
		result := make(chan error, 1)
		go func() { _, err := old.Write([]byte{0xaa}); result <- err }()
		<-d.beforeRetry
		shutdownTestDisconnected(d.shutdownTestTransport, 1)
		op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d.shutdownTestTransport, op)
		shutdownTestCommandComplete(d.shutdownTestTransport, op, 0)
		if len(h.bufCnt) != 0 {
			t.Fatal("retired poll reservation retained a credit")
		}
		accepted := make(chan *PlatData, 1)
		h.AcceptMasterHandler = func(p *PlatData) { accepted <- p }
		fresh := connectionIdentityComplete(t, d.shutdownTestTransport, accepted, [6]byte{1}, 1, 1).Conn.(*conn)
		close(d.resume)
		if err := <-result; !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("old poll retry = %v, want closed pipe", err)
		}
		if _, err := fresh.Write([]byte{0xbb}); err != nil {
			t.Fatal(err)
		}
		packet := <-d.writes
		if !bytes.Equal(packet[9:], []byte{0xbb}) {
			t.Fatalf("reused handle received stale payload: %x", packet)
		}
		if got := len(h.bufCnt); got != 1 {
			t.Fatalf("old retry refunded new connection's credit: %d", got)
		}
		h.connsmu.Lock()
		closed := h.closed
		h.connsmu.Unlock()
		if closed {
			t.Fatal("normal connection retirement closed the whole HCI")
		}
	})
}

func TestACLCompletionBeforeOuterWriteReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newSubmissionTestHCI(t, false)
		c := newConn(h, 1)
		h.conns[1] = c
		result := make(chan error, 1)
		go func() { _, err := c.Write([]byte{0xaa}); result <- err }()
		<-d.accepted
		<-d.writes
		d.input <- shutdownTestEvent(evt.NumberOfCompletedPkts, completedACL(1, 1)...)
		synctest.Wait()
		if len(h.bufCnt) != 0 {
			t.Fatal("accepted native packet was not published before completion")
		}
		shutdownTestDisconnected(d.shutdownTestTransport, 1)
		op := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		shutdownTestCommand(t, d.shutdownTestTransport, op)
		shutdownTestCommandComplete(d.shutdownTestTransport, op, 0)
		close(d.resume)
		if err := <-result; err != nil {
			t.Fatalf("accepted packet became a failure after normal disconnect: %v", err)
		}
		h.connsmu.Lock()
		closed := h.closed
		h.connsmu.Unlock()
		if closed {
			t.Fatal("Go scheduling delay incorrectly closed HCI")
		}
	})
}

func TestACLCloseTransportDuringSubmissionDoesNotDoubleRefund(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newSubmissionTestHCI(t, false)
		d.pauseAttempt = true
		c := newConn(h, 1)
		h.conns[1] = c
		result := make(chan error, 1)
		go func() { _, err := c.Write([]byte{0xaa}); result <- err }()
		<-d.accepted
		<-d.writes
		// Global transport closure must not wait for the submission fence: it
		// is also the escape path for native poll waits and failed submissions.
		h.closeTransport()
		if len(h.bufCnt) != 0 {
			t.Fatal("transport shutdown retained the uncommitted reservation")
		}
		close(d.resume)
		if err := <-result; err != nil {
			t.Fatalf("already accepted write returned %v", err)
		}
		h.connsmu.Lock()
		pending := c.aclOutstanding
		h.connsmu.Unlock()
		if pending != 0 || len(h.bufCnt) != 0 {
			t.Fatal("post-close result publication reintroduced or refunded credits twice")
		}
		if err := h.Close(); err != nil {
			t.Fatalf("normal closure acquired a spurious error: %v", err)
		}
	})
}

func TestACLBadCompletionClosesTransport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		c := newConn(h, 1)
		h.conns[1] = c
		if _, err := c.Write([]byte{0x13}); err != nil {
			t.Fatal(err)
		}
		<-d.writes
		// A valid first record followed by an unknown handle must not partially
		// mutate the ledger before reporting the inconsistent event.
		if err := h.handleNumberOfCompletedPkts([]byte{2, 1, 0, 1, 0, 2, 0, 1, 0}); err == nil {
			t.Fatal("unknown completion accepted")
		}
		h.connsmu.Lock()
		closed := h.closed
		h.connsmu.Unlock()
		if !closed {
			t.Fatal("bad completion left the transport open")
		}
	})
}

func TestMalformedControlEventsCloseHCI(t *testing.T) {
	for _, packet := range [][]byte{
		{byte(typEventPkt), evt.NumberOfCompletedPkts, 0},
		{byte(typEventPkt), evt.LEMeta, 1, byte(evt.LEConnectionComplete)},
		{byte(typEventPkt), evt.DisconnectionComplete, 1, 0},
		{byte(typEventPkt), evt.NumberOfCompletedPkts, 5, 1},
	} {
		synctest.Test(t, func(t *testing.T) {
			h, _ := newShutdownTestHCI(t, false)
			h.handlePacket(packet)
			h.connsmu.Lock()
			closed := h.closed
			h.connsmu.Unlock()
			if !closed {
				t.Fatalf("malformed control event did not end HCI: %x", packet)
			}
		})
	}
}

func TestNoncriticalEventsKeepHCIAvailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, _ := newShutdownTestHCI(t, false)
		h.AdvertisementHandler = func(*PlatData) { t.Error("malformed advertisement was published") }
		for _, packet := range [][]byte{
			shutdownTestEvent(evt.LEMeta, 0xff), // Unsupported subevent.
			shutdownTestEvent(evt.LEMeta, byte(evt.LEAdvertisingReport), 1),
		} {
			h.handlePacket(packet)
		}
		h.connsmu.Lock()
		closed := h.closed
		h.connsmu.Unlock()
		if closed {
			t.Fatal("noncritical event closed HCI")
		}
	})
}
