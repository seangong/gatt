package linux

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestConnStopUnblocksRead(t *testing.T) {
	h, c := newACLTestConnection(1, 2)
	result := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 23))
		result <- err
	}()
	h.connsmu.Lock()
	c.stop()
	c.stop()
	h.connsmu.Unlock()
	select {
	case err := <-result:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read() after stop = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read() remained blocked after stop")
	}
}

func TestConnStopUnblocksWriteWaitingForCredit(t *testing.T) {
	h, c := newACLTestConnection(1, 2)
	h.bufSize = 27
	h.bufCnt = make(chan struct{}, 1)
	h.bufCnt <- struct{}{}
	h.d = &flowTestDevice{input: make(chan []byte, 32), done: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := c.Write(make([]byte, 513))
		result <- err
	}()
	h.connsmu.Lock()
	c.stop()
	h.connsmu.Unlock()
	select {
	case err := <-result:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Write() after stop = %v, want closed pipe", err)
		}
	case <-time.After(time.Second):
		// Release the waiter before failing so that a broken cancellation
		// path does not leave a test goroutine blocked on the credit channel.
		for {
			select {
			case <-h.bufCnt:
			case <-result:
				t.Fatal("Write() remained blocked after stop")
			case <-time.After(time.Second):
				t.Fatal("could not release Write() after failed cancellation")
			}
		}
	}
}

type overflowTestDevice struct{ commands chan []byte }

func (d *overflowTestDevice) Read([]byte) (int, error) { return 0, io.EOF }

func (d *overflowTestDevice) Write(b []byte) (int, error) {
	d.commands <- append([]byte(nil), b...)
	return 0, io.ErrClosedPipe
}

func (d *overflowTestDevice) writeACL(b []byte, submit aclSubmitFunc) (int, error) {
	return submit(func() (int, error) { return d.Write(b) })
}

func (*overflowTestDevice) Close() error { return nil }

func TestHCIReceiveQueuePreservesOrderAndStopsOnOverflow(t *testing.T) {
	d := &overflowTestDevice{commands: make(chan []byte, 4)}
	h := &HCI{
		d:       d,
		c:       cmd.NewCmd(d),
		connsmu: &sync.Mutex{},
		conns:   make(map[uint16]*conn),
	}
	defer h.Close()
	c := newConn(h, 1)
	h.conns[c.attr] = c
	size := cap(c.aclc)
	if size == 0 {
		t.Fatal("receive queue is unbuffered")
	}
	for i := 0; i < size; i++ {
		if err := h.handleL2CAP(aclPacketForTest(c.attr, 0x02, 3, 0, 4, 0, 0x52, byte(i), 0)); err != nil {
			t.Fatal(err)
		}
	}
	buf := make([]byte, 3)
	for i := 0; i < size; i++ {
		n, err := c.Read(buf)
		if err != nil || !bytes.Equal(buf[:n], []byte{0x52, byte(i), 0}) {
			t.Fatalf("queued command %d = %x, %v", i, buf[:n], err)
		}
	}
	for i := 0; i < size; i++ {
		if err := h.handleL2CAP(aclPacketForTest(c.attr, 0x02, 3, 0, 4, 0, 0x52, byte(i), 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.handleL2CAP(aclPacketForTest(c.attr, 0x02, 3, 0, 4, 0, 0x52, 0xff, 0)); err == nil {
		t.Fatal("receive queue overflow was not reported")
	}
	select {
	case <-c.done:
	case <-time.After(time.Second):
		t.Fatal("queue overflow did not stop the connection")
	}
	if n, err := c.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read() after overflow = (%d, %v), want (0, EOF)", n, err)
	}
	if n, err := c.Write([]byte{0x13}); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write() after overflow = (%d, %v), want (0, closed pipe)", n, err)
	}
	select {
	case packet := <-d.commands:
		want := []byte{1, 6, 4, 3, 1, 0, 0x13}
		if !bytes.Equal(packet, want) {
			t.Fatalf("overflow disconnect command = %x, want %x", packet, want)
		}
	case <-time.After(time.Second):
		t.Fatal("queue overflow did not send a disconnect command")
	}
	if err := c.Close(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("repeated Close() = %v, want original disconnect error", err)
	}
	select {
	case packet := <-d.commands:
		t.Fatalf("connection sent a duplicate disconnect command: %x", packet)
	default:
	}
}

func TestHCILoopEOFReleasesConnectionIO(t *testing.T) {
	d := &overflowTestDevice{commands: make(chan []byte, 1)}
	h := &HCI{
		d:       d,
		c:       cmd.NewCmd(d),
		bufSize: 27,
		bufCnt:  make(chan struct{}, 1),
		connsmu: &sync.Mutex{},
		conns:   make(map[uint16]*conn),
	}
	defer h.c.Close()
	c := newConn(h, 1)
	h.conns[c.attr] = c
	h.bufCnt <- struct{}{}
	read := make(chan error, 1)
	write := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 23))
		read <- err
	}()
	go func() {
		_, err := c.Write([]byte{0x13})
		write <- err
	}()
	h.mainLoop()
	if err := h.Close(); !errors.Is(err, io.EOF) {
		t.Fatalf("Close() after read EOF = %v, want EOF", err)
	}
	for _, result := range []struct {
		name string
		err  <-chan error
		want error
	}{
		{"read", read, io.EOF},
		{"write", write, io.ErrClosedPipe},
	} {
		select {
		case err := <-result.err:
			if !errors.Is(err, result.want) {
				t.Errorf("%s after HCI EOF = %v, want %v", result.name, err, result.want)
			}
		case <-time.After(time.Second):
			t.Errorf("%s remained blocked after HCI EOF", result.name)
		}
	}
	if len(h.conns) != 0 {
		t.Errorf("HCI EOF left %d connections registered", len(h.conns))
	}
}

func TestStoppedConnDoesNotDeliverQueuedPackets(t *testing.T) {
	h, c := newACLTestConnection(1, 2)
	for i := 0; i < 2; i++ {
		if err := h.handleL2CAP(aclPacketForTest(c.attr, 0x02, 3, 0, 4, 0, 0x52, byte(i), 0)); err != nil {
			t.Fatal(err)
		}
	}
	h.connsmu.Lock()
	c.stop()
	h.connsmu.Unlock()
	for i := 0; i < 10; i++ {
		if n, err := c.Read(make([]byte, 23)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("Read() on stopped connection = (%d, %v), want (0, EOF)", n, err)
		}
	}
}

func TestConnStopReportsBytesSentBeforeCreditWait(t *testing.T) {
	h, c := newACLTestConnection(1, 2)
	d := &flowTestDevice{input: make(chan []byte, 32), done: make(chan struct{})}
	h.d, h.bufSize, h.bufCnt = d, 27, make(chan struct{}, 1)
	t.Cleanup(func() {
		h.connsmu.Lock()
		c.stop()
		h.connsmu.Unlock()
	})
	result := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := c.Write(make([]byte, 513))
		result <- struct {
			n   int
			err error
		}{n, err}
	}()
	select {
	case <-d.input: // The first ACL fragment is sent, but its credit is not released.
	case <-time.After(time.Second):
		t.Fatal("first ACL fragment was not sent")
	}
	h.connsmu.Lock()
	c.stop()
	h.connsmu.Unlock()
	select {
	case got := <-result:
		if got.n != 23 || !errors.Is(got.err, io.ErrClosedPipe) {
			t.Fatalf("Write() = (%d, %v), want (23, closed pipe)", got.n, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("stopped Write() did not return")
	}
}

func TestHCILoopEOFRejectsLateConnection(t *testing.T) {
	d := &overflowTestDevice{commands: make(chan []byte, 1)}
	accepted := false
	h := &HCI{
		d:                   d,
		c:                   cmd.NewCmd(d),
		connsmu:             &sync.Mutex{},
		conns:               make(map[uint16]*conn),
		AcceptMasterHandler: func(*PlatData) { accepted = true },
	}
	h.mainLoop()
	h.Close()
	packet := make([]byte, 19)
	packet[0], packet[2], packet[4] = 1, 1, 1
	h.handleConnection(packet)
	if accepted || len(h.conns) != 0 {
		t.Fatalf("closed HCI accepted late connection: callback=%t, connections=%d", accepted, len(h.conns))
	}
}
