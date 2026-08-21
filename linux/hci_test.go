package linux

import (
	"bytes"
	"sync"
	"testing"
)

func TestHandleL2CAPForwardsShortContinuationFragment(t *testing.T) {
	h := &HCI{
		connsmu: &sync.Mutex{},
		conns:   make(map[uint16]*conn),
	}
	c := newConn(h, 0x0001)
	c.aclc = make(chan *aclData, 2)
	h.conns[c.attr] = c

	// The first ACL fragment contains the L2CAP header and two payload bytes.
	if err := h.handleL2CAP([]byte{
		0x01, 0x00, 0x06, 0x00,
		0x05, 0x00, 0x04, 0x00, 0xaa, 0xbb,
	}); err != nil {
		t.Fatalf("handle first fragment: %v", err)
	}

	// A continuation fragment has no L2CAP header and may be shorter than four bytes.
	if err := h.handleL2CAP([]byte{
		0x01, 0x10, 0x03, 0x00,
		0xcc, 0xdd, 0xee,
	}); err != nil {
		t.Fatalf("handle continuation fragment: %v", err)
	}

	buf := make([]byte, 5)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read reassembled packet: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("read length: got %d want %d", n, len(buf))
	}
	want := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee}
	if !bytes.Equal(buf, want) {
		t.Fatalf("read payload: got %x want %x", buf, want)
	}
}

func TestHandleL2CAPRejectsShortFirstFragment(t *testing.T) {
	h := &HCI{
		connsmu: &sync.Mutex{},
		conns:   make(map[uint16]*conn),
	}
	c := newConn(h, 0x0001)
	c.aclc = make(chan *aclData, 1)
	h.conns[c.attr] = c

	if err := h.handleL2CAP([]byte{
		0x01, 0x00, 0x03, 0x00,
		0xaa, 0xbb, 0xcc,
	}); err != nil {
		t.Fatalf("handle short first fragment: %v", err)
	}
	if len(c.aclc) != 0 {
		t.Fatal("short first fragment was forwarded")
	}
}
