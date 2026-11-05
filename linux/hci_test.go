package linux

import (
	"bytes"
	"sync"
	"testing"
)

func TestHandleL2CAPReassemblesACLFragments(t *testing.T) {
	const handle = 0x0001
	attRead := []byte{0x0a, 0x01, 0x00}
	completeATT := aclPacketForTest(handle, 0x02, 0x03, 0x00, 0x04, 0x00, 0x0a, 0x01, 0x00)
	tests := []struct {
		name    string
		packets [][]byte
		want    []byte
	}{
		{
			name: "short continuation",
			packets: [][]byte{
				aclPacketForTest(handle, 0x02, 0x05, 0x00, 0x04, 0x00, 0xaa, 0xbb),
				aclPacketForTest(handle, 0x01, 0xcc, 0xdd, 0xee),
			},
			want: []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee},
		},
		{
			name: "three-byte start",
			packets: [][]byte{
				aclPacketForTest(handle, 0x02, 0x03, 0x00, 0x04),
				aclPacketForTest(handle, 0x01, 0x00, 0x0a, 0x01, 0x00),
			},
			want: attRead,
		},
		{
			name: "length field spans fragments",
			packets: [][]byte{
				aclPacketForTest(handle, 0x02, 0x03),
				aclPacketForTest(handle, 0x01, 0x00, 0x04),
				aclPacketForTest(handle, 0x01, 0x00, 0x0a, 0x01, 0x00),
			},
			want: attRead,
		},
		{
			name: "orphan continuation",
			packets: [][]byte{
				aclPacketForTest(handle, 0x01, 0x00, 0x00, 0x00),
				completeATT,
			},
			want: attRead,
		},
		{
			name: "fragmented signaling packet",
			packets: [][]byte{
				aclPacketForTest(handle, 0x02, 0x06, 0x00, 0x05, 0x00, 0x13, 0x02, 0x02),
				aclPacketForTest(handle, 0x01, 0x00, 0x00, 0x00),
				completeATT,
			},
			want: attRead,
		},
		{
			name: "oversize continuation",
			packets: [][]byte{
				aclPacketForTest(handle, 0x02, 0x01, 0x00, 0x04, 0x00),
				aclPacketForTest(handle, 0x01, 0xaa, 0xbb),
				completeATT,
			},
			want: attRead,
		},
		{
			name: "new start replaces incomplete packet",
			packets: [][]byte{
				aclPacketForTest(handle, 0x02, 0x03, 0x00, 0x04, 0x00, 0xbb),
				completeATT,
			},
			want: attRead,
		},
		{
			name: "legacy complete packet flag requires a complete PDU",
			packets: [][]byte{
				aclPacketForTest(handle, 0x03, 0x03, 0x00, 0x04, 0x00, 0x0a),
				aclPacketForTest(handle, 0x01, 0x01, 0x00),
				aclPacketForTest(handle, 0x03, 0x03, 0x00, 0x04, 0x00, 0x0a, 0x01, 0x00),
			},
			want: attRead,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, c := newACLTestConnection(handle, len(tt.packets))
			for i, packet := range tt.packets {
				if err := h.handleL2CAP(packet); err != nil {
					t.Fatalf("handle fragment %d: %v", i, err)
				}
				if i < len(tt.packets)-1 && len(c.aclc) != 0 {
					t.Fatalf("fragment %d unexpectedly queued an ATT packet", i)
				}
			}
			checkACLTestPayload(t, c, tt.want)
		})
	}
}

func TestHandleL2CAPReassemblesInterleavedConnections(t *testing.T) {
	h, first := newACLTestConnection(0x0001, 2)
	second := newConn(h, 0x0002)
	second.aclc = make(chan []byte, 2)
	h.conns[second.attr] = second
	for i, packet := range [][]byte{
		aclPacketForTest(first.attr, 0x02, 0x03, 0x00, 0x04),
		aclPacketForTest(second.attr, 0x02, 0x03, 0x00, 0x04, 0x00, 0x0a),
		aclPacketForTest(first.attr, 0x01, 0x00, 0x0a, 0x01, 0x00),
		aclPacketForTest(second.attr, 0x01, 0x02, 0x00),
	} {
		if err := h.handleL2CAP(packet); err != nil {
			t.Fatalf("handle fragment %d: %v", i, err)
		}
	}
	checkACLTestPayload(t, first, []byte{0x0a, 0x01, 0x00})
	checkACLTestPayload(t, second, []byte{0x0a, 0x02, 0x00})
}

func aclPacketForTest(handle uint16, pb byte, payload ...byte) []byte {
	return append([]byte{byte(handle), byte(handle>>8) | pb<<4, byte(len(payload)), byte(len(payload) >> 8)}, payload...)
}

func newACLTestConnection(handle uint16, bufferSize int) (*HCI, *conn) {
	h := &HCI{
		connsmu: &sync.Mutex{},
		conns:   make(map[uint16]*conn),
	}
	c := newConn(h, handle)
	c.aclc = make(chan []byte, bufferSize)
	h.conns[c.attr] = c
	return h, c
}

func checkACLTestPayload(t *testing.T, c *conn, want []byte) {
	t.Helper()
	if got := len(c.aclc); got != 1 {
		t.Fatalf("queued ATT packets: got %d want 1", got)
	}
	buf := make([]byte, len(want))
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("read reassembled packet: %v", err)
	}
	if n != len(want) || !bytes.Equal(buf, want) {
		t.Fatalf("read payload: got %x (%d bytes) want %x", buf, n, want)
	}
}
