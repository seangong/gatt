package linux

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestHCIOnlyATTChannelEntersATTQueue(t *testing.T) {
	for _, cid := range []uint16{0, 2, 4, 6, 0x40} {
		for _, fragmented := range []bool{false, true} {
			h, c := newACLTestConnection(1, 2)
			payload := []byte{0x0b, 0}
			pdu := append([]byte{2, 0, byte(cid), byte(cid >> 8)}, payload...)
			packets := [][]byte{aclPacketForTest(1, 2, pdu...)}
			if fragmented {
				// Split within the L2CAP header so CID routing follows reassembly.
				packets = [][]byte{aclPacketForTest(1, 2, pdu[:3]...), aclPacketForTest(1, 1, pdu[3:]...)}
			}
			for _, packet := range packets {
				if err := h.handleL2CAP(packet); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case got := <-c.aclc:
				if cid != 4 || !bytes.Equal(got, payload) {
					t.Fatalf("CID %d fragmented=%t entered ATT: %x", cid, fragmented, got)
				}
			default:
				if cid == 4 {
					t.Fatalf("ATT packet dropped, fragmented=%t", fragmented)
				}
			}
			if c.Context().Err() != nil {
				t.Fatalf("CID %d retired ATT connection", cid)
			}
		}
	}
}

func TestHCINonATTChannelDoesNotOverflowATTQueue(t *testing.T) {
	h, c := newACLTestConnection(1, 1)
	c.aclc <- []byte{0x0b, 42}
	for i := 0; i < 20; i++ {
		if err := h.handleL2CAP(aclPacketForTest(1, 2, 2, 0, 6, 0, 0x0b, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if c.Context().Err() != nil || !bytes.Equal(<-c.aclc, []byte{0x0b, 42}) {
		t.Fatal("unsupported channel altered the pending ATT response")
	}
	if err := h.handleL2CAP(aclPacketForTest(1, 2, 2, 0, 4, 0, 0x0b, 7)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-c.aclc, []byte{0x0b, 7}) {
		t.Fatal("ATT channel was unusable after dropping other channels")
	}
}

func TestHCISignalingChannelStillProcessesFragmentedResponse(t *testing.T) {
	h, c := newACLTestConnection(1, 1)
	// Connection Parameter Update Response, accepted. It must not reach ATT.
	payload := []byte{0x13, 2, 2, 0, 0, 0}
	pdu := make([]byte, 4, 4+len(payload))
	binary.LittleEndian.PutUint16(pdu, uint16(len(payload)))
	binary.LittleEndian.PutUint16(pdu[2:], 5)
	pdu = append(pdu, payload...)
	if err := h.handleL2CAP(aclPacketForTest(1, 2, pdu[:5]...)); err != nil {
		t.Fatal(err)
	}
	if err := h.handleL2CAP(aclPacketForTest(1, 1, pdu[5:]...)); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-c.aclc:
		t.Fatalf("signaling response entered ATT: %x", b)
	default:
	}
}
