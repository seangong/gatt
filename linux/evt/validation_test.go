package evt

import (
	"reflect"
	"testing"
)

func TestCompletedPacketsRejectsInvalidLengthWithoutMutation(t *testing.T) {
	for _, b := range [][]byte{nil, {1}, {1, 1, 0, 1}, {0, 1}, {2, 1, 0, 1, 0}} {
		e := NumberOfCompletedPktsEP{NumberOfHandles: 3, Packets: []NumOfCompletedPkt{{ConnectionHandle: 7}}}
		want := e
		if err := e.Unmarshal(b); err == nil {
			t.Fatalf("accepted malformed completed-packets payload %x", b)
		}
		if !reflect.DeepEqual(e, want) {
			t.Fatal("failed parse published partial completion data")
		}
	}
	var e NumberOfCompletedPktsEP
	if err := e.Unmarshal([]byte{2, 7, 0, 2, 0, 9, 0, 3, 0}); err != nil {
		t.Fatal(err)
	}
	if e.NumberOfHandles != 2 || e.Packets[0].ConnectionHandle != 7 || e.Packets[1].NumOfCompletedPkts != 3 {
		t.Fatalf("completed-packets parse = %+v", e)
	}
}

func TestConnectionEventsValidateWholePayload(t *testing.T) {
	for n := 0; n <= 20; n++ {
		if n == 19 {
			continue
		}
		e := LEConnectionCompleteEP{ConnectionHandle: 7}
		want := e
		if err := e.Unmarshal(make([]byte, n)); err == nil || e != want {
			t.Fatalf("connection length %d accepted or changed receiver", n)
		}
	}
	var e LEConnectionCompleteEP
	b := []byte{1, 0, 7, 0, 1, 0, 1, 2, 3, 4, 5, 6, 0x18, 0, 2, 0, 0x34, 0x12, 0x55}
	if err := e.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if e.ConnectionHandle != 7 || e.MasterClockAccuracy != 0x55 || e.SupervisionTimeout != 0x1234 || e.PeerAddress != [6]byte{6, 5, 4, 3, 2, 1} {
		t.Fatalf("connection payload = %+v", e)
	}
	for n := 0; n <= 5; n++ {
		if n == 4 {
			continue
		}
		e := DisconnectionCompleteEP{ConnectionHandle: 7}
		want := e
		if err := e.Unmarshal(make([]byte, n)); err == nil || e != want {
			t.Fatalf("disconnection length %d accepted or changed receiver", n)
		}
	}
}

func TestEventHeaderLengthDoesNotWrap(t *testing.T) {
	var h EventHeader
	if err := h.Unmarshal(make([]byte, 258)); err == nil {
		t.Fatal("event header accepted an overflowing actual length")
	}
	b := make([]byte, 257)
	b[1] = 255
	if err := h.Unmarshal(b); err != nil {
		t.Fatalf("max-size event header = %v", err)
	}
}
