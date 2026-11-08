package gatt

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestAdvertiseRejectsPacketErrorBeforeXPCRequest(t *testing.T) {
	a := (&AdvPacket{}).AppendName(string(bytes.Repeat([]byte{'n'}, 29))).AppendFlags(flagLEOnly)
	// No initialized transport: the packet error must be returned before any
	// attempt to send a request or inspect the device lifecycle.
	d := &device{}
	done := make(chan error, 1)
	go func() { done <- d.Advertise(a) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrEIRPacketTooLong) {
			t.Fatalf("Advertise error = %v, want ErrEIRPacketTooLong", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Advertise blocked instead of rejecting a failed packet")
	}
}
