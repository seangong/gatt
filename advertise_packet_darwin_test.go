package gatt

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

func TestAdvertiseRejectsPacketErrorBeforeXPCRequest(t *testing.T) {
	a := (&AdvPacket{}).AppendName(string(bytes.Repeat([]byte{'n'}, 29))).AppendFlags(flagLEOnly)
	d := &device{reqc: make(chan message, 1)}
	done := make(chan error, 1)
	go func() { done <- d.Advertise(a) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrEIRPacketTooLong) {
			t.Fatalf("Advertise error = %v, want ErrEIRPacketTooLong", err)
		}
	case request := <-d.reqc:
		request.rspc <- xpc.Dict{"kCBMsgArgResult": 0}
		<-done
		t.Fatal("Advertise sent an XPC request for a failed packet")
	case <-time.After(time.Second):
		t.Fatal("Advertise blocked instead of rejecting a failed packet")
	}
	select {
	case <-d.reqc:
		t.Fatal("Advertise sent an XPC request for a failed packet")
	default:
	}
}
