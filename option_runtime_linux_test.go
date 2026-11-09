package gatt

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestNewDeviceRejectsRuntimeHCIOptions(t *testing.T) {
	for _, tt := range []struct {
		name string
		opt  Option
	}{
		{"raw command", LnxSendHCIRawCommand(cmd.Reset{}, nil)},
		{"advertising enable", LnxSetAdvertisingEnable(true)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var skipped bool
			d, err := NewDevice(tt.opt, func(Device) error { skipped = true; return nil })
			if d != nil || err == nil || skipped {
				t.Fatalf("NewDevice = (%v, %v), later option called = %t", d, err, skipped)
			}
		})
	}
}

func TestUninitializedDeviceRejectsRawCommand(t *testing.T) {
	for _, d := range []*device{nil, {}} {
		if _, err := d.SendHCIRawCommand(cmd.Reset{}); err == nil {
			t.Fatal("raw command on uninitialized device succeeded")
		}
	}
}

type rawResponseTestWriter struct {
	data  []byte
	n     int
	err   error
	calls int
}

func (w *rawResponseTestWriter) Write(b []byte) (int, error) {
	w.calls++
	w.data = append([]byte(nil), b...)
	return w.n, w.err
}

func TestRawHCIResponsePreservesWriteAndCommandErrors(t *testing.T) {
	commandErr := errors.New("command failed")
	writeErr := errors.New("response destination failed")
	for _, tt := range []struct {
		name       string
		n          int
		writerErr  error
		commandErr error
		wantErr    error
	}{
		{"success", 3, nil, nil, nil},
		{"write failure", 0, writeErr, nil, writeErr},
		{"short write", 2, nil, nil, io.ErrShortWrite},
		{"full write with error", 3, writeErr, nil, writeErr},
		{"command failure", 3, nil, commandErr, commandErr},
		{"both failures", 0, writeErr, commandErr, writeErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := &rawResponseTestWriter{n: tt.n, err: tt.writerErr}
			data := []byte{0, 1, 2}
			err := writeHCIRawResponse(w, data, tt.commandErr)
			if !errors.Is(err, tt.wantErr) || tt.commandErr != nil && !errors.Is(err, tt.commandErr) {
				t.Fatalf("response error = %v, want %v and %v", err, tt.wantErr, tt.commandErr)
			}
			if w.calls != 1 || !bytes.Equal(w.data, data) {
				t.Fatalf("response writes = %d, data = %x", w.calls, w.data)
			}
		})
	}
	if err := writeHCIRawResponse(nil, nil, commandErr); err != commandErr {
		t.Fatalf("nil response writer lost original error: %v", err)
	}
}
