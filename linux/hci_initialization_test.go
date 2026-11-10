package linux

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/evt"
)

func TestHCIInitializationSelectsControllerCommands(t *testing.T) {
	for _, leOnly := range []bool{false, true} {
		name := "dual mode"
		if leOnly {
			name = "LE only"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				result := make(chan error, 1)
				go func() { result <- h.resetDevice(context.Background()) }()
				ops := []uint16{0x0c03, 0x1003}
				if !leOnly {
					ops = append(ops, 0x1002)
				}
				ops = append(ops, 0x0c01, 0x2001)
				if !leOnly {
					ops = append(ops, 0x0c56, 0x0c6d, 0x0c45, 0x0c47, 0x0c43, 0x0c24, 0x0c18, 0x080f, 0x0c33)
				}
				ops = append(ops, 0x200b)
				parameters := map[uint16][]byte{
					0x0c56: {1},
					0x0c6d: {1, 0},
					0x0c45: {2},
					0x0c47: {1},
					0x0c43: {1},
					0x0c24: {0x40, 0x02, 0x04},
					0x0c18: {0, 0x20},
					0x080f: {0x05, 0},
					0x0c33: {0, 0x10, 0xff, 0x14, 0, 0x0a, 0},
					0x200b: {1, 0x10, 0, 0x10, 0, 0, 0},
				}
				for _, op := range ops {
					packet := shutdownTestCommand(t, d, op)
					if want, ok := parameters[op]; ok && !bytes.Equal(packet[4:], want) {
						t.Fatalf("initialization command 0x%04x parameters = %x, want %x", op, packet[4:], want)
					}
					rsp := []byte{0}
					if op == 0x1003 {
						if len(packet) != 4 || packet[3] != 0 {
							t.Fatalf("read features command = %x", packet)
						}
						features := initializationTestFeatures()
						if leOnly {
							features = [8]byte{0, 0, 0, 0, 0x60, 0, 0, 0x80}
						}
						rsp = append([]byte{0}, features[:]...)
					} else if op == 0x1002 {
						if len(packet) != 4 || packet[3] != 0 {
							t.Fatalf("read commands command = %x", packet)
						}
						rsp = initializationTestCommands()
					}
					d.input <- shutdownTestEvent(evt.CommandComplete, append([]byte{1, byte(op), byte(op >> 8)}, rsp...)...)
					synctest.Wait()
				}
				if err := <-result; err != nil {
					t.Fatalf("resetDevice: %v", err)
				}
				pendingTestNoCommand(t, d)
			})
		})
	}
}

func TestHCIInitializationRejectsInvalidFeatures(t *testing.T) {
	for _, tt := range []struct {
		name string
		rsp  []byte
	}{
		{"missing status", nil},
		{"failed command", []byte{1}},
		{"truncated features", []byte{0, 0, 0, 0, 0, 0x60}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				result := make(chan error, 1)
				go func() { result <- h.resetDevice(context.Background()) }()
				shutdownTestCommand(t, d, 0x0c03)
				shutdownTestCommandComplete(d, 0x0c03, 0)
				shutdownTestCommand(t, d, 0x1003)
				d.input <- shutdownTestEvent(evt.CommandComplete, append([]byte{1, 3, 0x10}, tt.rsp...)...)
				if err := <-result; err == nil || !strings.Contains(err.Error(), "0x1003") {
					t.Fatalf("resetDevice error = %v, want read-features error", err)
				}
				pendingTestNoCommand(t, d)
			})
		})
	}
}

func TestHCIInitializationKeepsCommandErrorsAndCancellation(t *testing.T) {
	for _, op := range []uint16{0x0c03, 0x0c56, 0x0c6d, 0x200b} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			result := make(chan error, 1)
			go func() { result <- h.resetDevice(context.Background()) }()
			for {
				packet := <-d.writes
				got := uint16(packet[1]) | uint16(packet[2])<<8
				rsp := []byte{0}
				if got == 0x1003 {
					features := initializationTestFeatures()
					rsp = append([]byte{0}, features[:]...)
				} else if got == 0x1002 {
					rsp = initializationTestCommands()
				}
				if got == op {
					rsp[0] = 0x0c
				}
				d.input <- shutdownTestEvent(evt.CommandComplete, append([]byte{1, byte(got), byte(got >> 8)}, rsp...)...)
				synctest.Wait()
				if got == op {
					break
				}
			}
			if err := <-result; err == nil {
				t.Fatalf("command 0x%04x failure was ignored", op)
			}
			pendingTestNoCommand(t, d)
		})
	}
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- h.resetDevice(ctx) }()
		shutdownTestCommand(t, d, 0x0c03)
		shutdownTestCommandComplete(d, 0x0c03, 0)
		shutdownTestCommand(t, d, 0x1003)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("resetDevice after cancellation = %v", err)
		}
		pendingTestNoCommand(t, d)
	})
}
