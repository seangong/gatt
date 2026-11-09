package linux

import (
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
				ops := []uint16{0x0c03, 0x1003, 0x0c01, 0x2001}
				if !leOnly {
					ops = append(ops, 0x0c56, 0x0c6d, 0x0c45, 0x0c47, 0x0c43, 0x0c24, 0x0c18, 0x080f, 0x0c33)
				}
				ops = append(ops, 0x200b)
				for _, op := range ops {
					packet := shutdownTestCommand(t, d, op)
					rsp := []byte{0}
					if op == 0x1003 {
						if len(packet) != 4 || packet[3] != 0 {
							t.Fatalf("read features command = %x", packet)
						}
						rsp = make([]byte, 9)
						rsp[5] = 0x40 // LE Supported (Controller).
						if leOnly {
							rsp[5] |= 0x20 // BR/EDR Not Supported.
							rsp[8] = 0x80  // Other feature bits do not change the decision.
						}
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
	for _, op := range []uint16{0x0c03, 0x0c56, 0x200b} {
		synctest.Test(t, func(t *testing.T) {
			h, d := newShutdownTestHCI(t, false)
			result := make(chan error, 1)
			go func() { result <- h.resetDevice(context.Background()) }()
			for {
				packet := <-d.writes
				got := uint16(packet[1]) | uint16(packet[2])<<8
				rsp := []byte{0}
				if got == 0x1003 {
					rsp = make([]byte, 9)
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
