package linux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux/evt"
)

func initializationTestFeatures() [8]byte {
	return [8]byte{0xa0, 0, 0, 0x70, 0x40, 0, 0x09, 0}
}

func initializationTestCommands() []byte {
	rsp := make([]byte, 65)
	// Command-map offsets exclude Status. Supply the eight optional BR/EDR
	// commands configured by initialization, independently of production helpers.
	for _, bit := range [][2]int{{17, 6}, {12, 7}, {13, 1}, {12, 5}, {9, 1}, {7, 5}, {5, 4}, {10, 6}} {
		rsp[1+bit[0]] |= 1 << bit[1]
	}
	return rsp
}

func initializationTestReply(d *shutdownTestTransport, op uint16, rsp []byte) {
	d.input <- shutdownTestEvent(evt.CommandComplete, append([]byte{1, byte(op), byte(op >> 8)}, rsp...)...)
	synctest.Wait()
}

type initializationTestController struct {
	features    [8]byte
	commands    []byte
	queryStatus bool
	reject      func([]byte) byte
}

func initializationTestRun(t *testing.T, h *HCI, d *shutdownTestTransport, controller initializationTestController) ([][]byte, error) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- h.resetDevice(context.Background()) }()
	var packets [][]byte
	for {
		select {
		case err := <-result:
			pendingTestNoCommand(t, d)
			return packets, err
		case packet := <-d.writes:
			packets = append(packets, packet)
			op := uint16(packet[1]) | uint16(packet[2])<<8
			rsp := []byte{0}
			switch op {
			case 0x1003:
				rsp = append([]byte{0}, controller.features[:]...)
			case 0x1002:
				if controller.queryStatus {
					d.input <- shutdownTestEvent(evt.CommandStatus, controller.commands[0], 1, byte(op), byte(op>>8))
					synctest.Wait()
					continue
				}
				rsp = controller.commands
			}
			if controller.reject != nil {
				if status := controller.reject(packet); status != 0 {
					rsp = []byte{status}
				}
			}
			initializationTestReply(d, op, rsp)
		case <-time.After(hciCloseTimeout):
			t.Fatal("initialization did not finish after controller replies")
			return nil, nil
		}
	}
}

func TestHCIInitializationSkipsUnsupportedInterlacedScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		features := initializationTestFeatures()
		features[3] &^= 0x30 // Interlaced inquiry and page scan are optional.
		_, err := initializationTestRun(t, h, d, initializationTestController{features: features, commands: initializationTestCommands(), reject: func(packet []byte) byte {
			op := uint16(packet[1]) | uint16(packet[2])<<8
			if (op == 0x0c47 || op == 0x0c43) && packet[4] == 1 {
				return 0x11
			}
			return 0
		}})
		if err != nil {
			t.Fatalf("missing optional interlaced scan prevented BLE initialization: %v", err)
		}
	})
}

func initializationTestPacket(packets [][]byte, op uint16) []byte {
	for _, packet := range packets {
		if uint16(packet[1])|uint16(packet[2])<<8 == op {
			return packet
		}
	}
	return nil
}

func TestHCIInitializationSelectsOptionalParameters(t *testing.T) {
	for _, tt := range []struct {
		name       string
		feature    int
		clear      byte
		op         uint16
		parameters []byte
	}{
		{"no_simple_pairing", 6, 0x08, 0x0c56, nil},
		{"inquiry_rssi_without_eir", 6, 0x01, 0x0c45, []byte{1}},
		{"standard_inquiry", 3, 0x40, 0x0c45, nil},
		{"no_interlaced_page", 3, 0x20, 0x0c47, nil},
		{"no_interlaced_inquiry", 3, 0x10, 0x0c43, nil},
		{"only_role_switch", 0, 0x80, 0x080f, []byte{1, 0}},
		{"only_sniff", 0, 0x20, 0x080f, []byte{4, 0}},
		{"no_link_policy_features", 0, 0xa0, 0x080f, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				features := initializationTestFeatures()
				features[tt.feature] &^= tt.clear
				if tt.name == "standard_inquiry" {
					features[6] &^= 0x01
				}
				packets, err := initializationTestRun(t, h, d, initializationTestController{features: features, commands: initializationTestCommands()})
				if err != nil {
					t.Fatal(err)
				}
				packet := initializationTestPacket(packets, tt.op)
				if tt.parameters == nil {
					if packet != nil {
						t.Fatalf("unsupported optional configuration sent: %x", packet)
					}
				} else if len(packet) < 4 || !bytes.Equal(packet[4:], tt.parameters) {
					t.Fatalf("parameters for 0x%04x = %x, want %x", tt.op, packet, tt.parameters)
				}
				if packet := initializationTestPacket(packets, 0x200b); packet == nil {
					t.Fatal("optional-feature filtering skipped required LE initialization")
				}
			})
		})
	}
}

func TestHCIInitializationSkipsUnsupportedOptionalCommands(t *testing.T) {
	for _, tt := range []struct {
		op    uint16
		octet int
		bit   uint
	}{
		{0x0c56, 17, 6}, {0x0c45, 12, 7}, {0x0c47, 13, 1}, {0x0c43, 12, 5},
		{0x0c24, 9, 1}, {0x0c18, 7, 5}, {0x080f, 5, 4}, {0x0c33, 10, 6},
	} {
		t.Run(fmt.Sprintf("0x%04x", tt.op), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				commands := initializationTestCommands()
				commands[1+tt.octet] &^= 1 << tt.bit
				packets, err := initializationTestRun(t, h, d, initializationTestController{features: initializationTestFeatures(), commands: commands, reject: func(packet []byte) byte {
					if uint16(packet[1])|uint16(packet[2])<<8 == tt.op {
						return 0x01
					}
					return 0
				}})
				if err != nil || initializationTestPacket(packets, tt.op) != nil {
					t.Fatalf("unsupported command 0x%04x was required: %v", tt.op, err)
				}
			})
		})
	}
}

func TestHCIInitializationKeepsRequiredLECommandsWithEmptyCommandMap(t *testing.T) {
	for _, leReported := range []bool{false, true} {
		t.Run(fmt.Sprintf("LE_reported=%v", leReported), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				features := initializationTestFeatures()
				if !leReported {
					features[4] &^= 0x40 // Preserve LnxDeviceID(chk=false).
				}
				packets, err := initializationTestRun(t, h, d, initializationTestController{features: features, commands: make([]byte, 65)})
				if err != nil {
					t.Fatal(err)
				}
				var ops []uint16
				for _, packet := range packets {
					ops = append(ops, uint16(packet[1])|uint16(packet[2])<<8)
				}
				want := []uint16{0x0c03, 0x1003, 0x1002, 0x0c01, 0x2001, 0x0c6d, 0x200b}
				if !slices.Equal(ops, want) {
					t.Fatalf("empty command map initialization = %x, want %x", ops, want)
				}
				if got := initializationTestPacket(packets, 0x0c6d); !bytes.Equal(got[4:], []byte{1, 0}) {
					t.Fatalf("LE host support changed: %x", got)
				}
			})
		})
	}
}

func TestHCIInitializationUnknownCommandQueryUsesFeatureFallback(t *testing.T) {
	for _, tt := range []struct {
		name   string
		rsp    []byte
		status bool
	}{
		{"short_complete", []byte{1}, false},
		{"full_complete", append([]byte{1}, make([]byte, 64)...), false},
		{"command_status", []byte{1}, true},
	} {
		for _, interlaced := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/interlaced=%v", tt.name, interlaced), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					h, d := newShutdownTestHCI(t, false)
					features := initializationTestFeatures()
					if !interlaced {
						features[3] &^= 0x30
					}
					packets, err := initializationTestRun(t, h, d, initializationTestController{features: features, commands: tt.rsp, queryStatus: tt.status})
					if err != nil {
						t.Fatal(err)
					}
					var ops []uint16
					for _, packet := range packets {
						ops = append(ops, uint16(packet[1])|uint16(packet[2])<<8)
					}
					want := []uint16{0x0c03, 0x1003, 0x1002, 0x0c01, 0x2001, 0x0c56, 0x0c6d, 0x0c45}
					if interlaced {
						want = append(want, 0x0c47, 0x0c43)
					}
					want = append(want, 0x0c24, 0x0c18, 0x080f, 0x0c33, 0x200b)
					if !slices.Equal(ops, want) {
						t.Fatalf("query fallback sequence = %x, want %x", ops, want)
					}
				})
			})
		}
	}
}

func TestHCIInitializationRejectsMalformedOrFailedCommandQuery(t *testing.T) {
	for _, tt := range []struct {
		name   string
		rsp    []byte
		status bool
	}{
		{"missing_status", nil, false},
		{"success_without_commands", []byte{0}, false},
		{"truncated_success", make([]byte, 64), false},
		{"oversized_success", make([]byte, 66), false},
		{"command_disallowed", []byte{0x0c}, false},
		{"unsupported_parameter", []byte{0x11}, false},
		{"truncated_unknown_command", []byte{1, 0}, false},
		{"long_truncated_unknown_command", append([]byte{1}, make([]byte, 63)...), false},
		{"oversized_unknown_command", append([]byte{1}, make([]byte, 65)...), false},
		{"failed_command_status", []byte{0x0c}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				packets, err := initializationTestRun(t, h, d, initializationTestController{features: initializationTestFeatures(), commands: tt.rsp, queryStatus: tt.status})
				if err == nil || !strings.Contains(err.Error(), "0x1002") {
					t.Fatalf("invalid command-query reply = %v, want opcode context", err)
				}
				if len(packets) != 3 {
					t.Fatalf("failed command query continued initialization: %x", packets)
				}
			})
		})
	}
}

func TestHCIInitializationCommandQueryRespectsCallerContext(t *testing.T) {
	for _, mode := range []string{"cancel", "timeout", "zero_credit_cancel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				ctx, cancel := context.WithTimeout(context.Background(), hciCloseTimeout)
				defer cancel()
				started := time.Now()
				result := make(chan error, 1)
				go func() { result <- h.resetDevice(ctx) }()
				shutdownTestCommand(t, d, 0x0c03)
				initializationTestReply(d, 0x0c03, []byte{0})
				// Capability discovery shares the caller's original deadline.
				time.Sleep(2 * time.Second)
				shutdownTestCommand(t, d, 0x1003)
				features := initializationTestFeatures()
				credits := byte(1)
				if mode == "zero_credit_cancel" {
					credits = 0
				}
				d.input <- shutdownTestEvent(evt.CommandComplete, append([]byte{credits, 3, 0x10, 0}, features[:]...)...)
				synctest.Wait()
				if mode == "zero_credit_cancel" {
					pendingTestNoCommand(t, d)
				} else {
					shutdownTestCommand(t, d, 0x1002)
				}
				want := error(context.Canceled)
				if mode == "timeout" {
					want = context.DeadlineExceeded
					time.Sleep(3 * time.Second)
				} else {
					cancel()
				}
				synctest.Wait()
				if err := pendingTestResult(t, result); !errors.Is(err, want) {
					t.Fatalf("capability query context failure = %v, want %v", err, want)
				}
				if mode == "timeout" && time.Since(started) != hciCloseTimeout {
					t.Fatalf("capability query reset the initialization deadline: %v", time.Since(started))
				}
				pendingTestNoCommand(t, d)
			})
		})
	}
}
