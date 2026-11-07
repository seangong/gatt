package linux

import (
	"strings"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestHCISendCmdWithAdvOffSkipsDisableWhenAdvertisingIsOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		result := make(chan error, 1)
		go func() { result <- h.SendCmdWithAdvOff(cmd.LESetAdvertisingParameters{}) }()
		opcode := uint16(cmd.LESetAdvertisingParameters{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		if err := <-result; err != nil {
			t.Fatalf("configure advertising while disabled = %v, want nil", err)
		}
		select {
		case packet := <-d.writes:
			t.Fatalf("extra command after configuration = %x", packet)
		default:
		}
	})
}

func TestHCISendCmdWithAdvOffRestoresAdvertising(t *testing.T) {
	for _, tt := range []struct {
		name          string
		configStatus  byte
		restoreStatus byte
	}{
		{name: "successful configuration"},
		{name: "configuration rejected", configStatus: 0x12},
		{name: "restore rejected", restoreStatus: 0x0c},
		{name: "configuration and restore rejected", configStatus: 0x12, restoreStatus: 0x0c},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				h.advmu.Lock()
				h.adv = true
				h.advmu.Unlock()
				result := make(chan error, 1)
				go func() { result <- h.SendCmdWithAdvOff(cmd.LESetAdvertisingParameters{}) }()
				advertisingOpcode := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
				packet := shutdownTestCommand(t, d, advertisingOpcode)
				if len(packet) != 5 || packet[4] != 0 {
					t.Fatalf("advertising command before configuration = %x, want disable", packet)
				}
				shutdownTestCommandComplete(d, advertisingOpcode, 0)
				configOpcode := uint16(cmd.LESetAdvertisingParameters{}.Opcode())
				shutdownTestCommand(t, d, configOpcode)
				shutdownTestCommandComplete(d, configOpcode, tt.configStatus)
				packet = shutdownTestCommand(t, d, advertisingOpcode)
				if len(packet) != 5 || packet[4] != 1 {
					t.Fatalf("advertising command after configuration = %x, want enable", packet)
				}
				shutdownTestCommandComplete(d, advertisingOpcode, tt.restoreStatus)
				err := <-result
				if tt.configStatus == 0 && tt.restoreStatus == 0 {
					if err != nil {
						t.Fatalf("configure advertising = %v, want nil", err)
					}
				} else {
					if err == nil {
						t.Fatal("configure advertising ignored a rejected command")
					}
					if tt.configStatus != 0 && (!strings.Contains(err.Error(), "0x2006") || !strings.Contains(err.Error(), "0x12")) {
						t.Errorf("configure advertising error = %v, want configuration rejection", err)
					}
					if tt.restoreStatus != 0 && (!strings.Contains(err.Error(), "0x200a") || !strings.Contains(err.Error(), "0x0C")) {
						t.Errorf("configure advertising error = %v, want restore rejection", err)
					}
				}
				select {
				case packet := <-d.writes:
					t.Fatalf("extra command after restoring advertising = %x", packet)
				default:
				}
			})
		})
	}
}

func TestHCISendCmdWithAdvOffStopsWhenDisableIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.advmu.Lock()
		h.adv = true
		h.advmu.Unlock()
		result := make(chan error, 1)
		go func() { result <- h.SendCmdWithAdvOff(cmd.LESetAdvertisingParameters{}) }()
		opcode := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		packet := shutdownTestCommand(t, d, opcode)
		if len(packet) != 5 || packet[4] != 0 {
			t.Fatalf("advertising command = %x, want disable", packet)
		}
		shutdownTestCommandComplete(d, opcode, 0x0c)
		if err := <-result; err == nil || !strings.Contains(err.Error(), "0x200a") || !strings.Contains(err.Error(), "0x0C") || !strings.Contains(err.Error(), "enabled=false") {
			t.Fatalf("configure advertising = %v, want disable rejection", err)
		}
		select {
		case packet := <-d.writes:
			t.Fatalf("command after rejected disable = %x, want no configuration", packet)
		default:
		}
	})
}

func TestHCISetAdvertiseEnableReportsControllerRejection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		result := make(chan error, 1)
		go func() { result <- h.SetAdvertiseEnable(true) }()
		opcode := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
		packet := shutdownTestCommand(t, d, opcode)
		if len(packet) != 5 || packet[4] != 1 {
			t.Fatalf("advertising command = %x, want enable", packet)
		}
		shutdownTestCommandComplete(d, opcode, 0x0c)
		if err := <-result; err == nil || !strings.Contains(err.Error(), "0x200a") || !strings.Contains(err.Error(), "0x0C") || !strings.Contains(err.Error(), "enabled=true") {
			t.Fatalf("enable advertising = %v, want controller rejection", err)
		}
	})
}
