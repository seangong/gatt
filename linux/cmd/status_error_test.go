package cmd

import (
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
)

func TestCommandStatusErrorPreservesCodeOpcodeAndText(t *testing.T) {
	for _, status := range []byte{0x01, 0x0c, 0x11} {
		t.Run(fmt.Sprintf("0x%02x", status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				writes := make(chan []byte, 1)
				c := NewCmd(commandTestWriter(func(packet []byte) (int, error) {
					writes <- append([]byte(nil), packet...)
					return len(packet), nil
				}))
				defer func() { c.Close(); c.Wait() }()
				result := make(chan error, 1)
				go func() { _, err := c.Send(commandTestParam(1)); result <- err }()
				<-writes
				if err := c.HandleStatus([]byte{status, 1, 1, 0xfc}); err != nil {
					t.Fatal(err)
				}
				err := creditTestResult(t, result)
				want := fmt.Sprintf("HCI command 0xfc01 failed with status 0x%02x", status)
				if err == nil || err.Error() != want {
					t.Fatalf("status error = %v, want original text %q", err, want)
				}
				var details interface {
					HCIStatus() uint8
					HCIOpcode() uint16
				}
				if !errors.As(fmt.Errorf("controller query: %w", err), &details) ||
					details.HCIStatus() != status || details.HCIOpcode() != testCommandOpcode {
					t.Fatalf("wrapped status error did not retain opcode and status: %v", err)
				}
			})
		})
	}
}
