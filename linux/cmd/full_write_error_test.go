package cmd

import (
	"errors"
	"io"
	"testing"
	"testing/synctest"
)

func TestFullWriteErrorCannotCompleteLaterCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 2)
		want := errors.New("complete command write reported an error")
		c := NewCmd(commandTestWriter(func(packet []byte) (int, error) {
			writes <- append([]byte(nil), packet...)
			return len(packet), want
		}))
		defer func() { c.Close(); c.Wait() }()
		creditTestEvent(t, c, false, 2, 0, 0)
		if _, err := c.Send(commandTestParam(1)); !errors.Is(err, want) {
			t.Fatalf("complete write error = %v, want original error", err)
		}
		select {
		case <-c.done:
		default:
			t.Fatal("a fully submitted command with an error left the processor open to late-response reuse")
		}
		if _, err := c.Send(commandTestParam(2)); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("later command = %v, want closed pipe", err)
		}
		if err := c.HandleComplete([]byte{1, byte(testCommandOpcode & 0xff), byte(testCommandOpcode >> 8), 1}); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("late completion = %v, want closed pipe", err)
		}
		if len(writes) != 1 {
			t.Fatalf("transport received %d packets, want only the submitted command", len(writes))
		}
		c.sentMu.Lock()
		credits := c.credits
		c.sentMu.Unlock()
		if credits != 1 {
			t.Fatalf("remaining command credits = %d, want 1 without refund", credits)
		}
	})
}
