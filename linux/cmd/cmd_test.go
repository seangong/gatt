package cmd

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

const testCommandOpcode = 0xfc01

type commandTestParam byte

func (p commandTestParam) Marshal(b []byte) { b[0] = byte(p) }
func (commandTestParam) Opcode() int        { return testCommandOpcode }
func (commandTestParam) Len() int           { return 1 }

type commandTestWriter func([]byte) (int, error)

func (f commandTestWriter) Write(b []byte) (int, error) { return f(b) }

func TestConcurrentSendSerializesCommandTransactions(t *testing.T) {
	const count = 32
	writes := make(chan []byte, count)
	c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
		writes <- append([]byte(nil), b...)
		return len(b), nil
	}))
	defer c.Close()
	start := make(chan struct{})
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(value byte) {
			<-start
			response, err := c.Send(commandTestParam(value))
			if err == nil && !bytes.Equal(response, []byte{value}) {
				err = errors.New("response belongs to a different command")
			}
			errs <- err
		}(byte(i))
	}
	close(start)
	for i := 0; i < count; i++ {
		var raw []byte
		select {
		case raw = <-writes:
		case <-time.After(time.Second):
			t.Fatal("command was not written")
		}
		if i == 0 {
			select {
			case <-writes:
				t.Fatal("another command was written before the first completed")
			case <-time.After(25 * time.Millisecond):
			}
		}
		opcode := raw[1:3]
		if err := c.HandleComplete([]byte{1, opcode[0], opcode[1], raw[4]}); err != nil {
			t.Fatalf("complete command: %v", err)
		}
	}
	for i := 0; i < count; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Errorf("Send: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Send did not return after completion")
		}
	}
}

func TestCloseUnblocksPendingAndQueuedCommands(t *testing.T) {
	writes := make(chan []byte, 2)
	c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
		writes <- append([]byte(nil), b...)
		return len(b), nil
	}))
	defer c.Close()
	errs := make(chan error, 2)
	go func() { _, err := c.Send(commandTestParam(1)); errs <- err }()
	select {
	case <-writes:
	case <-time.After(time.Second):
		t.Fatal("first command did not start")
	}
	go func() { _, err := c.Send(commandTestParam(2)); errs <- err }()
	c.Close()
	c.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Errorf("Send after Close = %v, want closed pipe", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not release Send")
		}
	}
	if _, err := c.Send(commandTestParam(3)); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("new Send after Close = %v, want closed pipe", err)
	}
	if len(writes) != 0 {
		t.Errorf("%d commands were written after Close", len(writes))
	}
}

func TestCloseRejectsCommandEvents(t *testing.T) {
	c := NewCmd(commandTestWriter(func(b []byte) (int, error) { return len(b), nil }))
	c.Close()
	if err := c.HandleComplete([]byte{1, 1, 0xfc, 0}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("HandleComplete after Close = %v, want closed pipe", err)
	}
	if err := c.HandleStatus([]byte{0, 1, 1, 0xfc}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("HandleStatus after Close = %v, want closed pipe", err)
	}
}

func TestFailedSendDoesNotConsumeNextResponse(t *testing.T) {
	transportErr := errors.New("command transport failed")
	for _, shortWrite := range []bool{false, true} {
		name := "write error"
		if shortWrite {
			name = "short write"
		}
		t.Run(name, func(t *testing.T) {
			writes := make(chan []byte, 1)
			c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
				if b[4] == 1 {
					if shortWrite {
						return len(b) - 1, nil
					}
					return 0, transportErr
				}
				writes <- append([]byte(nil), b...)
				return len(b), nil
			}))
			defer c.Close()
			if _, err := c.Send(commandTestParam(1)); err == nil {
				t.Fatal("failed write returned success")
			}
			result := make(chan error, 1)
			go func() {
				response, err := c.Send(commandTestParam(2))
				if err == nil && !bytes.Equal(response, []byte{2}) {
					err = errors.New("wrong command response")
				}
				result <- err
			}()
			select {
			case <-writes:
			case <-time.After(time.Second):
				t.Fatal("second command was not written")
			}
			if err := c.HandleComplete([]byte{1, 1, 0xfc, 2}); err != nil {
				t.Fatalf("complete second command: %v", err)
			}
			select {
			case err := <-result:
				if err != nil {
					t.Errorf("second Send: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("failed command consumed the next command's response")
			}
		})
	}
}

func TestCommandStatusCompletesSend(t *testing.T) {
	written := make(chan struct{}, 1)
	c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
		written <- struct{}{}
		return len(b), nil
	}))
	defer c.Close()
	done := make(chan error, 1)
	go func() { _, err := c.Send(commandTestParam(1)); done <- err }()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("command was not written")
	}
	if err := c.HandleStatus([]byte{0, 1, 1, 0xfc}); err != nil {
		t.Fatalf("deliver command status: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Send returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("command status did not finish Send")
	}
}
