package cmd

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func creditTestEvent(t *testing.T, c *Cmd, status bool, credits byte, opcode uint16, value byte) {
	t.Helper()
	var err error
	if status {
		err = c.HandleStatus([]byte{0, credits, byte(opcode), byte(opcode >> 8)})
	} else {
		err = c.HandleComplete([]byte{credits, byte(opcode), byte(opcode >> 8), value})
	}
	if err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
}

func creditTestResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("command did not finish")
		return nil
	}
}

func TestCommandCreditsFollowBothEventsAndKeepTransactionsSerial(t *testing.T) {
	for _, status := range []bool{false, true} {
		name := "complete"
		if status {
			name = "status"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				writes := make(chan []byte, 3)
				c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
					writes <- append([]byte(nil), b...)
					return len(b), nil
				}))
				defer func() { c.Close(); c.Wait() }()
				first := make(chan error, 1)
				go func() { _, err := c.Send(commandTestParam(1)); first <- err }()
				<-writes // The initial command is allowed without an earlier event.
				creditTestEvent(t, c, status, 0, 0, 0)
				select {
				case err := <-first:
					t.Fatalf("opcode zero completed an in-flight command: %v", err)
				default:
				}
				creditTestEvent(t, c, status, 0, testCommandOpcode, 1)
				if err := creditTestResult(t, first); err != nil {
					t.Fatal(err)
				}

				second := make(chan error, 1)
				go func() { _, err := c.Send(commandTestParam(2)); second <- err }()
				synctest.Wait()
				if len(writes) != 0 {
					t.Fatal("command sent with zero controller credits")
				}
				creditTestEvent(t, c, !status, 2, 0, 0)
				if raw := <-writes; raw[4] != 2 {
					t.Fatalf("resumed command = %x, want value 2", raw)
				}
				third := make(chan error, 1)
				go func() { _, err := c.Send(commandTestParam(3)); third <- err }()
				synctest.Wait()
				if len(writes) != 0 {
					t.Fatal("multiple controller credits allowed overlapping transactions")
				}
				creditTestEvent(t, c, !status, 0, testCommandOpcode, 2)
				if err := creditTestResult(t, second); err != nil {
					t.Fatal(err)
				}
				if len(writes) != 0 {
					t.Fatal("absolute zero credit update retained an older grant")
				}
				creditTestEvent(t, c, status, 1, 0, 0)
				if raw := <-writes; raw[4] != 3 {
					t.Fatalf("next command = %x, want value 3", raw)
				}
				creditTestEvent(t, c, status, 1, testCommandOpcode, 3)
				if err := creditTestResult(t, third); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestCommandCreditWaitCancellationDoesNotStopProcessor(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		name := "ordinary"
		if cleanup {
			name = "cleanup"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				writes := make(chan []byte, 2)
				c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
					writes <- append([]byte(nil), b...)
					return len(b), nil
				}))
				defer func() { c.Close(); c.Wait() }()
				creditTestEvent(t, c, false, 0, 0, 0)
				send := c.SendContext
				if cleanup {
					c.BeginShutdown()
					send = c.SendShutdownContext
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := make(chan error, 1)
				go func() { _, err := send(ctx, commandTestParam(1)); result <- err }()
				synctest.Wait()
				if len(writes) != 0 {
					t.Fatal("credit waiter reached the transport")
				}
				cancel()
				if err := creditTestResult(t, result); !errors.Is(err, context.Canceled) {
					t.Fatalf("credit wait cancellation = %v", err)
				}
				creditTestEvent(t, c, true, 1, 0, 0)
				go func() { _, err := send(context.Background(), commandTestParam(2)); result <- err }()
				if raw := <-writes; raw[4] != 2 {
					t.Fatalf("next command = %x, want value 2", raw)
				}
				creditTestEvent(t, c, false, 1, testCommandOpcode, 2)
				if err := creditTestResult(t, result); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestBeginShutdownReleasesOrdinaryCreditWaitButCleanupNeedsCredits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 2)
		c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
			writes <- append([]byte(nil), b...)
			return len(b), nil
		}))
		defer func() { c.Close(); c.Wait() }()
		creditTestEvent(t, c, false, 0, 0, 0)
		ordinary, cleanup := make(chan error, 1), make(chan error, 1)
		go func() { _, err := c.Send(commandTestParam(1)); ordinary <- err }()
		synctest.Wait()
		go func() { _, err := c.SendShutdownContext(context.Background(), commandTestParam(2)); cleanup <- err }()
		synctest.Wait()
		c.BeginShutdown()
		if err := creditTestResult(t, ordinary); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("ordinary credit wait after shutdown = %v", err)
		}
		synctest.Wait()
		if len(writes) != 0 {
			t.Fatal("cleanup command bypassed controller credits")
		}
		creditTestEvent(t, c, true, 1, 0, 0)
		if raw := <-writes; raw[4] != 2 {
			t.Fatalf("shutdown command = %x, want value 2", raw)
		}
		creditTestEvent(t, c, false, 1, testCommandOpcode, 2)
		if err := creditTestResult(t, cleanup); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCommandCreditWaitExitsOnDeadlineAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 2)
		c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
			writes <- append([]byte(nil), b...)
			return len(b), nil
		}))
		defer func() { c.Close(); c.Wait() }()
		creditTestEvent(t, c, false, 0, 0, 0)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		deadline := make(chan error, 1)
		go func() { _, err := c.SendShutdownContext(ctx, commandTestParam(1)); deadline <- err }()
		time.Sleep(time.Second)
		synctest.Wait()
		if err := creditTestResult(t, deadline); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup credit deadline = %v", err)
		}
		waiting := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() { _, err := c.Send(commandTestParam(2)); waiting <- err }()
		}
		synctest.Wait()
		c.Close()
		for i := 0; i < 2; i++ {
			if err := creditTestResult(t, waiting); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("credit or serial waiter after Close = %v", err)
			}
		}
		if len(writes) != 0 {
			t.Fatal("a credit waiter reached the transport")
		}
	})
}

func TestFailedWriteRefundPreservesNewAbsoluteCredits(t *testing.T) {
	for _, short := range []bool{false, true} {
		for _, updated := range []bool{false, true} {
			name := "error"
			if short {
				name = "short"
			}
			if updated {
				name += "/updated"
			} else {
				name += "/unchanged"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					writes, entered := make(chan []byte, 2), make(chan struct{})
					release := make(chan struct{})
					want := errors.New("command write failed")
					c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
						if b[4] == 1 {
							close(entered)
							<-release
							if short {
								return len(b) - 1, nil
							}
							return 0, want
						}
						writes <- append([]byte(nil), b...)
						return len(b), nil
					}))
					defer func() { c.Close(); c.Wait() }()
					defer func() {
						select {
						case <-release:
						default:
							close(release)
						}
					}()
					result := make(chan error, 1)
					go func() { _, err := c.Send(commandTestParam(1)); result <- err }()
					<-entered
					if updated {
						creditTestEvent(t, c, false, 0, 0, 0)
					}
					close(release)
					if err := creditTestResult(t, result); err == nil || !short && !errors.Is(err, want) {
						t.Fatalf("failed write = %v", err)
					}
					go func() { _, err := c.Send(commandTestParam(2)); result <- err }()
					synctest.Wait()
					if updated {
						if len(writes) != 0 {
							t.Fatal("failed-write refund overwrote newer absolute zero credits")
						}
						creditTestEvent(t, c, true, 1, 0, 0)
					}
					if raw := <-writes; raw[4] != 2 {
						t.Fatalf("next command = %x, want value 2", raw)
					}
					creditTestEvent(t, c, false, 1, testCommandOpcode, 2)
					if err := creditTestResult(t, result); err != nil {
						t.Fatal(err)
					}
				})
			})
		}
	}
}

func TestFullWriteWithErrorRetiresProcessorWithoutRefund(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 2)
		want := errors.New("full command write reported an error")
		c := NewCmd(commandTestWriter(func(b []byte) (int, error) {
			if b[4] == 1 {
				return len(b), want
			}
			writes <- append([]byte(nil), b...)
			return len(b), nil
		}))
		defer func() { c.Close(); c.Wait() }()
		if _, err := c.Send(commandTestParam(1)); !errors.Is(err, want) {
			t.Fatalf("full write with error = %v, want original error", err)
		}
		result := make(chan error, 1)
		go func() { _, err := c.Send(commandTestParam(2)); result <- err }()
		synctest.Wait()
		if len(writes) != 0 {
			t.Fatal("full write with error incorrectly refunded its credit")
		}
		if err := creditTestResult(t, result); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("command after a full write error = %v, want closed pipe", err)
		}
		c.sentMu.Lock()
		credits := c.credits
		c.sentMu.Unlock()
		if credits != 0 {
			t.Fatalf("complete command refunded credit: %d", credits)
		}
		if err := c.HandleStatus([]byte{0, 1, 0, 0}); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("credit event after full write error = %v, want closed pipe", err)
		}
	})
}
