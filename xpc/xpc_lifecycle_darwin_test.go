//go:build gattxpc_test

package xpc

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type testEventHandler func(Dict, error)

func (f testEventHandler) HandleXpcEvent(event Dict, err error) { f(event, err) }

func waitConnectionSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestConnectionCallbackSurvivesGC(t *testing.T) {
	received := make(chan struct{}, 1)
	p := newConnectionTestPair(testEventHandler(func(event Dict, err error) {
		if err == nil && event.MustGetInt("number") == 42 {
			received <- struct{}{}
		}
	}))
	defer p.close()
	runtime.GC()
	if err := p.x.SendError(Dict{"number": int64(42)}, false); err != nil {
		t.Fatal(err)
	}
	waitConnectionSignal(t, received, "echo callback")
	_ = p.x.Close()
	waitConnectionSignal(t, p.x.state.released, "native finalizer")
}

func TestConnectionCopiesCloseConcurrently(t *testing.T) {
	p := newConnectionTestPair(nil)
	defer p.close()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		copy := p.x
		wg.Go(func() { _ = copy.Close() })
	}
	wg.Wait()
	waitConnectionSignal(t, p.x.Done(), "Done")
	waitConnectionSignal(t, p.x.state.released, "native finalizer")
	if !errors.Is(p.x.Err(), ErrClosed) {
		t.Fatalf("Err = %v", p.x.Err())
	}
	if err := p.x.SendError(Dict{}, false); !errors.Is(err, ErrClosed) {
		t.Fatalf("SendError after Close = %v", err)
	}
}

func TestConnectionCloseInsideCallback(t *testing.T) {
	ready := make(chan struct{})
	returned := make(chan struct{}, 1)
	var x XPC
	p := newConnectionTestPair(testEventHandler(func(_ Dict, err error) {
		if err != nil {
			return
		}
		<-ready
		_ = x.Close()
		returned <- struct{}{}
	}))
	defer p.close()
	x = p.x
	close(ready)
	p.emit(Dict{}, nil)
	waitConnectionSignal(t, returned, "Close inside callback")
	waitConnectionSignal(t, p.x.state.released, "native finalizer")
}

func TestConnectionCloseRetainsRunningCallback(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	p := newConnectionTestPair(testEventHandler(func(_ Dict, err error) {
		if err == nil {
			close(entered)
			<-unblock
		}
	}))
	defer p.close()
	p.emit(Dict{}, nil)
	waitConnectionSignal(t, entered, "callback entry")
	_ = p.x.Close()
	waitConnectionSignal(t, p.x.Done(), "Done while callback runs")
	select {
	case <-p.x.state.released:
		t.Fatal("handler token released before running callback returned")
	default:
	}
	close(unblock)
	waitConnectionSignal(t, p.x.state.released, "native finalizer")
}

func TestConnectionSendRacesClose(t *testing.T) {
	p := newConnectionTestPair(nil)
	defer p.close()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				err := p.x.SendError(Dict{"number": int64(j)}, false)
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Errorf("SendError = %v", err)
					return
				}
			}
		})
	}
	wg.Go(func() { _ = p.x.Close() })
	wg.Wait()
	waitConnectionSignal(t, p.x.state.released, "native finalizer")
}

func TestConnectionErrorLifecycle(t *testing.T) {
	for _, err := range []error{CONNECTION_INTERRUPTED, CONNECTION_INVALID, CONNECTION_TERMINATED} {
		t.Run(err.Error(), func(t *testing.T) {
			received := make(chan error, 2)
			p := newConnectionTestPair(testEventHandler(func(_ Dict, err error) { received <- err }))
			defer p.close()
			p.emit(nil, err)
			select {
			case got := <-received:
				if got != err {
					t.Fatalf("event error = %v, want %v", got, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("missing error callback")
			}
			if err == CONNECTION_INTERRUPTED {
				if p.x.Err() != nil {
					t.Fatalf("recoverable interruption closed connection: %v", p.x.Err())
				}
				select {
				case <-p.x.Done():
					t.Fatal("recoverable interruption closed Done")
				default:
				}
				_ = p.x.Close()
			} else if !errors.Is(p.x.Err(), err) {
				t.Fatalf("terminal Err = %v, want %v", p.x.Err(), err)
			}
			waitConnectionSignal(t, p.x.state.released, "native finalizer")
		})
	}
}

func TestConnectionCreationFailure(t *testing.T) {
	x := failedTestConnection(testEventHandler(func(Dict, error) {
		t.Error("creation failure must not synchronously call handler")
	}))
	if !errors.Is(x.Err(), CONNECTION_INVALID) {
		t.Fatalf("Err = %v", x.Err())
	}
	waitConnectionSignal(t, x.Done(), "failed creation Done")
	waitConnectionSignal(t, x.state.released, "failed creation handle release")
	_ = x.Close()
}

func TestConnectionInvalidAtActivation(t *testing.T) {
	received := make(chan struct{}, 1)
	p := newInvalidConnectionTestPair(testEventHandler(func(_ Dict, err error) {
		if err == CONNECTION_INVALID {
			received <- struct{}{}
		}
	}))
	defer p.close()
	waitConnectionSignal(t, received, "immediate INVALID")
	waitConnectionSignal(t, p.x.Done(), "immediate INVALID Done")
	waitConnectionSignal(t, p.x.state.released, "immediate INVALID finalizer")
	if !errors.Is(p.x.Err(), CONNECTION_INVALID) {
		t.Fatalf("Err = %v", p.x.Err())
	}
}

func TestZeroConnection(t *testing.T) {
	var x XPC
	if !errors.Is(x.Err(), ErrClosed) || !errors.Is(x.SendError(Dict{}, false), ErrClosed) {
		t.Fatal("zero connection must report ErrClosed")
	}
	waitConnectionSignal(t, x.Done(), "zero connection Done")
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}
}
