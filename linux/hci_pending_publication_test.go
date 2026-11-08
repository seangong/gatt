package linux

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestHCICancelWaitsForCreateOutcomePublication(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "rejected_create"},
		{name: "closed_transport", err: io.ErrClosedPipe},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := &shutdownTestTransport{writes: make(chan []byte, 1), closed: make(chan struct{})}
			h := &HCI{d: d, c: cmd.NewCmd(d), connsmu: &sync.Mutex{}, conns: make(map[uint16]*conn)}
			a := newConnectionAttempt(&PlatData{})
			h.connsmu.Lock()
			h.initLifecycle()
			h.pending = a
			h.connsmu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			locked, resultRead := false, false
			defer func() {
				// Restore a complete publication even if an assertion fails while
				// the producer lock is held, then unblock any unwanted command.
				if !locked {
					h.connsmu.Lock()
				}
				a.created()
				a.complete(nil, tt.err)
				h.connsmu.Unlock()
				h.c.Close()
				d.Close()
				cancel()
				if !resultRead {
					select {
					case <-result:
					case <-time.After(2 * time.Second):
						t.Error("cancellation did not exit during cleanup")
					}
				}
				h.c.Wait()
				// An interrupted command can start shared shutdown from the
				// cancellation defer; join it before releasing the fixture.
				h.Close()
			}()
			go func() { result <- h.cancelPendingConnection(ctx, a) }()

			ready := time.NewTicker(time.Millisecond)
			defer ready.Stop()
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for !locked {
				h.connsmu.Lock()
				if a.cancelStarted {
					locked = true
					break
				}
				h.connsmu.Unlock()
				select {
				case <-ready.C:
				case <-deadline.C:
					t.Fatal("cancellation did not start")
				}
			}

			// Both producers publish createDone before the terminal outcome
			// while holding connsmu. Keep that exact intermediate state visible.
			a.created()
			select {
			case packet := <-d.writes:
				t.Fatalf("command sent before the Create outcome was published: %x", packet)
			case err := <-result:
				resultRead = true
				t.Fatalf("cancellation finished before the Create outcome was published: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			a.complete(nil, tt.err)
			h.connsmu.Unlock()
			locked = false

			select {
			case err := <-result:
				resultRead = true
				if !errors.Is(err, tt.err) {
					t.Fatalf("cancellation = %v, want %v", err, tt.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancellation did not finish after the outcome was published")
			}
			select {
			case packet := <-d.writes:
				t.Fatalf("terminal Create outcome sent a command: %x", packet)
			default:
			}
			h.connsmu.Lock()
			pending := h.pending
			h.connsmu.Unlock()
			if pending != nil {
				t.Fatal("completed cancellation retained its pending attempt")
			}
		})
	}
}
