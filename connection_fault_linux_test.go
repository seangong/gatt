package gatt

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux"
)

type faultResultContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *faultResultContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Context.Done()
}

func TestDeviceHCIFaultPreservesPendingConnectFailure(t *testing.T) {
	for _, mode := range []string{"fault", "fault before observer", "fault with reentrant Stop", "fault with earlier command error", "explicit Stop"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cause := errors.New("controller failed during Connect")
				resultCause := cause
				if mode == "fault with earlier command error" {
					resultCause = errors.New("connection command was rejected")
				}
				terminated := make(chan struct{})
				d := &device{hci: &linux.HCI{}, hciDone: terminated, closeHCI: func() error { return cause }}
				var stateCallback func(Device, State)
				reentered := make(chan error, 1)
				if mode == "fault with reentrant Stop" {
					stateCallback = func(_ Device, state State) {
						if state == StatePoweredOff {
							reentered <- d.Stop()
						}
					}
				}
				if err := d.Init(stateCallback); err != nil {
					t.Fatal(err)
				}
				called := make(chan connectionFailureResult, 2)
				d.peripheralConnected = func(p Peripheral, err error) { called <- connectionFailureResult{p, err} }
				source := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}}
				gate := &faultResultContext{Context: d.ctx, entered: make(chan struct{}), release: make(chan struct{})}
				d.ctx = gate
				result := make(chan linux.ConnectionResult, 1)
				d.connectionResult(source, result)
				<-gate.entered
				if mode != "fault before observer" {
					go d.watchHCI()
				}
				if mode == "explicit Stop" {
					if err := d.Stop(); !errors.Is(err, cause) {
						t.Fatal(err)
					}
				}
				// HCI publishes its pending result before its stopping signal.
				result <- linux.ConnectionResult{Peripheral: source.pd, Err: resultCause}
				close(result)
				close(terminated)
				if mode == "fault before observer" {
					if err := d.Stop(); !errors.Is(err, cause) {
						t.Fatal(err)
					}
					go d.watchHCI()
				}
				synctest.Wait()
				if mode == "fault with reentrant Stop" {
					if err := <-reentered; !errors.Is(err, cause) {
						t.Fatalf("reentrant Stop lost original fault: %v", err)
					}
				}
				close(gate.release)
				synctest.Wait()
				select {
				case got := <-called:
					if mode == "explicit Stop" {
						t.Fatalf("explicit Stop reported an unpublished connection: %+v", got)
					}
					if got.p != source || !errors.Is(got.err, resultCause) {
						t.Fatalf("failure lost original request or cause: %+v", got)
					}
				default:
					if mode != "explicit Stop" {
						t.Fatal("HCI fault lost the pending Connect failure callback")
					}
				}
				if len(called) != 0 {
					t.Fatal("pending Connect reported more than one terminal result")
				}
			})
		})
	}
}

func TestDeviceHCIFaultDoesNotWaitForFailureCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("controller failed during Connect")
		terminated, entered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		d := &device{hci: &linux.HCI{}, hciDone: terminated, closeHCI: func() error { return cause }}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		finished := make(chan error, 1)
		d.peripheralConnected = func(Peripheral, error) {
			close(entered)
			<-release
			finished <- d.Stop()
		}
		source := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}}
		result := make(chan linux.ConnectionResult, 1)
		d.connectionResult(source, result)
		go d.watchHCI()
		result <- linux.ConnectionResult{Peripheral: source.pd, Err: cause}
		close(result)
		close(terminated)
		synctest.Wait()
		<-entered
		select {
		case <-d.stopDone:
		default:
			t.Fatal("fault shutdown waited for an application failure callback")
		}
		close(release)
		synctest.Wait()
		if err := <-finished; !errors.Is(err, cause) {
			t.Fatalf("failure callback could not reenter Stop: %v", err)
		}
	})
}

func TestDeviceConnectRejectsTerminatedSession(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			terminated := make(chan struct{})
			d := &device{hci: &linux.HCI{}, hciDone: terminated, closeHCI: func() error { return nil }}
			if err := d.Init(nil); err != nil {
				t.Fatal(err)
			}
			defer d.Stop()
			called := make(chan struct{}, 1)
			d.peripheralConnected = func(Peripheral, error) { called <- struct{}{} }
			if stopped {
				if err := d.Stop(); err != nil {
					t.Fatal(err)
				}
			} else {
				close(terminated) // The observer has not run yet.
			}
			// The fixture has no command transport. Rejection must happen before
			// touching HCI, and must not produce a new asynchronous callback.
			d.Connect(&peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}})
			synctest.Wait()
			if len(called) != 0 {
				t.Fatal("a terminated session admitted another Connect")
			}
		})
	}
}

func TestDeviceHCIFaultBeforeSuccessPublicationReportsFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newLifecycleTestConn()
		gate := connectionAdmissionContext{Context: conn.ctx, entered: make(chan struct{}, 2), release: make(chan struct{})}
		conn.ctx = gate
		cause := errors.New("controller failed before connection callback")
		terminated := make(chan struct{})
		d := &device{hci: &linux.HCI{}, hciDone: terminated, closeHCI: func() error { conn.Close(); return cause }}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		called := make(chan connectionFailureResult, 2)
		disconnected := make(chan struct{}, 1)
		d.peripheralConnected = func(p Peripheral, err error) { called <- connectionFailureResult{p, err} }
		d.peripheralDisconnected = func(Peripheral, error) { disconnected <- struct{}{} }
		source := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}}
		result := make(chan linux.ConnectionResult, 1)
		result <- linux.ConnectionResult{Peripheral: &linux.PlatData{Conn: conn}}
		close(result)
		d.connectionResult(source, result)
		<-gate.entered
		go d.watchHCI()
		close(terminated)
		synctest.Wait()
		close(gate.release)
		synctest.Wait()
		select {
		case got := <-called:
			if got.p != source || !errors.Is(got.err, cause) {
				t.Fatalf("unpublished success did not report original fault: %+v", got)
			}
		default:
			t.Fatal("unpublished connection lost its failure result during shutdown")
		}
		if len(called) != 0 || len(disconnected) != 0 {
			t.Fatal("unpublished connection reported duplicate success or Disconnected")
		}
		if err := d.Stop(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	})
}

func TestDeviceHCIFaultBeforeObserverPreservesCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cause := errors.New("controller failed before observer dispatch")
		terminated := make(chan struct{})
		d := &device{hci: &linux.HCI{}, hciDone: terminated, closeHCI: func() error { return cause }}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		called := make(chan connectionFailureResult, 1)
		d.peripheralConnected = func(p Peripheral, err error) { called <- connectionFailureResult{p, err} }
		source := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}}
		// Connect was admitted while HCI was live, but its synchronous command
		// returns after HCI has closed. Do not schedule watchHCI yet.
		close(terminated)
		result := make(chan linux.ConnectionResult, 1)
		result <- linux.ConnectionResult{Peripheral: source.pd, Err: io.ErrClosedPipe}
		close(result)
		d.connectionResult(source, result)
		synctest.Wait()
		select {
		case got := <-called:
			if got.p != source || !errors.Is(got.err, cause) {
				t.Fatalf("failure before observer lost original cause: %+v", got)
			}
		default:
			t.Fatal("failure before observer did not start shared shutdown")
		}
		if err := d.Stop(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	})
}
