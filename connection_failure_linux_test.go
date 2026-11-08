package gatt

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux"
)

type connectionFailureResult struct {
	p   Peripheral
	err error
}

func TestDeviceConnectionFailureUsesOriginalRequestAndCanStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		source := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}}
		called := make(chan connectionFailureResult, 1)
		stopped := make(chan error, 1)
		d.peripheralConnected = func(p Peripheral, err error) {
			called <- connectionFailureResult{p, err}
			stopped <- d.Stop()
		}
		want := errors.New("controller rejected connection")
		result := make(chan linux.ConnectionResult, 1)
		d.connectionResult(source, result)
		result <- linux.ConnectionResult{Peripheral: source.pd, Err: want}
		close(result)
		synctest.Wait()
		got := <-called
		if got.p != source || !errors.Is(got.err, want) {
			t.Fatalf("failure callback = %+v", got)
		}
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		select {
		case extra := <-called:
			t.Fatalf("duplicate failure callback: %+v", extra)
		default:
		}
	})
}

func TestDeviceConnectionFailuresDoNotOverwriteOtherRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		defer d.Stop()
		first := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}}
		second := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{2}}}
		called := make(chan connectionFailureResult, 2)
		d.peripheralConnected = func(p Peripheral, err error) { called <- connectionFailureResult{p, err} }
		original, busy := make(chan linux.ConnectionResult, 1), make(chan linux.ConnectionResult, 1)
		d.connectionResult(first, original)
		busy <- linux.ConnectionResult{Peripheral: second.pd, Err: errors.New("already pending")}
		close(busy)
		d.connectionResult(second, busy)
		synctest.Wait()
		if got := <-called; got.p != second || got.err == nil {
			t.Fatalf("busy request callback = %+v", got)
		}
		want := errors.New("first attempt failed")
		original <- linux.ConnectionResult{Peripheral: first.pd, Err: want}
		close(original)
		synctest.Wait()
		if got := <-called; got.p != first || !errors.Is(got.err, want) {
			t.Fatalf("original request callback = %+v", got)
		}
	})
}

type connectionAdmissionContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
}

func (c connectionAdmissionContext) Err() error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return c.Context.Err()
}

type connectionAdmissionConn struct {
	*lifecycleTestConn
	intentional atomic.Bool
}

func (c *connectionAdmissionConn) ConnectionCanceled() bool { return c.intentional.Load() }

func TestDeviceConnectionAdmissionDistinguishesRemoteCloseCancelAndStop(t *testing.T) {
	for _, mode := range []string{"remote-close", "cancel", "stop"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := &connectionAdmissionConn{lifecycleTestConn: newLifecycleTestConn()}
				gate := connectionAdmissionContext{Context: conn.ctx, entered: make(chan struct{}, 2), release: make(chan struct{})}
				conn.ctx = gate
				d := &device{hci: &linux.HCI{}, closeHCI: conn.Close}
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
				var stopped chan error
				if mode == "stop" {
					stopped = make(chan error, 1)
					go func() { stopped <- d.Stop() }()
					synctest.Wait()
				} else {
					conn.intentional.Store(mode == "cancel")
					conn.cancel()
				}
				close(gate.release)
				synctest.Wait()
				if mode == "remote-close" {
					got := <-called
					if got.p != source || !errors.Is(got.err, io.ErrClosedPipe) {
						t.Fatalf("early remote close callback = %+v", got)
					}
				}
				select {
				case extra := <-called:
					t.Fatalf("unexpected callback after %s: %+v", mode, extra)
				case <-disconnected:
					t.Fatal("unpublished connection reported Disconnected")
				default:
				}
				if stopped != nil {
					if err := <-stopped; err != nil {
						t.Fatal(err)
					}
				} else if err := d.Stop(); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestDeviceConnectionAdmissionReportsLifecycleCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newLifecycleTestConn()
		d := &device{hci: &linux.HCI{}, closeHCI: conn.Close}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		defer d.Stop()
		for i := 0; i < cap(d.lifecycleSlots); i++ {
			d.lifecycleSlots <- struct{}{}
		}
		source := &peripheral{d: d, pd: &linux.PlatData{Address: [6]byte{1}}}
		called := make(chan connectionFailureResult, 1)
		d.peripheralConnected = func(p Peripheral, err error) { called <- connectionFailureResult{p, err} }
		d.acceptPeripheral(&linux.PlatData{Conn: conn}, source)
		synctest.Wait()
		if got := <-called; got.p != source || got.err == nil || !strings.Contains(got.err.Error(), "capacity exhausted") {
			t.Fatalf("capacity failure = %+v", got)
		}
		if conn.Context().Err() == nil {
			t.Fatal("rejected connection was not closed")
		}
	})
}
