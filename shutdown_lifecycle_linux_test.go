package gatt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/linux"
)

type lifecycleTestConn struct {
	ctx       context.Context
	cancel    context.CancelFunc
	reads     chan []byte
	writes    chan []byte
	readCalls atomic.Int32
}

func newLifecycleTestConn() *lifecycleTestConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &lifecycleTestConn{ctx: ctx, cancel: cancel, reads: make(chan []byte, 2), writes: make(chan []byte, 2)}
}

func (c *lifecycleTestConn) Context() context.Context { return c.ctx }
func (c *lifecycleTestConn) Close() error             { c.cancel(); return nil }
func (c *lifecycleTestConn) Read(b []byte) (int, error) {
	c.readCalls.Add(1)
	select {
	case <-c.ctx.Done():
		return 0, io.EOF
	case p := <-c.reads:
		return copy(b, p), nil
	}
}
func (c *lifecycleTestConn) Write(b []byte) (int, error) {
	select {
	case <-c.ctx.Done():
		return 0, io.ErrClosedPipe
	case c.writes <- append([]byte(nil), b...):
		return len(b), nil
	}
}

func TestDeviceConnectedCompletesBeforeATTAndDisconnect(t *testing.T) {
	conn := newLifecycleTestConn()
	defer conn.Close()
	d := &device{hci: &linux.HCI{}, closeHCI: conn.Close}
	s := NewService(UUID16(0xff00))
	c := s.AddCharacteristic(UUID16(0xff01))
	c.SetValue([]byte("ready"))
	if err := d.AddService(s); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	d.centralConnected = func(Central) { close(entered); <-release }
	disconnected := make(chan error, 1)
	d.centralDisconnected = func(Central) { disconnected <- d.Stop() }
	if err := d.Init(nil); err != nil {
		t.Fatal(err)
	}
	conn.reads <- []byte{attOpReadReq, byte(c.vh), byte(c.vh >> 8)}
	d.hci.AcceptMasterHandler(&linux.PlatData{Conn: conn})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Connected callback did not start")
	}
	if got := conn.readCalls.Load(); got != 0 {
		t.Fatalf("ATT read started %d times before Connected completed", got)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case response := <-conn.writes:
		if !bytes.Equal(response, append([]byte{attOpReadRsp}, []byte("ready")...)) {
			t.Fatalf("read response = %x", response)
		}
	case <-time.After(time.Second):
		t.Fatal("ATT did not start after Connected completed")
	}
	if err := d.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-disconnected:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Disconnected could not reenter Stop")
	}
}

func TestDeviceConnectedCanStopBeforeReturning(t *testing.T) {
	conn := newLifecycleTestConn()
	defer conn.Close()
	d := &device{hci: &linux.HCI{}, closeHCI: conn.Close}
	connectedResult, disconnected := make(chan error, 1), make(chan struct{})
	d.centralConnected = func(Central) { connectedResult <- d.Stop() }
	d.centralDisconnected = func(Central) { close(disconnected) }
	if err := d.Init(nil); err != nil {
		t.Fatal(err)
	}
	d.hci.AcceptMasterHandler(&linux.PlatData{Conn: conn})
	select {
	case err := <-connectedResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Connected deadlocked when calling Stop")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("Disconnected did not follow Connected")
	}
	if got := conn.readCalls.Load(); got != 0 {
		t.Fatalf("ATT started after Connected stopped device: %d", got)
	}
}

func TestDeviceClientConnectedCanMakeSynchronousRequests(t *testing.T) {
	conn := newLifecycleTestConn()
	d := &device{hci: &linux.HCI{}, closeHCI: conn.Close}
	defer d.Stop()
	result := make(chan error, 1)
	d.peripheralConnected = func(p Peripheral, err error) {
		if err != nil {
			result <- err
			return
		}
		if err := p.SetMTU(64); err != nil {
			result <- err
			return
		}
		_, err = p.DiscoverServices(nil)
		result <- err
	}
	if err := d.Init(nil); err != nil {
		t.Fatal(err)
	}
	d.hci.AcceptSlaveHandler(&linux.PlatData{Conn: conn})
	select {
	case packet := <-conn.writes:
		if !bytes.Equal(packet, []byte{attOpMtuReq, 64, 0}) {
			t.Fatalf("MTU request = %x", packet)
		}
	case <-time.After(time.Second):
		t.Fatal("PeripheralConnected could not submit a synchronous request before returning")
	}
	conn.reads <- []byte{attOpMtuRsp, 64, 0}
	select {
	case packet := <-conn.writes:
		if !bytes.Equal(packet, []byte{attOpReadByGroupReq, 1, 0, 255, 255, 0, 0x28}) {
			t.Fatalf("service discovery request = %x", packet)
		}
	case <-time.After(time.Second):
		t.Fatal("PeripheralConnected could not continue with service discovery")
	}
	conn.reads <- attErrorRsp(attOpReadByGroupReq, 1, attEcodeAttrNotFound)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("PeripheralConnected did not receive its synchronous response")
	}
}

func TestDeviceStateCallbacksOrderedAndReentrant(t *testing.T) {
	var closes atomic.Int32
	wantErr := errors.New("device shutdown failed")
	d := &device{hci: &linux.HCI{}, closeHCI: func() error { closes.Add(1); return wantErr }}
	states := make(chan State, 2)
	errs := make(chan error, 2)
	if err := d.Init(func(_ Device, s State) { states <- s; errs <- d.Stop() }); err != nil {
		t.Fatal(err)
	}
	for _, want := range []State{StatePoweredOn, StatePoweredOff} {
		select {
		case got := <-states:
			if got != want {
				t.Fatalf("state = %v, want %v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("state callback did not finish")
		}
		select {
		case err := <-errs:
			if !errors.Is(err, wantErr) {
				t.Fatalf("callback Stop() = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("state callback deadlocked calling Stop")
		}
	}
	if err := d.Stop(); !errors.Is(err, wantErr) {
		t.Fatalf("Stop() = %v", err)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("device shutdowns = %d, want 1", got)
	}
	if err := d.Init(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Init after Stop = %v", err)
	}
}

func TestDeviceStopContextCancelsOnlyCallerWait(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var closes atomic.Int32
	wantErr := errors.New("shutdown result")
	d := &device{hci: &linux.HCI{}, closeHCI: func() error {
		closes.Add(1)
		close(entered)
		<-release
		return wantErr
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.StopContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("StopContext(canceled) = %v, want canceled wait", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("canceled caller did not start shared shutdown")
	}
	select {
	case <-d.stopDone:
		t.Fatal("canceled wait terminated shared shutdown")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if err := d.Stop(); !errors.Is(err, wantErr) {
		t.Fatalf("Stop() = %v, want final shared result", err)
	}
	if err := d.StopContext(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("completed StopContext = %v, want cached result", err)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("device shutdowns = %d, want 1", got)
	}
	if err := d.Init(nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Init after Stop before Init = %v", err)
	}
}

func TestDeviceStopReleasesATTWithoutWaitingForHandler(t *testing.T) {
	conn := newLifecycleTestConn()
	defer conn.Close()
	handlerCanceled := make(chan struct{})
	d := &device{hci: &linux.HCI{}, closeHCI: func() error {
		// Upper-layer cancellation must not depend on transport shutdown.
		<-handlerCanceled
		return conn.Close()
	}}
	s := NewService(UUID16(0xff00))
	c := s.AddCharacteristic(UUID16(0xff01))
	entered, release, handlerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	c.HandleReadFunc(func(rsp ResponseWriter, req *ReadRequest) {
		defer close(handlerDone)
		stopCancel := context.AfterFunc(req.Context(), func() { close(handlerCanceled) })
		defer stopCancel()
		close(entered)
		<-release // Model application code that does not promptly observe cancellation.
		if req.Context().Err() == nil {
			t.Error("handler context was not canceled")
		}
		_, _ = rsp.Write([]byte("late response"))
	})
	if err := d.AddService(s); err != nil {
		t.Fatal(err)
	}
	if err := d.Init(nil); err != nil {
		t.Fatal(err)
	}
	conn.reads <- []byte{attOpReadReq, byte(c.vh), byte(c.vh >> 8)}
	d.hci.AcceptMasterHandler(&linux.PlatData{Conn: conn})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- d.Stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop waited for a noncooperative handler")
	}
	if got := len(d.requestSlots); got != 1 {
		t.Fatalf("running handler slots = %d, want 1 until it returns", got)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish")
	}
	select {
	case response := <-conn.writes:
		t.Fatalf("late handler wrote response %x after shutdown", response)
	default:
	}
}

func TestCentralCanceledWriteKeepsHandlerDataOwned(t *testing.T) {
	conn := newLifecycleTestConn()
	defer conn.Close()
	s := NewService(UUID16(0xff00))
	c := s.AddCharacteristic(UUID16(0xff01))
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	value := make(chan []byte, 1)
	c.HandleWriteFunc(func(r Request, data []byte) byte {
		close(entered)
		<-release
		if r.Context().Err() == nil {
			t.Error("write handler context was not canceled")
		}
		value <- append([]byte(nil), data...)
		return StatusSuccess
	})
	central := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
	request := []byte{attOpWriteReq, byte(c.vh), byte(c.vh >> 8), 'a', 'b'}
	response := make(chan []byte, 1)
	go func() { response <- central.handleReq(request) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("write handler did not start")
	}
	conn.Close()
	select {
	case <-response:
	case <-time.After(time.Second):
		t.Fatal("canceled write did not return")
	}
	request[3], request[4] = 'x', 'y'
	releaseOnce.Do(func() { close(release) })
	select {
	case got := <-value:
		if !bytes.Equal(got, []byte("ab")) {
			t.Fatalf("late handler value = %q, want original ab", got)
		}
	case <-time.After(time.Second):
		t.Fatal("write handler did not finish")
	}
}

func TestNotifierStopDoesNotWaitForTransportWrite(t *testing.T) {
	n, conn := newNotificationTestNotifier(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	conn.writeResult = func(b []byte) (int, error) { close(entered); <-release; return len(b), nil }
	writeDone := make(chan struct{})
	go func() { defer close(writeDone); _, _ = n.Write([]byte("pending")) }()
	<-entered
	stopped := make(chan struct{})
	go func() { n.stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("notifier stop waited for a slow transport write")
	}
	if !n.Done() {
		t.Fatal("notifier was not stopped")
	}
	if _, err := n.Write([]byte("next")); err == nil {
		t.Fatal("stopped notifier accepted another write")
	}
	releaseOnce.Do(func() { close(release) })
	<-writeDone
}

func TestRequestContextWithoutOptionalCapability(t *testing.T) {
	if ctx := (Request{}).Context(); ctx == nil || ctx.Err() != nil {
		t.Fatalf("Context() = %v, want live background context", ctx)
	}
}

func TestDeviceBoundsRetiredConnectionCallbacks(t *testing.T) {
	connections := []*lifecycleTestConn{newLifecycleTestConn(), newLifecycleTestConn(), newLifecycleTestConn()}
	d := &device{hci: &linux.HCI{}, maxConn: 1, closeHCI: func() error {
		for _, c := range connections {
			c.Close()
		}
		return nil
	}}
	entered, disconnected := make(chan struct{}, 3), make(chan struct{}, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	d.centralConnected = func(Central) { entered <- struct{}{}; <-release }
	d.centralDisconnected = func(Central) { disconnected <- struct{}{} }
	if err := d.Init(nil); err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	for _, c := range connections[:2] {
		d.hci.AcceptMasterHandler(&linux.PlatData{Conn: c})
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("Connected callback did not start")
		}
		c.Close() // The user callback keeps running after its connection ends.
	}
	d.hci.AcceptMasterHandler(&linux.PlatData{Conn: connections[2]})
	select {
	case <-connections[2].ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("device accepted another connection after callback capacity was exhausted")
	}
	select {
	case <-entered:
		t.Fatal("an extra Connected callback started beyond the limit")
	default:
	}
	if err := d.Stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-disconnected:
		t.Fatal("Disconnected overtook a blocked Connected callback")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		select {
		case <-disconnected:
		case <-time.After(time.Second):
			t.Fatal("Disconnected did not follow the released Connected callback")
		}
	}
}

func TestCentralBoundsHandlersThatIgnoreCancellation(t *testing.T) {
	slots := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	finished := make(chan struct{}, 2)
	for i := 0; i < cap(slots); i++ {
		conn := newLifecycleTestConn()
		c := newCentral(nil, nil, conn)
		c.requestSlots = slots
		entered := make(chan struct{})
		result := make(chan bool, 1)
		go func() { result <- c.callHandler(func() { close(entered); <-release; finished <- struct{}{} }) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("handler did not start")
		}
		conn.Close()
		select {
		case completed := <-result:
			if completed {
				t.Fatal("canceled handler was reported complete")
			}
		case <-time.After(time.Second):
			t.Fatal("core still waited for the canceled handler")
		}
	}
	conn := newLifecycleTestConn()
	defer conn.Close()
	c := newCentral(nil, nil, conn)
	c.requestSlots = slots
	if c.callHandler(func() { t.Error("handler ran beyond the global limit") }) {
		t.Fatal("device accepted another handler while old handlers were still running")
	}
	releaseOnce.Do(func() { close(release) })
	for i := 0; i < cap(finished); i++ {
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("released handler did not finish")
		}
	}
}
