package gatt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type peripheralTestRead struct {
	b   []byte
	err error
}

type peripheralTestConn struct {
	ctx         context.Context
	cancel      context.CancelFunc
	reads       chan peripheralTestRead
	readPackets chan []byte
	writes      chan []byte
	writeResult func([]byte) (int, error)
	closeOnce   sync.Once
}

func newPeripheralTestConn() *peripheralTestConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &peripheralTestConn{
		ctx: ctx, cancel: cancel,
		reads: make(chan peripheralTestRead, 8), readPackets: make(chan []byte, 8),
		writes: make(chan []byte, 8),
	}
}

func (c *peripheralTestConn) Context() context.Context { return c.ctx }

func (c *peripheralTestConn) Read(b []byte) (int, error) {
	select {
	case <-c.ctx.Done():
		return 0, io.EOF
	case r := <-c.reads:
		if r.err != nil {
			return 0, r.err
		}
		c.readPackets <- append([]byte(nil), r.b...)
		return copy(b, r.b), nil
	}
}

func (c *peripheralTestConn) Write(b []byte) (int, error) {
	select {
	case <-c.ctx.Done():
		return 0, io.ErrClosedPipe
	default:
	}
	c.writes <- append([]byte(nil), b...)
	if c.writeResult != nil {
		return c.writeResult(b)
	}
	return len(b), nil
}

func (c *peripheralTestConn) Close() error {
	c.closeOnce.Do(c.cancel)
	return nil
}

func (c *peripheralTestConn) Abort(error) { c.Close() }

type peripheralTestConnWithoutContext struct{ conn *peripheralTestConn }

func (c peripheralTestConnWithoutContext) Read(b []byte) (int, error)  { return c.conn.Read(b) }
func (c peripheralTestConnWithoutContext) Write(b []byte) (int, error) { return c.conn.Write(b) }
func (c peripheralTestConnWithoutContext) Close() error                { return c.conn.Close() }

func newPeripheralForTest(conn io.ReadWriteCloser) *peripheral {
	return &peripheral{
		l2c: conn, mtu: 23, sub: newSubscriber(),
		reqc: make(chan message), quitc: make(chan struct{}),
	}
}

func startPeripheralForTest(t *testing.T, p *peripheral, c *peripheralTestConn) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); p.loop() }()
	t.Cleanup(func() {
		c.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("peripheral loop or request pump did not exit")
		}
	})
	return done
}

func peripheralResultForTest(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("peripheral operation remained blocked")
		return nil
	}
}

func peripheralWriteForTest(t *testing.T, c *peripheralTestConn) []byte {
	t.Helper()
	select {
	case b := <-c.writes:
		return b
	case <-time.After(time.Second):
		t.Fatal("peripheral request was not written")
		return nil
	}
}

func TestPeripheralClosedOperationsReturnErrors(t *testing.T) {
	service := NewService(UUID16(0xff00))
	service.h, service.endh = 1, 5
	characteristic := service.AddCharacteristic(UUID16(0xff01))
	characteristic.h, characteristic.vh, characteristic.endh = 2, 3, 5
	descriptor := characteristic.AddDescriptor(UUID16(0x2902))
	descriptor.h = 4
	characteristic.cccd = descriptor
	for _, tt := range []struct {
		name string
		call func(*peripheral) error
	}{
		{"read characteristic", func(p *peripheral) error { _, err := p.ReadCharacteristic(characteristic); return err }},
		{"read long characteristic", func(p *peripheral) error { _, err := p.ReadLongCharacteristic(characteristic); return err }},
		{"write request", func(p *peripheral) error { return p.WriteCharacteristic(characteristic, []byte{1}, false) }},
		{"write command", func(p *peripheral) error { return p.WriteCharacteristic(characteristic, []byte{1}, true) }},
		{"read descriptor", func(p *peripheral) error { _, err := p.ReadDescriptor(descriptor); return err }},
		{"write descriptor", func(p *peripheral) error { return p.WriteDescriptor(descriptor, []byte{1}) }},
		{"notify", func(p *peripheral) error {
			return p.SetNotifyValue(characteristic, func(*Characteristic, []byte, error) {})
		}},
		{"indicate", func(p *peripheral) error {
			return p.SetIndicateValue(characteristic, func(*Characteristic, []byte, error) {})
		}},
		{"MTU", func(p *peripheral) error { return p.SetMTU(64) }},
		{"services", func(p *peripheral) error { _, err := p.DiscoverServices(nil); return err }},
		{"characteristics", func(p *peripheral) error { _, err := p.DiscoverCharacteristics(nil, service); return err }},
		{"descriptors", func(p *peripheral) error { _, err := p.DiscoverDescriptors(nil, characteristic); return err }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPeripheralTestConn()
			c.Close()
			p := newPeripheralForTest(c)
			result := make(chan error, 1)
			go func() { result <- tt.call(p) }()
			if err := peripheralResultForTest(t, result); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("operation after cancellation = %v, want closed pipe", err)
			}
			if len(c.writes) != 0 {
				t.Fatal("closed operation reached the transport")
			}
		})
	}
}

func TestPeripheralCancellationReleasesQueuedAndInFlightRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		done := make(chan struct{})
		go func() { defer close(done); p.loop() }()
		defer func() { c.Close(); <-done }()
		results := make(chan error, 2)
		go func() { _, err := p.sendReq(attOpReadReq, []byte{attOpReadReq, 1, 0}); results <- err }()
		peripheralWriteForTest(t, c)
		go func() { _, err := p.sendReq(attOpReadReq, []byte{attOpReadReq, 2, 0}); results <- err }()
		// The first caller is waiting for a response; the second is blocked
		// submitting to reqc while the request pump is serving the first.
		synctest.Wait()
		c.Close()
		for i := 0; i < 2; i++ {
			if err := peripheralResultForTest(t, results); err == nil {
				t.Fatal("canceled request returned success")
			}
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("request pump remained blocked waiting for a response")
		}
		if len(c.writes) != 0 {
			t.Fatal("queued request was written after cancellation")
		}
	})
}

func TestPeripheralReadEOFReleasesRequestsWithoutConnectionContext(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(peripheralTestConnWithoutContext{c})
	startPeripheralForTest(t, p, c)
	result := make(chan error, 1)
	go func() { _, err := p.sendReq(attOpReadReq, []byte{attOpReadReq, 1, 0}); result <- err }()
	peripheralWriteForTest(t, c)
	c.reads <- peripheralTestRead{err: io.EOF}
	if err := peripheralResultForTest(t, result); !errors.Is(err, io.EOF) {
		t.Fatalf("request after EOF = %v, want EOF", err)
	}
	if _, err := p.sendReq(attOpReadReq, []byte{attOpReadReq, 2, 0}); !errors.Is(err, io.EOF) {
		t.Fatalf("request after loop exit = %v, want EOF", err)
	}
}

func TestPeripheralWriteFailureIsReturned(t *testing.T) {
	writeErr := errors.New("ATT transport failed")
	for _, noRsp := range []bool{false, true} {
		name := "request"
		if noRsp {
			name = "command"
		}
		t.Run(name, func(t *testing.T) {
			c := newPeripheralTestConn()
			c.writeResult = func([]byte) (int, error) { return 0, writeErr }
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			result := make(chan error, 1)
			go func() { result <- p.WriteCharacteristic(&Characteristic{vh: 3}, []byte{1}, noRsp) }()
			if err := peripheralResultForTest(t, result); !errors.Is(err, writeErr) {
				t.Fatalf("write failure = %v, want original transport error", err)
			}
		})
	}
	t.Run("short write", func(t *testing.T) {
		c := newPeripheralTestConn()
		c.writeResult = func(b []byte) (int, error) { return len(b) - 1, nil }
		p := newPeripheralForTest(c)
		startPeripheralForTest(t, p, c)
		result := make(chan error, 1)
		go func() { result <- p.SetMTU(64) }()
		if err := peripheralResultForTest(t, result); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short write = %v, want short write", err)
		}
	})
}

func TestPeripheralSerialRequestsKeepNotificationDelivery(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	notified := make(chan []byte, 1)
	p.sub.subscribe(3, func(b []byte, _ error) { notified <- b })
	first := make(chan peripheralResult, 1)
	go func() {
		b, err := p.ReadCharacteristic(&Characteristic{vh: 1})
		first <- peripheralResult{b: b, err: err}
	}()
	if got := peripheralWriteForTest(t, c); !bytes.Equal(got, []byte{attOpReadReq, 1, 0}) {
		t.Fatalf("first request = %x", got)
	}
	second := make(chan peripheralResult, 1)
	go func() { b, err := p.ReadDescriptor(&Descriptor{h: 2}); second <- peripheralResult{b: b, err: err} }()
	c.reads <- peripheralTestRead{b: []byte{attOpHandleInd, 3, 0, 'n'}}
	if got := peripheralWriteForTest(t, c); !bytes.Equal(got, []byte{attOpHandleCnf}) {
		t.Fatalf("indication confirmation = %x", got)
	}
	select {
	case b := <-notified:
		if !bytes.Equal(b, []byte{'n'}) {
			t.Fatalf("notification = %q", b)
		}
	case <-time.After(time.Second):
		t.Fatal("notification callback was not called during an in-flight request")
	}
	if len(c.writes) != 0 {
		t.Fatal("second request was written before the first response")
	}
	c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'a'}}
	select {
	case got := <-first:
		if got.err != nil || !bytes.Equal(got.b, []byte{'a'}) {
			t.Fatalf("first response = %q, %v", got.b, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("first request did not complete")
	}
	if got := peripheralWriteForTest(t, c); !bytes.Equal(got, []byte{attOpReadReq, 2, 0}) {
		t.Fatalf("second request = %x", got)
	}
	c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'b'}}
	select {
	case got := <-second:
		if got.err != nil || !bytes.Equal(got.b, []byte{'b'}) {
			t.Fatalf("second response = %q, %v", got.b, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("second request did not complete")
	}
}

func TestPeripheralATTErrorDoesNotCloseConnection(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	result := make(chan error, 1)
	go func() { result <- p.WriteDescriptor(&Descriptor{h: 4}, []byte{1}) }()
	peripheralWriteForTest(t, c)
	c.reads <- peripheralTestRead{b: attErrorRsp(attOpWriteReq, 4, attEcodeWriteNotPerm)}
	if err := peripheralResultForTest(t, result); !errors.Is(err, attEcodeWriteNotPerm) {
		t.Fatalf("ATT rejection = %v, want write not permitted", err)
	}
	go func() { result <- p.WriteCharacteristic(&Characteristic{vh: 3}, []byte{1}, true) }()
	if got := peripheralWriteForTest(t, c); got[0] != attOpWriteCmd {
		t.Fatalf("write command = %x", got)
	}
	if err := peripheralResultForTest(t, result); err != nil {
		t.Fatal(err)
	}
}

func TestPeripheralCancellationInterruptsResponseDelivery(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	done := startPeripheralForTest(t, p, c)
	c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'x'}}
	select {
	case <-c.readPackets:
	case <-time.After(time.Second):
		t.Fatal("reader did not receive unsolicited response")
	}
	c.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked delivering a response without a pending request")
	}
}

func TestPeripheralReadLongCancellationBetweenRequests(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	result := make(chan error, 1)
	go func() { _, err := p.ReadLongCharacteristic(&Characteristic{vh: 3}); result <- err }()
	peripheralWriteForTest(t, c)
	c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, bytes.Repeat([]byte{'x'}, 22)...)}
	if got := peripheralWriteForTest(t, c); got[0] != attOpReadBlobReq {
		t.Fatalf("continuation request = %x", got)
	}
	c.Close()
	if err := peripheralResultForTest(t, result); err == nil {
		t.Fatal("canceled long read returned success")
	}
}

func TestPeripheralInvalidResponseStopsRequestPump(t *testing.T) {
	for _, response := range [][]byte{
		{attOpMtuRsp, 64},
		{attOpError, attOpMtuReq, 1},
	} {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		startPeripheralForTest(t, p, c)
		result := make(chan error, 1)
		go func() { result <- p.SetMTU(64) }()
		peripheralWriteForTest(t, c)
		c.reads <- peripheralTestRead{b: response}
		if err := peripheralResultForTest(t, result); !errors.Is(err, ErrInvalidLength) {
			t.Fatalf("SetMTU() for response %x = %v, want invalid length", response, err)
		}
	}
}

func TestPeripheralCallbackCapacityFailureClosesConnection(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	done := startPeripheralForTest(t, p, c)
	p.callout = func(func()) bool { return false }
	p.sub.subscribe(3, func([]byte, error) { t.Error("rejected callback was called") })
	c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 'n'}}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback capacity failure did not stop the peripheral")
	}
}

func TestPeripheralConcurrentSubscriptionUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		done := make(chan struct{})
		go func() { defer close(done); p.loop() }()
		defer func() { c.Close(); <-done }()
		characteristic := &Characteristic{vh: 3, cccd: &Descriptor{h: 4}}
		called := make(chan string, 2)
		first := make(chan error, 1)
		go func() {
			first <- p.SetNotifyValue(characteristic, func(*Characteristic, []byte, error) { called <- "first" })
		}()
		peripheralWriteForTest(t, c)
		second := make(chan error, 1)
		go func() {
			second <- p.SetNotifyValue(characteristic, func(*Characteristic, []byte, error) { called <- "second" })
		}()
		synctest.Wait()
		c.reads <- peripheralTestRead{b: attErrorRsp(attOpWriteReq, 4, attEcodeWriteNotPerm)}
		if err := peripheralResultForTest(t, first); !errors.Is(err, attEcodeWriteNotPerm) {
			t.Fatalf("first subscription = %v, want write not permitted", err)
		}
		peripheralWriteForTest(t, c)
		c.reads <- peripheralTestRead{b: []byte{attOpWriteRsp}}
		if err := peripheralResultForTest(t, second); err != nil {
			t.Fatalf("second subscription = %v, want nil", err)
		}
		c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 'n'}}
		select {
		case name := <-called:
			if name != "second" {
				t.Fatalf("notification called %s subscription, want second", name)
			}
		case <-time.After(time.Second):
			t.Fatal("failed subscription rollback removed the later successful callback")
		}
	})
}

func TestPeripheralSubscriptionCancellationReleasesQueuedUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		done := make(chan struct{})
		go func() { defer close(done); p.loop() }()
		defer func() { c.Close(); <-done }()
		characteristic := &Characteristic{vh: 3, cccd: &Descriptor{h: 4}}
		results := make(chan error, 2)
		go func() { results <- p.SetNotifyValue(characteristic, func(*Characteristic, []byte, error) {}) }()
		peripheralWriteForTest(t, c)
		go func() { results <- p.SetIndicateValue(characteristic, func(*Characteristic, []byte, error) {}) }()
		synctest.Wait()
		c.Close()
		for i := 0; i < 2; i++ {
			if err := peripheralResultForTest(t, results); err == nil {
				t.Fatal("canceled subscription update returned success")
			}
		}
		if len(c.writes) != 0 {
			t.Fatal("queued subscription update reached transport after cancellation")
		}
	})
}
