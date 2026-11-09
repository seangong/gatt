package gatt

import (
	"bytes"
	"errors"
	"github.com/seangong/gatt/v2/linux"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestPeripheralNotificationCallbacksKeepWireOrder(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	c := newPeripheralTestConn()
	d := &device{maxConn: 1}
	d.initLifecycleLocked()
	p := newPeripheralForTest(c)
	p.callbackSlots = d.callbackSlots
	observed := make(chan byte, 2)
	p.sub.subscribe(3, func(b []byte, _ error) { observed <- b[0] })
	c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
	c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 2}}
	startPeripheralForTest(t, p, c)
	next := func() byte {
		select {
		case b := <-observed:
			return b
		case <-time.After(time.Second):
			t.Fatal("missing callback")
			return 0
		}
	}
	first, second := next(), next()
	if first != 1 || second != 2 {
		t.Fatalf("wire notifications 1,2 delivered as %d,%d", first, second)
	}
}

func TestPeripheralNotificationCanReadWhileIndicationsAreConfirmed(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	result := make(chan peripheralReadResult, 1)
	second := make(chan byte, 1)
	p.sub.subscribe(3, func(b []byte, _ error) {
		if b[0] == 1 {
			value, err := p.ReadCharacteristic(&Characteristic{vh: 5})
			result <- peripheralReadResult{value: value, err: err}
		} else {
			second <- b[0]
		}
	})
	c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
	if raw := peripheralWriteForTest(t, c); !bytes.Equal(raw, []byte{attOpReadReq, 5, 0}) {
		t.Fatalf("callback read=%x", raw)
	}
	c.reads <- peripheralTestRead{b: []byte{attOpHandleInd, 3, 0, 2}}
	if raw := peripheralWriteForTest(t, c); !bytes.Equal(raw, []byte{attOpHandleCnf}) {
		t.Fatalf("indication confirmation=%x", raw)
	}
	select {
	case <-second:
		t.Fatal("notification overtook the callback waiting for its read")
	default:
	}
	c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'v'}}
	select {
	case got := <-result:
		if got.err != nil || string(got.value) != "v" {
			t.Fatalf("callback read=%q,%v", got.value, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("notification callback could not complete a synchronous read")
	}
	select {
	case value := <-second:
		if value != 2 {
			t.Fatalf("second callback=%d", value)
		}
	case <-time.After(time.Second):
		t.Fatal("queued indication was not delivered")
	}
}

func TestPeripheralConnectedCanWaitForNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		d := &device{hci: &linux.HCI{}, closeHCI: c.Close}
		defer d.Stop()
		result, notified := make(chan error, 1), make(chan struct{}, 1)
		d.peripheralConnected = func(p Peripheral, err error) {
			if err == nil {
				err = p.SetNotifyValue(&Characteristic{vh: 3, cccd: &Descriptor{h: 4}}, func(*Characteristic, []byte, error) { notified <- struct{}{} })
			}
			if err == nil {
				select {
				case <-notified:
				case <-time.After(time.Second):
					err = errors.New("Connected waited for a notification callback that could not run")
				}
			}
			result <- err
		}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		d.hci.AcceptSlaveHandler(&linux.PlatData{Conn: c})
		if raw := peripheralWriteForTest(t, c); !bytes.Equal(raw, []byte{attOpWriteReq, 4, 0, 1, 0}) {
			t.Fatalf("subscription=%x", raw)
		}
		c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
		c.reads <- peripheralTestRead{b: []byte{attOpWriteRsp}}
		if err := peripheralResultForTest(t, result); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPeripheralNotificationCanCloseAndWaitForDisconnected(t *testing.T) {
	for _, stopDevice := range []bool{false, true} {
		name := "connection"
		if stopDevice {
			name = "device"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newPeripheralTestConn()
				d := &device{hci: &linux.HCI{}, closeHCI: c.Close}
				defer d.Stop()
				connected, ended := make(chan *peripheral, 1), make(chan struct{}, 1)
				d.peripheralConnected = func(p Peripheral, _ error) { connected <- p.(*peripheral) }
				d.peripheralDisconnected = func(Peripheral, error) { ended <- struct{}{} }
				if err := d.Init(nil); err != nil {
					t.Fatal(err)
				}
				d.hci.AcceptSlaveHandler(&linux.PlatData{Conn: c})
				p := <-connected
				result, release := make(chan error, 1), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				p.sub.subscribe(3, func([]byte, error) {
					var err error
					if stopDevice {
						err = d.Stop()
					} else {
						err = c.Close()
					}
					if err == nil {
						select {
						case <-ended:
						case <-time.After(time.Second):
							err = errors.New("Disconnected waited for its running notification")
						}
					}
					result <- err
					<-release
				})
				c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
				if err := peripheralResultForTest(t, result); err != nil {
					t.Fatal(err)
				}
				if len(d.callbackSlots) != 1 {
					t.Fatalf("running notification retained %d global tokens, want 1", len(d.callbackSlots))
				}
				once.Do(func() { close(release) })
				synctest.Wait()
				if len(d.callbackSlots) != 0 {
					t.Fatal("finished notification retained its global token")
				}
			})
		})
	}
}

func TestPeripheralNotificationsDoNotBlockOtherConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{maxConn: 2}
		d.initLifecycleLocked()
		a, b := newPeripheralTestConn(), newPeripheralTestConn()
		pa, pb := newPeripheralForTest(a), newPeripheralForTest(b)
		pa.callbackSlots, pb.callbackSlots = d.callbackSlots, d.callbackSlots
		startPeripheralForTest(t, pa, a)
		startPeripheralForTest(t, pb, b)
		entered, release, second := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
		var once sync.Once
		defer once.Do(func() { close(release) })
		pa.sub.subscribe(3, func([]byte, error) { close(entered); <-release })
		pb.sub.subscribe(3, func([]byte, error) { second <- struct{}{} })
		a.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
		<-entered
		b.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
		select {
		case <-second:
		case <-time.After(time.Second):
			t.Fatal("one connection blocked another's notifications")
		}
		once.Do(func() { close(release) })
	})
}

func TestPeripheralNotificationQueueKeepsGlobalBound(t *testing.T) {
	for _, connections := range []int{1, 2} {
		name := "one connection"
		if connections == 2 {
			name = "two connections"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := &device{maxConn: connections}
				d.initLifecycleLocked()
				c := newPeripheralTestConn()
				p := newPeripheralForTest(c)
				p.callbackSlots = d.callbackSlots
				done := startPeripheralForTest(t, p, c)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				var calls atomic.Int32
				p.sub.subscribe(3, func([]byte, error) { calls.Add(1); close(entered); <-release })
				c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
				<-entered
				<-c.readPackets
				for i := 1; i < cap(d.callbackSlots); i++ {
					c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, byte(i)}}
					<-c.readPackets
					synctest.Wait()
				}
				if len(d.callbackSlots) != cap(d.callbackSlots) {
					t.Fatalf("queue holds %d global tokens, want %d", len(d.callbackSlots), cap(d.callbackSlots))
				}
				// A single connection may continue using the entire configured device
				// allowance. Only the pre-existing global bound rejects the next value.
				select {
				case <-p.quitc:
					t.Fatal("queue introduced a smaller per-connection limit")
				default:
				}
				c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 99}}
				<-done
				if !errors.Is(p.closedError(), errPeripheralNotificationCapacity) {
					t.Fatalf("overflow error=%v", p.closedError())
				}
				if len(d.callbackSlots) != 1 {
					t.Fatalf("overflow retained %d global tokens, want the running callback only", len(d.callbackSlots))
				}
				once.Do(func() { close(release) })
				synctest.Wait()
				if calls.Load() != 1 || len(d.callbackSlots) != 0 {
					t.Fatalf("overflow ran %d callbacks and retained %d tokens", calls.Load(), len(d.callbackSlots))
				}
			})
		})
	}
}

func TestPeripheralCloseDiscardsQueuedNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		p.callbackSlots = make(chan struct{}, 16)
		done := startPeripheralForTest(t, p, c)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		var calls atomic.Int32
		p.sub.subscribe(3, func([]byte, error) { calls.Add(1); close(entered); <-release })
		c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
		<-entered
		<-c.readPackets
		for i := 0; i < 3; i++ {
			c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 2}}
			<-c.readPackets
			synctest.Wait()
		}
		c.Close()
		<-done
		if len(p.callbackSlots) != 1 {
			t.Fatalf("Close retained %d tokens, want the running callback only", len(p.callbackSlots))
		}
		once.Do(func() { close(release) })
		synctest.Wait()
		if calls.Load() != 1 || len(p.callbackSlots) != 0 {
			t.Fatalf("Close ran %d callbacks and retained %d tokens", calls.Load(), len(p.callbackSlots))
		}
	})
}

type notificationBlockedIOConn struct {
	*peripheralTestConn
	readCalls    atomic.Int32
	readBlocked  chan struct{}
	writeBlocked chan struct{}
	release      chan struct{}
}

func (c *notificationBlockedIOConn) Read(b []byte) (int, error) {
	if c.readCalls.Add(1) > 2 {
		close(c.readBlocked)
		<-c.release
		return 0, io.EOF
	}
	return c.peripheralTestConn.Read(b)
}

func (c *notificationBlockedIOConn) Write([]byte) (int, error) {
	close(c.writeBlocked)
	<-c.release
	return 0, io.ErrClosedPipe
}

func TestPeripheralTransportCancellationDiscardsNotificationsDuringBlockedIO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &notificationBlockedIOConn{
			peripheralTestConn: newPeripheralTestConn(), readBlocked: make(chan struct{}),
			writeBlocked: make(chan struct{}), release: make(chan struct{}),
		}
		p := newPeripheralForTest(c)
		p.callbackSlots = make(chan struct{}, 16)
		done := make(chan struct{})
		go func() { defer close(done); p.loop() }()
		entered, release := make(chan struct{}), make(chan struct{})
		var once, ioOnce sync.Once
		defer once.Do(func() { close(release) })
		defer ioOnce.Do(func() { close(c.release) })
		var calls atomic.Int32
		p.sub.subscribe(3, func([]byte, error) { calls.Add(1); close(entered); <-release })
		c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 1}}
		<-entered
		c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 2}}
		<-c.readBlocked
		result := make(chan error, 1)
		go func() { _, err := p.ReadCharacteristic(&Characteristic{vh: 5}); result <- err }()
		<-c.writeBlocked
		c.cancel()
		synctest.Wait()
		if len(p.callbackSlots) != 1 {
			t.Fatalf("transport cancellation retained %d notification tokens, want the running callback only", len(p.callbackSlots))
		}
		if err := <-result; !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("canceled read = %v", err)
		}
		ioOnce.Do(func() { close(c.release) })
		<-done
		once.Do(func() { close(release) })
		synctest.Wait()
		if calls.Load() != 1 || len(p.callbackSlots) != 0 {
			t.Fatalf("transport cancellation ran %d callbacks and retained %d tokens", calls.Load(), len(p.callbackSlots))
		}
	})
}
