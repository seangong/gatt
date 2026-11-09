package gatt

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestServerRequestTimeoutRetiresBearer(t *testing.T) {
	for _, op := range []byte{attOpReadReq, attOpWriteReq} {
		t.Run(fmt.Sprintf("op%02x", op), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := newPeripheralTestConn()
				s := NewService(UUID16(0xff00))
				ch := s.AddCharacteristic(UUID16(0xff01))
				entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var requestContext context.Context
				ch.HandleReadFunc(func(w ResponseWriter, r *ReadRequest) {
					requestContext = r.Context()
					close(entered)
					<-release
					_, _ = w.Write([]byte("late"))
					close(returned)
				})
				ch.HandleWriteFunc(func(r Request, _ []byte) byte {
					requestContext = r.Context()
					close(entered)
					<-release
					close(returned)
					return StatusSuccess
				})
				c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
				ended := make(chan struct{})
				go func() { defer close(ended); c.loop() }()
				defer func() { c.Close(); <-ended }()
				conn.reads <- peripheralTestRead{b: []byte{op, byte(ch.vh), byte(ch.vh >> 8)}}
				<-entered
				time.Sleep(31 * time.Second)
				if c.Context().Err() == nil || requestContext.Err() == nil {
					t.Error("server request timeout did not cancel the bearer and handler context")
				}
				if len(c.requestSlots) != 1 {
					t.Error("timed-out handler released its resource slot before returning")
				}
				close(release)
				<-returned
				synctest.Wait()
				if len(c.requestSlots) != 0 {
					t.Error("returned handler retained its slot")
				}
				if len(conn.writes) != 0 {
					t.Errorf("server sent late response: %x", <-conn.writes)
				}
			})
		})
	}
}

func TestServerCompletedRequestCancelsTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		s := NewService(UUID16(0xff00))
		ch := s.AddCharacteristic(UUID16(0xff01))
		ch.SetValue([]byte("ready"))
		c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
		defer c.Close()
		if !c.handleAndRespond([]byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}) {
			t.Fatal("read failed")
		}
		if got := peripheralWriteForTest(t, conn); !bytes.Equal(got, []byte{attOpReadRsp, 'r', 'e', 'a', 'd', 'y'}) {
			t.Fatalf("response=%x", got)
		}
		time.Sleep(31 * time.Second)
		if c.Context().Err() != nil {
			t.Fatal("completed request timed out")
		}
	})
}

func TestServerWriteCommandDoesNotStartResponseTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		s := NewService(UUID16(0xff00))
		ch := s.AddCharacteristic(UUID16(0xff01))
		entered, release := make(chan struct{}), make(chan struct{})
		ch.HandleWriteFunc(func(Request, []byte) byte { close(entered); <-release; return StatusSuccess })
		c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
		done := make(chan bool, 1)
		go func() { done <- c.handleAndRespond([]byte{attOpWriteCmd, byte(ch.vh), byte(ch.vh >> 8), 'v'}) }()
		<-entered
		time.Sleep(31 * time.Second)
		if c.Context().Err() != nil {
			t.Error("Write Command started a response timeout")
		}
		close(release)
		if !<-done || len(conn.writes) != 0 {
			t.Error("Write Command produced a response")
		}
		c.Close()
	})
}

func TestServerRequestTimeoutIncludesQueueWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		s := NewService(UUID16(0xff00))
		ch := s.AddCharacteristic(UUID16(0xff01))
		entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
		ch.HandleWriteFunc(func(Request, []byte) byte { close(entered); <-release; close(returned); return StatusSuccess })
		readCalls := 0
		ch.HandleReadFunc(func(ResponseWriter, *ReadRequest) { readCalls++ })
		c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
		ended := make(chan struct{})
		go func() { defer close(ended); c.loop() }()
		defer func() { c.Close(); <-ended }()
		conn.reads <- peripheralTestRead{b: []byte{attOpWriteCmd, byte(ch.vh), byte(ch.vh >> 8), 'v'}}
		<-entered
		time.Sleep(10 * time.Second)
		conn.reads <- peripheralTestRead{b: []byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}}
		// Ensure the reader admitted the second PDU before advancing the clock.
		<-conn.readPackets
		<-conn.readPackets
		synctest.Wait()
		time.Sleep(21 * time.Second)
		if c.Context().Err() != nil {
			t.Error("queued request inherited the earlier command's reception time")
		}
		time.Sleep(10 * time.Second)
		if c.Context().Err() == nil {
			t.Error("queued request's deadline waited for the command handler to finish")
		}
		close(release)
		<-returned
		synctest.Wait()
		if readCalls != 0 || len(conn.writes) != 0 {
			t.Error("expired queued request reached a handler or produced a response")
		}
	})
}

func TestServerRequestTimeoutIncludesSendGateWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		s := NewService(UUID16(0xff00))
		ch := s.AddCharacteristic(UUID16(0xff01))
		ch.SetValue([]byte("ready"))
		c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
		defer c.Close()
		c.sendGate <- struct{}{}
		done := make(chan bool, 1)
		go func() { done <- c.handleAndRespond([]byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}) }()
		synctest.Wait()
		time.Sleep(31 * time.Second)
		if <-done || c.Context().Err() == nil || len(conn.writes) != 0 {
			t.Error("request send-gate wait outlived its transaction deadline")
		}
		<-c.sendGate
	})
}

type serverTimeoutWriteConn struct {
	*peripheralTestConn
	entered chan struct{}
}

func (c *serverTimeoutWriteConn) Write([]byte) (int, error) {
	close(c.entered)
	<-c.ctx.Done()
	return 0, io.ErrClosedPipe
}

func TestServerRequestTimeoutInterruptsResponseWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &serverTimeoutWriteConn{peripheralTestConn: newPeripheralTestConn(), entered: make(chan struct{})}
		s := NewService(UUID16(0xff00))
		ch := s.AddCharacteristic(UUID16(0xff01))
		ch.SetValue([]byte("ready"))
		c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
		defer c.Close()
		done := make(chan bool, 1)
		go func() { done <- c.handleAndRespond([]byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}) }()
		<-conn.entered
		time.Sleep(31 * time.Second)
		if <-done || c.Context().Err() == nil || conn.Context().Err() == nil {
			t.Error("response write blocked transaction expiry")
		}
	})
}

func TestServerRequestTimeoutStillHandlesConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		s := NewService(UUID16(0xff00))
		ch := s.AddCharacteristic(UUID16(0xff01))
		entered, release := make(chan struct{}), make(chan struct{})
		ch.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) { close(entered); <-release; _, _ = w.Write([]byte("read")) })
		ch.HandleNotifyFunc(func(Request, Notifier) {})
		c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
		n := newNotifier(c, &attr{valueHandle: ch.vh}, 20)
		n.ccc = gattCCCIndicateFlag
		c.notifiers[ch.Descriptor().h] = n
		if _, err := n.Write([]byte("ind")); err != nil {
			t.Fatal(err)
		}
		peripheralWriteForTest(t, conn)
		ended := make(chan struct{})
		go func() { defer close(ended); c.loop() }()
		defer func() { c.Close(); <-ended }()
		conn.reads <- peripheralTestRead{b: []byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}}
		<-entered
		conn.reads <- peripheralTestRead{b: []byte{attOpHandleCnf}}
		<-conn.readPackets
		<-conn.readPackets
		synctest.Wait()
		c.notifiersmu.Lock()
		pending := c.indication != nil
		c.notifiersmu.Unlock()
		if pending {
			t.Error("handler blocked the indication confirmation")
		}
		time.Sleep(31 * time.Second)
		if !n.Done() {
			t.Error("transaction timeout left notifier active")
		}
		close(release)
		synctest.Wait()
	})
}

func TestServerCompletedRequestDoesNotExpireLaterRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		s := NewService(UUID16(0xff00))
		ch := s.AddCharacteristic(UUID16(0xff01))
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		ch.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
			entered <- struct{}{}
			<-release
			_, _ = w.Write([]byte("ready"))
		})
		c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
		defer c.Close()
		read := func() <-chan bool {
			done := make(chan bool, 1)
			go func() { done <- c.handleAndRespond([]byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}) }()
			return done
		}
		first := read()
		<-entered
		time.Sleep(29 * time.Second)
		close(release)
		if !<-first {
			t.Fatal("read completed before its deadline was rejected")
		}
		peripheralWriteForTest(t, conn)
		// The first handler has returned before its next invocation reads release.
		release = make(chan struct{})
		second := read()
		<-entered
		time.Sleep(2 * time.Second)
		if c.Context().Err() != nil {
			t.Error("a completed request's deadline canceled the later request")
		}
		close(release)
		if !<-second {
			t.Error("later request was rejected before its own deadline")
		}
		peripheralWriteForTest(t, conn)
	})
}
