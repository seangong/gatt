package gatt

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type cccdTestSubscription struct {
	index    int
	notifier Notifier
}

func newCCCDTestCentral(t *testing.T, count int, conn io.ReadWriteCloser) (*central, []*Characteristic, <-chan cccdTestSubscription) {
	t.Helper()
	s := NewService(UUID16(0xff00))
	chars := make([]*Characteristic, count)
	subscribed := make(chan cccdTestSubscription, count*4)
	for i := range chars {
		chars[i] = s.AddCharacteristic(UUID16(uint16(0xff01 + i)))
		chars[i].HandleNotifyFunc(func(_ Request, n Notifier) {
			subscribed <- cccdTestSubscription{index: i, notifier: n}
		})
	}
	c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
	t.Cleanup(func() { _ = c.Close() })
	return c, chars, subscribed
}

func cccdTestWrite(c *central, ch *Characteristic, value uint16) []byte {
	h := ch.Descriptor().Handle()
	return c.handleReq([]byte{attOpWriteReq, byte(h), byte(h >> 8), byte(value), byte(value >> 8)})
}

func cccdTestEnable(t *testing.T, c *central, ch *Characteristic, value uint16, subscribed <-chan cccdTestSubscription) Notifier {
	t.Helper()
	if got := cccdTestWrite(c, ch, value); !bytes.Equal(got, []byte{attOpWriteRsp}) {
		t.Fatalf("write CCCD %04x = %x, want write response", value, got)
	}
	select {
	case sub := <-subscribed:
		return sub.notifier
	case <-time.After(time.Second):
		t.Fatal("notification handler did not start")
		return nil
	}
}

func cccdTestRead(t *testing.T, c *central, ch *Characteristic, value uint16) {
	t.Helper()
	h := ch.Descriptor().Handle()
	for _, tt := range []struct {
		name    string
		request []byte
		want    []byte
	}{
		{"read", []byte{attOpReadReq, byte(h), byte(h >> 8)}, []byte{attOpReadRsp, byte(value), byte(value >> 8)}},
		{"read by type", []byte{attOpReadByTypeReq, byte(h), byte(h >> 8), byte(h), byte(h >> 8), 0x02, 0x29}, []byte{attOpReadByTypeRsp, 4, byte(h), byte(h >> 8), byte(value), byte(value >> 8)}},
		{"blob start", []byte{attOpReadBlobReq, byte(h), byte(h >> 8), 0, 0}, []byte{attOpReadBlobRsp, byte(value), byte(value >> 8)}},
		{"blob suffix", []byte{attOpReadBlobReq, byte(h), byte(h >> 8), 1, 0}, []byte{attOpReadBlobRsp, byte(value >> 8)}},
		{"blob end", []byte{attOpReadBlobReq, byte(h), byte(h >> 8), 2, 0}, []byte{attOpReadBlobRsp}},
		{"blob invalid offset", []byte{attOpReadBlobReq, byte(h), byte(h >> 8), 3, 0}, attErrorRsp(attOpReadBlobReq, h, attEcodeInvalidOffset)},
	} {
		if got := c.handleReq(tt.request); !bytes.Equal(got, tt.want) {
			t.Errorf("%s CCCD = %x, want %x", tt.name, got, tt.want)
		}
	}
}

func cccdTestPackets(conn *notificationTestConn) [][]byte {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	packets := make([][]byte, len(conn.packets))
	for i := range packets {
		packets[i] = append([]byte(nil), conn.packets[i]...)
	}
	return packets
}

func TestCCCDValuesAreConnectionLocal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c1, chars, subscribed := newCCCDTestCentral(t, 1, &notificationTestConn{})
		c2 := newCentral(c1.attrs, nil, &notificationTestConn{})
		defer c2.Close()
		ch := chars[0]
		cccdTestRead(t, c1, ch, 0)
		cccdTestRead(t, c2, ch, 0)
		n1 := cccdTestEnable(t, c1, ch, 1, subscribed)
		cccdTestRead(t, c1, ch, 1)
		cccdTestRead(t, c2, ch, 0)
		n2 := cccdTestEnable(t, c2, ch, 2, subscribed)
		cccdTestRead(t, c1, ch, 1)
		cccdTestRead(t, c2, ch, 2)
		if got := cccdTestWrite(c1, ch, 0); !bytes.Equal(got, []byte{attOpWriteRsp}) {
			t.Fatalf("disable CCCD = %x", got)
		}
		cccdTestRead(t, c1, ch, 0)
		cccdTestRead(t, c2, ch, 2)
		if !n1.Done() || n2.Done() {
			t.Errorf("disabled notifier Done=%t, other connection Done=%t", n1.Done(), n2.Done())
		}
	})
}

func TestCCCDModeChangesReuseHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &notificationTestConn{}
		c, chars, subscribed := newCCCDTestCentral(t, 1, conn)
		ch := chars[0]
		n := cccdTestEnable(t, c, ch, 1, subscribed)
		for _, value := range []uint16{1, 2, 2, 3, 1} {
			if got := cccdTestWrite(c, ch, value); !bytes.Equal(got, []byte{attOpWriteRsp}) {
				t.Fatalf("write CCCD %d = %x", value, got)
			}
			synctest.Wait()
			select {
			case <-subscribed:
				t.Fatal("changing enabled CCCD restarted notification handler")
			default:
			}
			cccdTestRead(t, c, ch, value)
			if count, err := n.Write([]byte{byte(value)}); count != 1 || err != nil {
				t.Fatalf("write after CCCD %d = (%d, %v)", value, count, err)
			}
			packets := cccdTestPackets(conn)
			op := byte(attOpHandleNotify)
			if value == 2 {
				op = attOpHandleInd
			}
			if got := packets[len(packets)-1][0]; got != op {
				t.Errorf("CCCD %d opcode = %02x, want %02x", value, got, op)
			}
			if value == 2 {
				if got := c.handleReq([]byte{attOpHandleCnf}); got != nil {
					t.Errorf("confirmation response = %x, want none", got)
				}
			}
		}
	})
}

func TestCCCDRejectedWritePreservesConfiguration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, chars, subscribed := newCCCDTestCentral(t, 1, &notificationTestConn{})
		ch := chars[0]
		cccdTestEnable(t, c, ch, 1, subscribed)
		for _, value := range []uint16{4, 0x8000, 0xffff} {
			if got := cccdTestWrite(c, ch, value); len(got) != 5 || got[0] != attOpError || got[1] != attOpWriteReq {
				t.Errorf("reserved CCCD %04x response = %x, want ATT error", value, got)
			}
			cccdTestRead(t, c, ch, 1)
		}
		synctest.Wait()
		select {
		case <-subscribed:
			t.Fatal("rejected CCCD restarted notification handler")
		default:
		}
	})
}

func TestCCCDRejectsUnsupportedMode(t *testing.T) {
	for _, property := range []Property{CharNotify, CharIndicate} {
		t.Run(property.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, chars, _ := newCCCDTestCentral(t, 1, &notificationTestConn{})
				ch := chars[0]
				ch.props &^= property
				value := uint16(1)
				if property == CharIndicate {
					value = 2
				}
				if got := cccdTestWrite(c, ch, value); len(got) != 5 || got[0] != attOpError {
					t.Fatalf("unsupported CCCD %d response = %x, want ATT error", value, got)
				}
				cccdTestRead(t, c, ch, 0)
			})
		})
	}
}

func TestCCCDCapacityFailureRollsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, chars, subscribed := newCCCDTestCentral(t, 1, &notificationTestConn{})
		ch := chars[0]
		c.callbackSlots = make(chan struct{}, 1)
		c.callbackSlots <- struct{}{}
		got := cccdTestWrite(c, ch, 2)
		want := attErrorRsp(attOpWriteReq, ch.Descriptor().Handle(), attEcodeInsuffResources)
		if !bytes.Equal(got, want) {
			t.Fatalf("subscribe at callback capacity = %x, want %x", got, want)
		}
		cccdTestRead(t, c, ch, 0)
		<-c.callbackSlots
		cccdTestEnable(t, c, ch, 2, subscribed)
		cccdTestRead(t, c, ch, 2)
	})
}

func TestIndicationRequiresValidConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &notificationTestConn{}
		c, chars, subscribed := newCCCDTestCentral(t, 1, conn)
		n := cccdTestEnable(t, c, chars[0], 2, subscribed)
		if got := c.handleReq([]byte{attOpHandleCnf}); got != nil {
			t.Errorf("unsolicited confirmation response = %x, want none", got)
		}
		if count, err := n.Write([]byte{'a'}); count != 1 || err != nil {
			t.Fatalf("first indication = (%d, %v)", count, err)
		}
		packets := cccdTestPackets(conn)
		h := chars[0].VHandle()
		if len(packets) != 1 || !bytes.Equal(packets[0], []byte{attOpHandleInd, byte(h), byte(h >> 8), 'a'}) {
			t.Fatalf("indication packet = %x", packets)
		}
		if count, err := n.Write([]byte{'b'}); count != 0 || !errors.Is(err, ErrIndicationPending) {
			t.Errorf("write while awaiting confirmation = (%d, %v), want (0, error)", count, err)
		}
		if got := c.handleReq([]byte{attOpHandleCnf, 0}); got != nil {
			t.Errorf("malformed confirmation response = %x, want none", got)
		}
		if count, err := n.Write([]byte{'c'}); count != 0 || !errors.Is(err, ErrIndicationPending) {
			t.Errorf("malformed confirmation released pending indication: (%d, %v)", count, err)
		}
		if got := c.handleReq([]byte{attOpHandleCnf}); got != nil {
			t.Errorf("confirmation response = %x, want none", got)
		}
		if count, err := n.Write([]byte{'d'}); count != 1 || err != nil {
			t.Errorf("write after confirmation = (%d, %v)", count, err)
		}
		if got := len(cccdTestPackets(conn)); got != 2 {
			t.Errorf("transport writes = %d, want 2", got)
		}
	})
}

func TestIndicationPendingIsSharedByCharacteristics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &notificationTestConn{}
		c, chars, subscribed := newCCCDTestCentral(t, 2, conn)
		notifiers := []Notifier{
			cccdTestEnable(t, c, chars[0], 2, subscribed),
			cccdTestEnable(t, c, chars[1], 2, subscribed),
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		for _, n := range notifiers {
			go func() {
				<-start
				_, err := n.Write([]byte{'a'})
				results <- err
			}()
		}
		close(start)
		errs := []error{<-results, <-results}
		if (errs[0] == nil) == (errs[1] == nil) {
			t.Fatalf("concurrent indication errors = %v, want one success and one busy", errs)
		}
		for _, err := range errs {
			if err != nil && !errors.Is(err, ErrIndicationPending) {
				t.Errorf("concurrent indication error = %v, want ErrIndicationPending", err)
			}
		}
		if got := len(cccdTestPackets(conn)); got != 1 {
			t.Fatalf("concurrent transport indications = %d, want 1", got)
		}
		c.handleReq([]byte{attOpHandleCnf})
		if _, err := notifiers[0].Write([]byte{'b'}); err != nil {
			t.Errorf("indication after confirmation = %v", err)
		}
	})
}

func TestIndicationUnsubscribeKeepsOutstandingConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, chars, subscribed := newCCCDTestCentral(t, 1, &notificationTestConn{})
		ch := chars[0]
		old := cccdTestEnable(t, c, ch, 2, subscribed)
		if _, err := old.Write([]byte{'a'}); err != nil {
			t.Fatal(err)
		}
		if got := cccdTestWrite(c, ch, 0); !bytes.Equal(got, []byte{attOpWriteRsp}) {
			t.Fatalf("unsubscribe = %x", got)
		}
		current := cccdTestEnable(t, c, ch, 2, subscribed)
		if !old.Done() || current.Done() {
			t.Fatalf("old Done=%t, current Done=%t", old.Done(), current.Done())
		}
		if count, err := current.Write([]byte{'b'}); count != 0 || !errors.Is(err, ErrIndicationPending) {
			t.Fatalf("resubscribe discarded outstanding indication: (%d, %v)", count, err)
		}
		c.handleReq([]byte{attOpHandleCnf})
		if _, err := current.Write([]byte{'c'}); err != nil {
			t.Errorf("write after old indication confirmed = %v", err)
		}
	})
}

func TestIndicationModeSwitchKeepsOutstandingConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &notificationTestConn{}
		c, chars, subscribed := newCCCDTestCentral(t, 1, conn)
		ch := chars[0]
		n := cccdTestEnable(t, c, ch, 2, subscribed)
		if _, err := n.Write([]byte{'a'}); err != nil {
			t.Fatal(err)
		}
		if got := cccdTestWrite(c, ch, 3); !bytes.Equal(got, []byte{attOpWriteRsp}) {
			t.Fatalf("enable notification and indication = %x", got)
		}
		if count, err := n.Write([]byte{'b'}); count != 1 || err != nil {
			t.Fatalf("notification while indication pending = (%d, %v)", count, err)
		}
		packets := cccdTestPackets(conn)
		if len(packets) != 2 || packets[1][0] != attOpHandleNotify {
			t.Fatalf("both-enabled packet = %x, want notification", packets)
		}
		if got := cccdTestWrite(c, ch, 2); !bytes.Equal(got, []byte{attOpWriteRsp}) {
			t.Fatalf("switch back to indication = %x", got)
		}
		if count, err := n.Write([]byte{'c'}); count != 0 || !errors.Is(err, ErrIndicationPending) {
			t.Fatalf("mode switch discarded outstanding indication: (%d, %v)", count, err)
		}
		c.handleReq([]byte{attOpHandleCnf})
		if _, err := n.Write([]byte{'d'}); err != nil {
			t.Errorf("indication after original confirmation = %v", err)
		}
	})
}

func TestIndicationConfirmedTimerDoesNotCloseLaterTransaction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, chars, subscribed := newCCCDTestCentral(t, 1, &notificationTestConn{})
		n := cccdTestEnable(t, c, chars[0], 2, subscribed)
		if _, err := n.Write([]byte{'a'}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Second)
		c.handleReq([]byte{attOpHandleCnf})
		if _, err := n.Write([]byte{'b'}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20*time.Second + time.Millisecond)
		synctest.Wait()
		if err := c.Context().Err(); err != nil {
			t.Fatalf("old indication timer closed later transaction: %v", err)
		}
		c.handleReq([]byte{attOpHandleCnf})
		time.Sleep(31 * time.Second)
		synctest.Wait()
		if err := c.Context().Err(); err != nil {
			t.Fatalf("confirmed indication timed out: %v", err)
		}
	})
}

func TestIndicationTimeoutClosesConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newLifecycleTestConn()
		c, chars, subscribed := newCCCDTestCentral(t, 1, conn)
		n := cccdTestEnable(t, c, chars[0], 2, subscribed)
		if _, err := n.Write([]byte{'a'}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30*time.Second + time.Millisecond)
		synctest.Wait()
		if c.Context().Err() == nil || conn.Context().Err() == nil || !n.Done() {
			t.Fatalf("timeout did not close central/transport/notifier: %v, %v, %t", c.Context().Err(), conn.Context().Err(), n.Done())
		}
		if count, err := n.Write([]byte{'b'}); count != 0 || err == nil {
			t.Errorf("write after timeout = (%d, %v)", count, err)
		}
	})
}

func TestIndicationTransportFailureClosesConnection(t *testing.T) {
	transportErr := errors.New("indication write failed")
	for _, tt := range []struct {
		name  string
		write func([]byte) (int, error)
		want  error
	}{
		{"before writing", func([]byte) (int, error) { return 0, transportErr }, transportErr},
		{"after partial writing", func([]byte) (int, error) { return 4, transportErr }, transportErr},
		{"after full writing", func(b []byte) (int, error) { return len(b), transportErr }, transportErr},
		{"short write", func(b []byte) (int, error) { return len(b) - 1, nil }, io.ErrShortWrite},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := &notificationTestConn{writeResult: tt.write}
				c, chars, subscribed := newCCCDTestCentral(t, 1, conn)
				n := cccdTestEnable(t, c, chars[0], 2, subscribed)
				if _, err := n.Write([]byte{'a', 'b'}); !errors.Is(err, tt.want) {
					t.Errorf("write error = %v, want %v", err, tt.want)
				}
				synctest.Wait()
				if c.Context().Err() == nil || !n.Done() {
					t.Fatalf("uncertain indication left bearer usable: context=%v, Done=%t", c.Context().Err(), n.Done())
				}
				if count, err := n.Write([]byte{'c'}); count != 0 || err == nil {
					t.Errorf("write after transport failure = (%d, %v)", count, err)
				}
				if got := len(cccdTestPackets(conn)); got != 1 {
					t.Errorf("transport writes after failure = %d, want 1", got)
				}
			})
		})
	}
}

func TestIndicationTimeoutReleasesBlockedTransportWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newLifecycleTestConn()
		conn.writes = make(chan []byte)
		c, chars, subscribed := newCCCDTestCentral(t, 1, conn)
		n := cccdTestEnable(t, c, chars[0], 2, subscribed)
		result := make(chan error, 1)
		go func() { _, err := n.Write([]byte{'a'}); result <- err }()
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("blocked transport write returned before cancellation: %v", err)
		default:
		}
		time.Sleep(30*time.Second + time.Millisecond)
		synctest.Wait()
		if err := <-result; !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("timed-out transport write = %v, want closed pipe", err)
		}
		if c.Context().Err() == nil || !n.Done() {
			t.Errorf("timed-out write did not stop bearer: context=%v, Done=%t", c.Context().Err(), n.Done())
		}
	})
}

func TestIndicationCloseStopsNotifierAndTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, chars, subscribed := newCCCDTestCentral(t, 1, &notificationTestConn{})
		n := cccdTestEnable(t, c, chars[0], 2, subscribed)
		if _, err := n.Write([]byte{'a'}); err != nil {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if count, err := n.Write([]byte{'b'}); count != 0 || err == nil || !n.Done() {
			t.Errorf("write after close = (%d, %v), Done=%t", count, err, n.Done())
		}
		time.Sleep(31 * time.Second)
		synctest.Wait()
	})
}

func TestCentralReceivesConfirmationWhileReadHandlerBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newLifecycleTestConn()
		c, chars, subscribed := newCCCDTestCentral(t, 2, conn)
		n := cccdTestEnable(t, c, chars[0], 2, subscribed)
		if _, err := n.Write([]byte{'a'}); err != nil {
			t.Fatal(err)
		}
		<-conn.writes
		entered, release := make(chan struct{}), make(chan struct{})
		chars[1].HandleReadFunc(func(rsp ResponseWriter, req *ReadRequest) {
			close(entered)
			select {
			case <-release:
				_, _ = rsp.Write([]byte{'r'})
			case <-req.Context().Done():
			}
		})
		// The callback is configured before the ATT loop starts; regenerate the
		// shared table so its read permission is visible on the wire.
		c.attrs = generateAttributes([]*Service{chars[0].Service()}, 1)
		loopDone := make(chan struct{})
		go func() { c.loop(); close(loopDone) }()
		h := chars[1].VHandle()
		conn.reads <- []byte{attOpReadReq, byte(h), byte(h >> 8)}
		<-entered
		conn.reads <- []byte{attOpHandleCnf}
		synctest.Wait()
		if count, err := n.Write([]byte{'b'}); count != 1 || err != nil {
			t.Fatalf("confirmation blocked behind application handler: (%d, %v)", count, err)
		}
		if packet := <-conn.writes; packet[0] != attOpHandleInd {
			t.Fatalf("next indication = %x", packet)
		}
		conn.reads <- []byte{attOpHandleCnf}
		close(release)
		if got := <-conn.writes; !bytes.Equal(got, []byte{attOpReadRsp, 'r'}) {
			t.Errorf("read response after release = %x", got)
		}
		_ = c.Close()
		<-loopDone
	})
}

func TestCentralRequestQueueOverflowCancelsBlockedHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newLifecycleTestConn()
		conn.reads = make(chan []byte, 64)
		c, chars, _ := newCCCDTestCentral(t, 1, conn)
		ch := chars[0]
		entered, canceled := make(chan struct{}), make(chan struct{})
		var writes atomic.Int32
		ch.HandleReadFunc(func(_ ResponseWriter, req *ReadRequest) {
			close(entered)
			<-req.Context().Done()
			close(canceled)
		})
		ch.HandleWriteFunc(func(Request, []byte) byte { writes.Add(1); return StatusSuccess })
		c.attrs = generateAttributes([]*Service{ch.Service()}, 1)
		loopDone := make(chan struct{})
		go func() { c.loop(); close(loopDone) }()
		h := ch.VHandle()
		conn.reads <- []byte{attOpReadReq, byte(h), byte(h >> 8)}
		<-entered
		for range 32 {
			conn.reads <- []byte{attOpWriteCmd, byte(h), byte(h >> 8), 'x'}
		}
		synctest.Wait()
		if c.Context().Err() == nil || conn.Context().Err() == nil {
			t.Fatal("full ATT request queue did not close the bearer")
		}
		<-canceled
		<-loopDone
		if got := writes.Load(); got != 0 {
			t.Errorf("queued writes executed after shutdown: %d", got)
		}
	})
}

func TestCCCDReadbackConcurrentWithModeChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, chars, subscribed := newCCCDTestCentral(t, 1, &notificationTestConn{})
		ch := chars[0]
		cccdTestEnable(t, c, ch, 1, subscribed)
		h := ch.Descriptor().Handle()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := range 100 {
				if got := cccdTestWrite(c, ch, uint16(i%3+1)); !bytes.Equal(got, []byte{attOpWriteRsp}) {
					t.Errorf("concurrent mode change = %x", got)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for range 100 {
				got := c.handleReq([]byte{attOpReadReq, byte(h), byte(h >> 8)})
				if len(got) != 3 || got[0] != attOpReadRsp || got[1] < 1 || got[1] > 3 || got[2] != 0 {
					t.Errorf("concurrent CCCD read = %x", got)
					return
				}
			}
		}()
		wg.Wait()
	})
}
