package gatt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestServerReceiveMTULimits(t *testing.T) {
	for _, mtu := range []uint16{23, 517} {
		for _, op := range []byte{attOpWriteReq, attOpWriteCmd} {
			limit := min(int(mtu)-3, attMaxValueLen)
			for _, size := range []int{limit, limit + 1} {
				t.Run(fmt.Sprintf("mtu%d/op%02x/value%d", mtu, op, size), func(t *testing.T) {
					s := NewService(UUID16(0xff00))
					ch := s.AddCharacteristic(UUID16(0xff01))
					calls := 0
					ch.HandleWriteFunc(func(Request, []byte) byte { calls++; return StatusSuccess })
					c := newCentral(generateAttributes([]*Service{s}, 1), nil, &notificationTestConn{})
					defer c.Close()
					if !c.handleAndRespond([]byte{attOpMtuReq, byte(mtu), byte(mtu >> 8)}) {
						t.Fatal("MTU response was not sent")
					}
					req := append([]byte{op, byte(ch.vh), byte(ch.vh >> 8)}, bytes.Repeat([]byte{'x'}, size)...)
					got := c.handleReq(req)
					wantCalls := 0
					var want []byte
					if size <= limit {
						wantCalls = 1
						if op == attOpWriteReq {
							want = []byte{attOpWriteRsp}
						}
					} else if op == attOpWriteReq {
						want = attErrorRsp(op, 0, attEcodeInvalidPDU)
						if len(req) <= int(mtu) {
							want = attErrorRsp(op, ch.vh, attEcodeInvalAttrValueLen)
						}
					}
					if calls != wantCalls || !bytes.Equal(got, want) {
						t.Fatalf("calls=%d response=%x, want %d and %x", calls, got, wantCalls, want)
					}
					if got := c.handleReq([]byte{attOpWriteReq, byte(ch.vh), byte(ch.vh >> 8), 'v'}); !bytes.Equal(got, []byte{attOpWriteRsp}) {
						t.Fatalf("write after local rejection = %x", got)
					}
				})
			}
		}
	}
	c := newCentral(nil, nil, &notificationTestConn{})
	defer c.Close()
	if got := c.handleReq(append([]byte{attOpSignedWriteCmd}, make([]byte, 23)...)); got != nil {
		t.Fatalf("oversized unsupported command received a response: %x", got)
	}
}

func TestServerOversizedConfirmationDoesNotReleaseIndication(t *testing.T) {
	n, _ := newNotificationTestNotifier(t)
	defer n.central.Close()
	n.ccc = gattCCCIndicateFlag
	if _, err := n.Write([]byte{'a'}); err != nil {
		t.Fatal(err)
	}
	if got := n.central.handleReq(append([]byte{attOpHandleCnf}, make([]byte, 23)...)); got != nil {
		t.Fatalf("confirmation received an ATT error: %x", got)
	}
	if _, err := n.Write([]byte{'b'}); !errors.Is(err, ErrIndicationPending) {
		t.Fatalf("oversized confirmation released pending indication: %v", err)
	}
	n.central.handleReq([]byte{attOpHandleCnf})
	if _, err := n.Write([]byte{'c'}); err != nil {
		t.Fatalf("valid confirmation did not release indication: %v", err)
	}
}

func TestServerMTUResponsePrecedesNotification(t *testing.T) {
	for _, exchange := range []struct {
		old, next uint16
		size      int
		short     bool
	}{{23, 64, 61, false}, {64, 23, 21, true}} {
		for _, closeDuringWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d-to-%d/close%t", exchange.old, exchange.next, closeDuringWrite), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					n, _ := newNotificationTestNotifier(t)
					c := n.central
					defer c.Close()
					if !c.handleAndRespond([]byte{attOpMtuReq, byte(exchange.old), byte(exchange.old >> 8)}) {
						t.Fatal("initial MTU response failed")
					}
					conn := newPeripheralTestConn()
					c.l2conn = conn
					release := make(chan struct{})
					conn.writeResult = func(b []byte) (int, error) {
						if b[0] == attOpMtuRsp {
							select {
							case <-release:
							case <-conn.ctx.Done():
								return 0, io.ErrClosedPipe
							}
						}
						return len(b), nil
					}
					exchanged := make(chan bool, 1)
					go func() {
						exchanged <- c.handleAndRespond([]byte{attOpMtuReq, byte(exchange.next), byte(exchange.next >> 8)})
					}()
					if got := peripheralWriteForTest(t, conn); !bytes.Equal(got, []byte{attOpMtuRsp, byte(exchange.next), byte(exchange.next >> 8)}) {
						t.Fatalf("MTU response attempt = %x", got)
					}
					if c.MTU() != int(exchange.old) {
						t.Fatal("new MTU was published before the response was accepted")
					}
					notified := make(chan error, 1)
					go func() { _, err := n.Write(bytes.Repeat([]byte{'x'}, exchange.size)); notified <- err }()
					synctest.Wait()
					if len(conn.writes) != 0 {
						t.Fatal("notification overtook the MTU response")
					}
					if closeDuringWrite {
						if err := c.Close(); err != nil {
							t.Fatal(err)
						}
						if <-exchanged || !errors.Is(<-notified, io.ErrClosedPipe) || c.MTU() != int(exchange.old) {
							t.Fatal("closure did not release gate waiters without publishing MTU")
						}
						return
					}
					close(release)
					if !<-exchanged {
						t.Fatal("MTU response failed")
					}
					err := <-notified
					if exchange.short {
						if !errors.Is(err, io.ErrShortWrite) || len(conn.writes) != 0 {
							t.Fatalf("notification after MTU shrink = %v", err)
						}
					} else {
						if err != nil {
							t.Fatalf("notification after MTU response = %v", err)
						}
						if got := peripheralWriteForTest(t, conn); len(got) != exchange.size+3 || got[0] != attOpHandleNotify {
							t.Fatalf("notification at new MTU = %x", got)
						}
					}
					if c.MTU() != int(exchange.next) {
						t.Fatal("successful MTU response did not publish new size")
					}
				})
			})
		}
	}
}

func TestServerFailedMTUResponseKeepsOldSize(t *testing.T) {
	for _, short := range []bool{false, true} {
		conn := &notificationTestConn{writeResult: func(b []byte) (int, error) {
			if short {
				return len(b) - 1, nil
			}
			return 0, io.ErrClosedPipe
		}}
		c := newCentral(nil, nil, conn)
		if c.handleAndRespond([]byte{attOpMtuReq, 64, 0}) || c.MTU() != 23 {
			t.Fatal("failed MTU response changed the effective size")
		}
		c.Close()
	}
}

func TestServerHandlerCanSendNotification(t *testing.T) {
	n, conn := newNotificationTestNotifier(t)
	c := n.central
	defer c.Close()
	ch := n.a.pvt.(*Descriptor).char
	ch.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
		if _, err := n.Write([]byte{'n'}); err != nil {
			t.Errorf("notification in read handler: %v", err)
		}
		_, _ = w.Write([]byte{'r'})
	})
	c.attrs = generateAttributes([]*Service{ch.svc}, 1)
	done := make(chan bool, 1)
	go func() { done <- c.handleAndRespond([]byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}) }()
	select {
	case ok := <-done:
		if !ok || len(conn.packets) != 2 || conn.packets[0][0] != attOpHandleNotify || conn.packets[1][0] != attOpReadRsp {
			t.Fatalf("handler packets = %x", conn.packets)
		}
	case <-time.After(time.Second):
		c.Close()
		<-done
		t.Fatal("business handler held the send gate")
	}
}

func TestServerQueuedRequestUsesNegotiatedMTU(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		service := NewService(UUID16(0xff00))
		ch := service.AddCharacteristic(UUID16(0xff01))
		entered, release := make(chan struct{}), make(chan struct{})
		ch.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
			close(entered)
			<-release
			_, _ = w.Write([]byte{'r'})
		})
		written := make(chan int, 1)
		ch.HandleWriteFunc(func(_ Request, value []byte) byte { written <- len(value); return StatusSuccess })
		c := newCentral(generateAttributes([]*Service{service}, 1), nil, conn)
		done := make(chan struct{})
		go func() { defer close(done); c.loop() }()
		defer func() { c.Close(); <-done }()
		conn.reads <- peripheralTestRead{b: []byte{attOpReadReq, byte(ch.vh), byte(ch.vh >> 8)}}
		<-entered
		conn.reads <- peripheralTestRead{b: []byte{attOpMtuReq, 64, 0}}
		conn.reads <- peripheralTestRead{b: append([]byte{attOpWriteReq, byte(ch.vh), byte(ch.vh >> 8)}, make([]byte, 61)...)}
		for i := 0; i < 3; i++ {
			<-conn.readPackets
		}
		close(release)
		for _, want := range [][]byte{{attOpReadRsp, 'r'}, {attOpMtuRsp, 64, 0}, {attOpWriteRsp}} {
			if got := peripheralWriteForTest(t, conn); !bytes.Equal(got, want) {
				t.Fatalf("queued request response = %x, want %x", got, want)
			}
		}
		if n := <-written; n != 61 {
			t.Fatalf("write at newly negotiated MTU = %d bytes", n)
		}
	})
}

func TestClientOversizedResponseRetiresWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		p := newPeripheralForTest(conn)
		done := startPeripheralForTest(t, p, conn)
		results := make(chan error, 2)
		go func() { _, err := p.ReadCharacteristic(&Characteristic{vh: 3}); results <- err }()
		peripheralWriteForTest(t, conn)
		go func() { _, err := p.ReadDescriptor(&Descriptor{h: 4}); results <- err }()
		synctest.Wait()
		conn.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, make([]byte, 23)...)}
		for i := 0; i < 2; i++ {
			if err := peripheralResultForTest(t, results); !errors.Is(err, ErrInvalidLength) {
				t.Fatalf("oversized response error = %v", err)
			}
		}
		<-done
		if len(conn.writes) != 0 {
			t.Fatal("queued request reached retired bearer")
		}
	})
}

func TestClientDropsOversizedUpdatesAndConfirmsIndications(t *testing.T) {
	for _, mtu := range []uint16{23, 517} {
		for _, op := range []byte{attOpHandleNotify, attOpHandleInd} {
			t.Run(fmt.Sprintf("mtu%d/op%02x", mtu, op), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					conn := newPeripheralTestConn()
					p := newPeripheralForTest(conn)
					p.mtu = mtu
					startPeripheralForTest(t, p, conn)
					notified := make(chan int, 1)
					p.sub.subscribe(3, func(b []byte, _ error) { notified <- len(b) })
					read := make(chan peripheralReadResult, 1)
					go func() { b, err := p.ReadCharacteristic(&Characteristic{vh: 4}); read <- peripheralReadResult{b, err} }()
					peripheralWriteForTest(t, conn)
					limit := min(int(mtu)-3, attMaxValueLen)
					conn.reads <- peripheralTestRead{b: append([]byte{op, 3, 0}, make([]byte, limit+1)...)}
					if op == attOpHandleInd {
						if got := peripheralWriteForTest(t, conn); !bytes.Equal(got, []byte{attOpHandleCnf}) {
							t.Fatalf("discarded indication confirmation = %x", got)
						}
					}
					conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'r'}}
					got := peripheralReadResultForTest(t, conn, read)
					synctest.Wait()
					if got.err != nil || string(got.value) != "r" || len(notified) != 0 || p.closedError() != nil {
						t.Fatalf("oversized update affected pending request: %q %v", got.value, got.err)
					}
					conn.reads <- peripheralTestRead{b: append([]byte{attOpHandleNotify, 3, 0}, make([]byte, limit)...)}
					select {
					case n := <-notified:
						if n != limit {
							t.Fatalf("notification length = %d", n)
						}
					case <-time.After(time.Second):
						t.Fatal("legal notification was dropped")
					}
				})
			})
		}
	}
}

func TestClientReaderWaitsForResponseProcessing(t *testing.T) {
	for _, update := range []struct {
		old, next uint16
		size      int
		want      bool
	}{{23, 64, 61, true}, {64, 23, 21, false}, {64, 23, 20, true}} {
		t.Run(fmt.Sprintf("%d-to-%d/value%d", update.old, update.next, update.size), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := newPeripheralTestConn()
				p := newPeripheralForTest(conn)
				p.responsePending = true // This test supplies the response owner.
				rspc, processed := make(chan []byte), make(chan uint16)
				done := make(chan struct{})
				go func() { defer close(done); p.readLoop(rspc, processed, update.old) }()
				defer func() { conn.Close(); <-done }()
				notified := make(chan int, 1)
				p.sub.subscribe(3, func(b []byte, _ error) { notified <- len(b) })
				conn.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, byte(update.next), byte(update.next >> 8)}}
				<-rspc
				<-conn.readPackets
				conn.reads <- peripheralTestRead{b: append([]byte{attOpHandleNotify, 3, 0}, make([]byte, update.size)...)}
				synctest.Wait()
				if len(conn.readPackets) != 0 || len(notified) != 0 {
					t.Fatal("reader passed a response before its owner committed MTU")
				}
				processed <- update.next
				conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'r'}}
				<-rspc
				processed <- update.next
				synctest.Wait()
				if update.want {
					if n := <-notified; n != update.size {
						t.Fatalf("post-exchange notification length = %d", n)
					}
				} else if len(notified) != 0 {
					t.Fatal("post-exchange notification used the old MTU")
				}
			})
		})
	}
}

func TestClientReaderResponseWaitsAreCancelable(t *testing.T) {
	for _, waitForProcessing := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			conn := newPeripheralTestConn()
			p := newPeripheralForTest(conn)
			p.responsePending = true // Exercise an active response handoff.
			rspc, processed := make(chan []byte), make(chan uint16)
			done := make(chan struct{})
			go func() { defer close(done); p.readLoop(rspc, processed, 23) }()
			conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'r'}}
			<-conn.readPackets
			if waitForProcessing {
				<-rspc
			}
			synctest.Wait()
			p.stop(context.Canceled)
			<-done
			conn.Close()
		})
	}
}

func TestClientMTUResponseFollowedImmediatelyByNotification(t *testing.T) {
	for _, exchange := range []struct {
		old, next uint16
		size      int
		want      bool
	}{{23, 64, 61, true}, {64, 23, 21, false}, {64, 23, 20, true}} {
		t.Run(fmt.Sprintf("%d-to-%d/value%d", exchange.old, exchange.next, exchange.size), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := newPeripheralTestConn()
				p := newPeripheralForTest(conn)
				p.mtu = exchange.old
				startPeripheralForTest(t, p, conn)
				notified := make(chan int, 1)
				p.sub.subscribe(3, func(b []byte, _ error) { notified <- len(b) })
				exchanged := make(chan error, 1)
				go func() { exchanged <- p.SetMTU(exchange.next) }()
				peripheralWriteForTest(t, conn)
				conn.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, byte(exchange.next), byte(exchange.next >> 8)}}
				conn.reads <- peripheralTestRead{b: append([]byte{attOpHandleNotify, 3, 0}, make([]byte, exchange.size)...)}
				if err := peripheralResultForTest(t, exchanged); err != nil {
					t.Fatal(err)
				}
				read := make(chan peripheralReadResult, 1)
				go func() { b, err := p.ReadCharacteristic(&Characteristic{vh: 4}); read <- peripheralReadResult{b, err} }()
				peripheralWriteForTest(t, conn)
				conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'r'}}
				if got := peripheralReadResultForTest(t, conn, read); got.err != nil {
					t.Fatal(got.err)
				}
				synctest.Wait()
				if exchange.want {
					if n := <-notified; n != exchange.size {
						t.Fatalf("notification after MTU response = %d bytes", n)
					}
				} else if len(notified) != 0 {
					t.Fatal("notification after MTU shrink used the old receive size")
				}
			})
		})
	}
}

func TestClientResponseOwnerAckWaitCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		p := newPeripheralForTest(conn)
		rspc, processed := make(chan []byte), make(chan uint16)
		done := make(chan struct{})
		go func() { defer close(done); p.requestLoop(rspc, processed) }()
		defer conn.Close()
		result := make(chan error, 1)
		go func() { _, err := p.ReadCharacteristic(&Characteristic{vh: 3}); result <- err }()
		peripheralWriteForTest(t, conn)
		rspc <- []byte{attOpReadRsp, 'r'}
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("request owner skipped response processing acknowledgement: %v", err)
		default:
		}
		p.stop(context.Canceled)
		if err := peripheralResultForTest(t, result); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel while waiting for reader acknowledgement = %v", err)
		}
		<-done
	})
}
