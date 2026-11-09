package gatt

import (
	"bytes"
	"testing"
	"testing/synctest"
)

func TestPeripheralIdleResponsesDoNotBlockUpdatesOrCompleteFutureRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		p := newPeripheralForTest(conn)
		startPeripheralForTest(t, p, conn)
		notified := make(chan string, 3)
		p.sub.subscribe(3, func(b []byte, _ error) { notified <- string(b) })
		for _, response := range [][]byte{
			{attOpReadRsp, 's', 't', 'a', 'l', 'e'},
			{attOpWriteRsp},
			{attOpMtuRsp, 64, 0},
			attErrorRsp(attOpReadReq, 3, attEcodeReadNotPerm),
		} {
			conn.reads <- peripheralTestRead{b: response}
		}
		// An unsolicited MTU response cannot raise the receive limit.
		conn.reads <- peripheralTestRead{b: append([]byte{attOpHandleNotify, 3, 0}, make([]byte, 21)...)}
		conn.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 'n'}}
		conn.reads <- peripheralTestRead{b: []byte{attOpHandleInd, 3, 0, 'i'}}
		synctest.Wait()
		if len(notified) != 2 {
			t.Fatalf("idle responses blocked notifications or changed MTU: callbacks=%d", len(notified))
		}
		seen := map[string]bool{<-notified: true, <-notified: true}
		if !seen["n"] || !seen["i"] {
			t.Fatalf("delivered updates = %v", seen)
		}
		if got := peripheralWriteForTest(t, conn); !bytes.Equal(got, []byte{attOpHandleCnf}) {
			t.Fatalf("indication confirmation after idle responses = %x", got)
		}
		result := make(chan peripheralReadResult, 1)
		go func() { b, err := p.ReadCharacteristic(&Characteristic{vh: 4}); result <- peripheralReadResult{b, err} }()
		if got := peripheralWriteForTest(t, conn); !bytes.Equal(got, []byte{attOpReadReq, 4, 0}) {
			t.Fatalf("next read request = %x", got)
		}
		synctest.Wait()
		select {
		case got := <-result:
			t.Fatalf("idle response completed a future read: %q %v", got.value, got.err)
		default:
		}
		conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'f', 'r', 'e', 's', 'h'}}
		if got := peripheralReadResultForTest(t, conn, result); got.err != nil || string(got.value) != "fresh" {
			t.Fatalf("next read result = %q %v", got.value, got.err)
		}
	})
}

func TestPeripheralFastResponseBeforeWriteReturns(t *testing.T) {
	for _, exchangeMTU := range []bool{false, true} {
		name := "read"
		if exchangeMTU {
			name = "mtu"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := newPeripheralTestConn()
				release := make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				conn.writeResult = func(b []byte) (int, error) { <-release; return len(b), nil }
				p := newPeripheralForTest(conn)
				startPeripheralForTest(t, p, conn)
				notified := make(chan int, 1)
				p.sub.subscribe(3, func(b []byte, _ error) { notified <- len(b) })
				result := make(chan peripheralReadResult, 1)
				go func() {
					if exchangeMTU {
						result <- peripheralReadResult{err: p.SetMTU(64)}
						return
					}
					b, err := p.ReadCharacteristic(&Characteristic{vh: 4})
					result <- peripheralReadResult{b, err}
				}()
				peripheralWriteForTest(t, conn)
				response, size := []byte{attOpReadRsp, 'v'}, 1
				if exchangeMTU {
					response, size = []byte{attOpMtuRsp, 64, 0}, 61
				}
				conn.reads <- peripheralTestRead{b: response}
				conn.reads <- peripheralTestRead{b: append([]byte{attOpHandleNotify, 3, 0}, make([]byte, size)...)}
				synctest.Wait()
				if len(conn.readPackets) != 1 || len(notified) != 0 {
					t.Fatal("reader passed a response before its request owner processed it")
				}
				select {
				case got := <-result:
					t.Fatalf("response completed before Write returned: %q %v", got.value, got.err)
				default:
				}
				close(release)
				got := peripheralReadResultForTest(t, conn, result)
				if got.err != nil || !exchangeMTU && string(got.value) != "v" {
					t.Fatalf("fast response result = %q %v", got.value, got.err)
				}
				synctest.Wait()
				select {
				case n := <-notified:
					if n != size {
						t.Fatalf("notification after response = %d bytes, want %d", n, size)
					}
				default:
					t.Fatal("fast response lost its following notification")
				}
			})
		})
	}
}

func TestPeripheralBackToBackDuplicateResponseDoesNotCompleteNextRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		p := newPeripheralForTest(conn)
		startPeripheralForTest(t, p, conn)
		notified := make(chan struct{}, 1)
		p.sub.subscribe(9, func([]byte, error) { notified <- struct{}{} })
		result := make(chan peripheralReadResult, 1)
		go func() { b, err := p.ReadCharacteristic(&Characteristic{vh: 3}); result <- peripheralReadResult{b, err} }()
		peripheralWriteForTest(t, conn)
		conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'a'}}
		conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'x'}}
		if got := peripheralReadResultForTest(t, conn, result); got.err != nil || string(got.value) != "a" {
			t.Fatalf("first read = %q %v", got.value, got.err)
		}
		conn.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 9, 0, 'n'}}
		synctest.Wait()
		if len(notified) != 1 {
			t.Fatal("duplicate response blocked subsequent notifications")
		}
		go func() { b, err := p.ReadCharacteristic(&Characteristic{vh: 4}); result <- peripheralReadResult{b, err} }()
		peripheralWriteForTest(t, conn)
		synctest.Wait()
		select {
		case got := <-result:
			t.Fatalf("duplicate response completed the next read: %q %v", got.value, got.err)
		default:
		}
		conn.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'b'}}
		if got := peripheralReadResultForTest(t, conn, result); got.err != nil || string(got.value) != "b" {
			t.Fatalf("next read = %q %v", got.value, got.err)
		}
	})
}
