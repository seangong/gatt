package gatt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seangong/gatt/v2/linux"
)

type peripheralReadResult struct {
	value []byte
	err   error
}

func peripheralReadResultForTest(t *testing.T, c *peripheralTestConn, result <-chan peripheralReadResult) peripheralReadResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case packet := <-c.writes:
		t.Fatalf("unexpected continuation request: %x", packet)
	case <-time.After(time.Second):
		t.Fatal("read did not finish")
	}
	return peripheralReadResult{}
}

func TestDeviceClientReadLongUsesDefaultMTU(t *testing.T) {
	c := newPeripheralTestConn()
	d := &device{hci: &linux.HCI{}, closeHCI: c.Close}
	defer d.Stop()
	result := make(chan peripheralReadResult, 1)
	d.peripheralConnected = func(p Peripheral, err error) {
		if err != nil {
			result <- peripheralReadResult{err: err}
			return
		}
		value, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
		result <- peripheralReadResult{value: value, err: err}
	}
	if err := d.Init(nil); err != nil {
		t.Fatal(err)
	}
	d.hci.AcceptSlaveHandler(&linux.PlatData{Conn: c})
	if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpReadReq, 3, 0}) {
		t.Fatalf("initial request = %x", packet)
	}
	c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, []byte("ready")...)}
	got := peripheralReadResultForTest(t, c, result)
	if string(got.value) != "ready" || got.err != nil {
		t.Fatalf("default-MTU read = %q, %v", got.value, got.err)
	}
}

func TestPeripheralReadLongFirstBlobAttributeNotLong(t *testing.T) {
	for _, tt := range []struct {
		name        string
		errorHandle uint16
		code        attEcode
		wantSuccess bool
		laterBlob   bool
	}{
		{"exactly one full value", 3, attEcodeAttrNotLong, true, false},
		{"different handle", 4, attEcodeAttrNotLong, false, false},
		{"read not permitted", 3, attEcodeReadNotPerm, false, false},
		{"invalid offset", 3, attEcodeInvalidOffset, false, false},
		{"later blob", 3, attEcodeAttrNotLong, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			value := bytes.Repeat([]byte{'x'}, 22)
			result := make(chan peripheralReadResult, 1)
			go func() {
				b, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
				result <- peripheralReadResult{value: b, err: err}
			}()
			peripheralWriteForTest(t, c)
			c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, value...)}
			if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpReadBlobReq, 3, 0, 22, 0}) {
				t.Fatalf("first blob request = %x", packet)
			}
			if tt.laterBlob {
				c.reads <- peripheralTestRead{b: append([]byte{attOpReadBlobRsp}, value...)}
				if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpReadBlobReq, 3, 0, 44, 0}) {
					t.Fatalf("later blob request = %x", packet)
				}
			}
			c.reads <- peripheralTestRead{b: attErrorRsp(attOpReadBlobReq, tt.errorHandle, tt.code)}
			got := peripheralReadResultForTest(t, c, result)
			if tt.wantSuccess {
				if !bytes.Equal(got.value, value) || got.err != nil {
					t.Fatalf("complete short value = %x, %v", got.value, got.err)
				}
			} else if got.value != nil || !errors.Is(got.err, tt.code) {
				t.Fatalf("blob failure = %x, %v; want nil, %v", got.value, got.err, tt.code)
			}
		})
	}
}

func TestPeripheralMTUAppliedBeforeResultConsumption(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	// Leave both caller results unread, so caller scheduling cannot apply MTU.
	// The peer has nevertheless completed each exchange before the next request.
	for _, mtu := range []byte{64, 32} {
		request := message{op: attOpMtuReq, b: []byte{attOpMtuReq, mtu, 0}, rspc: make(chan peripheralResult, 1)}
		select {
		case p.reqc <- request:
		case <-time.After(time.Second):
			t.Fatal("MTU request was not accepted")
		}
		if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, request.b) {
			t.Fatalf("MTU request = %x", packet)
		}
		c.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, mtu, 0}}
	}
	result := make(chan peripheralReadResult, 1)
	go func() {
		value, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
		result <- peripheralReadResult{value: value, err: err}
	}()
	peripheralWriteForTest(t, c)
	value := bytes.Repeat([]byte{'x'}, 30) // Short at the peer's latest MTU, 32.
	c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, value...)}
	got := peripheralReadResultForTest(t, c, result)
	if !bytes.Equal(got.value, value) || got.err != nil {
		t.Fatalf("read after queued exchanges = %x, %v", got.value, got.err)
	}
}

func TestPeripheralSetMTUPreservesNegotiationAndFailure(t *testing.T) {
	for _, tt := range []struct {
		name   string
		client byte
		server byte
	}{
		{"server smaller", 128, 64},
		{"client smaller", 64, 128},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			mtuResult := make(chan error, 1)
			go func() { mtuResult <- p.SetMTU(uint16(tt.client)) }()
			if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpMtuReq, tt.client, 0}) {
				t.Fatalf("MTU request = %x", packet)
			}
			c.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, tt.server, 0}}
			if err := peripheralResultForTest(t, mtuResult); err != nil {
				t.Fatal(err)
			}
			go func() { mtuResult <- p.SetMTU(96) }()
			peripheralWriteForTest(t, c)
			c.reads <- peripheralTestRead{b: attErrorRsp(attOpMtuReq, 0, attEcodeReqNotSupp)}
			if err := peripheralResultForTest(t, mtuResult); !errors.Is(err, attEcodeReqNotSupp) {
				t.Fatalf("failed exchange = %v", err)
			}
			result := make(chan peripheralReadResult, 1)
			go func() {
				value, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
				result <- peripheralReadResult{value: value, err: err}
			}()
			peripheralWriteForTest(t, c)
			value := bytes.Repeat([]byte{'x'}, 40)
			c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, value...)}
			got := peripheralReadResultForTest(t, c, result)
			if !bytes.Equal(got.value, value) || got.err != nil {
				t.Fatalf("read after failed exchange = %x, %v", got.value, got.err)
			}
		})
	}
}

func TestPeripheralReadLongUsesTransactionMTU(t *testing.T) {
	for _, tt := range []struct {
		name        string
		blob        []byte
		wantSuccess bool
	}{
		{"continuation at new MTU", append([]byte{attOpReadBlobRsp}, []byte("tail")...), true},
		{"attribute not long at new MTU", attErrorRsp(attOpReadBlobReq, 3, attEcodeAttrNotLong), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPeripheralTestConn()
			defer c.Close()
			// Script request results directly to delay a response until after MTU
			// has changed, independently of goroutine scheduling in requestLoop.
			p := newPeripheralForTest(c)
			p.mtu = 64
			result := make(chan peripheralReadResult, 1)
			go func() {
				b, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
				result <- peripheralReadResult{value: b, err: err}
			}()
			var first message
			select {
			case first = <-p.reqc:
			case <-time.After(time.Second):
				t.Fatal("initial read was not submitted")
			}
			prefix := bytes.Repeat([]byte{'x'}, 22)
			first.rspc <- peripheralResult{b: append([]byte{attOpReadRsp}, prefix...), mtu: 23}
			var blob message
			select {
			case blob = <-p.reqc:
			case got := <-result:
				t.Fatalf("old-MTU response was considered complete: %x, %v", got.value, got.err)
			case <-time.After(time.Second):
				t.Fatal("continuation was not submitted")
			}
			if !bytes.Equal(blob.b, []byte{attOpReadBlobReq, 3, 0, 22, 0}) {
				t.Fatalf("continuation request = %x", blob.b)
			}
			response := peripheralResult{b: tt.blob, mtu: 64}
			if tt.blob[0] == attOpError {
				response.err = fmt.Errorf("scripted blob response: %w", attEcodeAttrNotLong)
			}
			blob.rspc <- response
			got := peripheralReadResultForTest(t, c, result)
			if tt.wantSuccess {
				want := append(prefix, []byte("tail")...)
				if !bytes.Equal(got.value, want) || got.err != nil {
					t.Fatalf("read across MTU change = %x, %v", got.value, got.err)
				}
			} else if got.value != nil || !errors.Is(got.err, attEcodeAttrNotLong) {
				t.Fatalf("uncertain completeness after MTU change = %x, %v", got.value, got.err)
			}
		})
	}
}

func TestPeripheralResultRetainsMTUWhenCallerIsDelayed(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	read := message{op: attOpReadReq, b: []byte{attOpReadReq, 3, 0}, rspc: make(chan peripheralResult, 1)}
	select {
	case p.reqc <- read:
	case <-time.After(time.Second):
		t.Fatal("read was not accepted")
	}
	peripheralWriteForTest(t, c)
	c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'x'}}
	mtuResult := make(chan error, 1)
	go func() { mtuResult <- p.SetMTU(64) }()
	peripheralWriteForTest(t, c)
	c.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, 64, 0}}
	if err := peripheralResultForTest(t, mtuResult); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-read.rspc:
		if got.mtu != 23 || !bytes.Equal(got.b, []byte{attOpReadRsp, 'x'}) || got.err != nil {
			t.Fatalf("delayed pre-exchange result = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("pre-exchange read result was lost")
	}
}

func TestPeripheralReadLongBoundaries(t *testing.T) {
	for _, length := range []int{0, 21, 22, 23, 44, 512, 600, 65538} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			value := bytes.Repeat([]byte{'x'}, length)
			result := make(chan peripheralReadResult, 1)
			go func() {
				b, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
				result <- peripheralReadResult{value: b, err: err}
			}()
			nextOffset := 0
			for {
				select {
				case <-c.readPackets:
					// Drain the fake transport's bounded trace for long values.
				case packet := <-c.writes:
					if nextOffset > 0xffff {
						t.Fatalf("blob offset wrapped after %d bytes: %x", nextOffset, packet)
					}
					op := byte(attOpReadRsp)
					if nextOffset == 0 {
						if !bytes.Equal(packet, []byte{attOpReadReq, 3, 0}) {
							t.Fatalf("initial request = %x", packet)
						}
					} else {
						op = attOpReadBlobRsp
						if len(packet) != 5 || packet[0] != attOpReadBlobReq || int(binary.LittleEndian.Uint16(packet[3:])) != nextOffset {
							t.Fatalf("request at offset %d = %x", nextOffset, packet)
						}
					}
					end := min(nextOffset+22, len(value))
					c.reads <- peripheralTestRead{b: append([]byte{op}, value[nextOffset:end]...)}
					nextOffset = end
				case got := <-result:
					if length > 0xffff {
						if got.value != nil || !errors.Is(got.err, ErrInvalidLength) {
							t.Fatalf("overflowing offset = %x, %v", got.value, got.err)
						}
					} else if !bytes.Equal(got.value, value) || got.err != nil {
						t.Fatalf("read length %d = %d bytes, %v", length, len(got.value), got.err)
					}
					return
				case <-time.After(time.Second):
					t.Fatal("long read did not progress")
				}
			}
		})
	}
}

func TestPeripheralReadLongWithConcurrentMTUExchange(t *testing.T) {
	for _, cancelExchange := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelExchange), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newPeripheralTestConn()
				p := newPeripheralForTest(c)
				done := make(chan struct{})
				go func() { defer close(done); p.loop() }()
				defer func() { c.Close(); <-done }()
				readResult := make(chan peripheralReadResult, 1)
				go func() {
					b, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
					readResult <- peripheralReadResult{value: b, err: err}
				}()
				peripheralWriteForTest(t, c)
				chunk := bytes.Repeat([]byte{'x'}, 22)
				c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, chunk...)}
				if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpReadBlobReq, 3, 0, 22, 0}) {
					t.Fatalf("first continuation = %x", packet)
				}
				mtuResult := make(chan error, 1)
				go func() { mtuResult <- p.SetMTU(64) }()
				// Queue the exchange while the previous Blob transaction is pending.
				synctest.Wait()
				c.reads <- peripheralTestRead{b: append([]byte{attOpReadBlobRsp}, chunk...)}
				if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpMtuReq, 64, 0}) {
					t.Fatalf("queued MTU exchange = %x", packet)
				}
				if cancelExchange {
					c.Close()
					if err := peripheralResultForTest(t, mtuResult); err == nil {
						t.Fatal("canceled exchange returned success")
					}
					got := peripheralReadResultForTest(t, c, readResult)
					if got.value != nil || got.err == nil {
						t.Fatalf("canceled long read = %x, %v", got.value, got.err)
					}
					return
				}
				c.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, 64, 0}}
				if err := peripheralResultForTest(t, mtuResult); err != nil {
					t.Fatal(err)
				}
				if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpReadBlobReq, 3, 0, 44, 0}) {
					t.Fatalf("continuation after exchange = %x", packet)
				}
				c.reads <- peripheralTestRead{b: append([]byte{attOpReadBlobRsp}, []byte("tail")...)}
				got := peripheralReadResultForTest(t, c, readResult)
				want := append(bytes.Repeat([]byte{'x'}, 44), []byte("tail")...)
				if !bytes.Equal(got.value, want) || got.err != nil {
					t.Fatalf("long read across exchange = %x, %v", got.value, got.err)
				}
			})
		})
	}
}
