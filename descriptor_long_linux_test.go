package gatt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

func TestPeripheralReadDescriptorCompleteValue(t *testing.T) {
	for _, tt := range []struct {
		name     string
		length   int
		terminal attEcode
	}{
		{"empty", 0, attEcodeSuccess},
		{"short", 21, attEcodeSuccess},
		{"one full response then empty", 22, attEcodeSuccess},
		{"one full response then attribute not long", 22, attEcodeAttrNotLong},
		{"one full response then invalid offset", 22, attEcodeInvalidOffset},
		{"two full responses then empty", 44, attEcodeSuccess},
		{"two full responses then invalid offset", 44, attEcodeInvalidOffset},
		{"53 bytes", 53, attEcodeSuccess},
		{"512 bytes", 512, attEcodeSuccess},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			value := bytes.Repeat([]byte{'x'}, tt.length)
			result := make(chan peripheralReadResult, 1)
			go func() {
				b, err := p.ReadDescriptor(&Descriptor{h: 4})
				result <- peripheralReadResult{value: b, err: err}
			}()
			offset := 0
			for {
				select {
				case <-c.readPackets:
					// Drain the bounded transport trace for values needing many PDUs.
				case packet := <-c.writes:
					op := byte(attOpReadRsp)
					if offset == 0 {
						if !bytes.Equal(packet, []byte{attOpReadReq, 4, 0}) {
							t.Fatalf("initial descriptor request = %x", packet)
						}
					} else {
						op = attOpReadBlobRsp
						if len(packet) != 5 || packet[0] != attOpReadBlobReq ||
							binary.LittleEndian.Uint16(packet[1:3]) != 4 ||
							int(binary.LittleEndian.Uint16(packet[3:])) != offset {
							t.Fatalf("descriptor continuation at offset %d = %x", offset, packet)
						}
						if offset == len(value) && tt.terminal != attEcodeSuccess {
							c.reads <- peripheralTestRead{b: attErrorRsp(attOpReadBlobReq, 4, tt.terminal)}
							continue
						}
					}
					end := min(offset+22, len(value))
					c.reads <- peripheralTestRead{b: append([]byte{op}, value[offset:end]...)}
					offset = end
				case got := <-result:
					if !bytes.Equal(got.value, value) || got.err != nil {
						t.Fatalf("descriptor returned %d of %d bytes, error = %v", len(got.value), len(value), got.err)
					}
					return
				case <-time.After(time.Second):
					t.Fatal("descriptor read did not progress")
				}
			}
		})
	}
}

func TestPeripheralReadDescriptorBlobErrors(t *testing.T) {
	for _, tt := range []struct {
		name      string
		op        byte
		handle    uint16
		code      attEcode
		laterBlob bool
	}{
		{"permission denied", attOpReadBlobReq, 4, attEcodeReadNotPerm, false},
		{"blob unsupported", attOpReadBlobReq, 4, attEcodeReqNotSupp, false},
		{"invalid offset for another handle", attOpReadBlobReq, 5, attEcodeInvalidOffset, false},
		{"invalid offset for another operation", attOpReadReq, 4, attEcodeInvalidOffset, false},
		{"attribute not long for another handle", attOpReadBlobReq, 5, attEcodeAttrNotLong, false},
		{"attribute not long after continuation", attOpReadBlobReq, 4, attEcodeAttrNotLong, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			result := make(chan peripheralReadResult, 1)
			go func() {
				b, err := p.ReadDescriptor(&Descriptor{h: 4})
				result <- peripheralReadResult{value: b, err: err}
			}()
			peripheralWriteForTest(t, c)
			chunk := bytes.Repeat([]byte{'x'}, 22)
			c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, chunk...)}
			peripheralWriteForTest(t, c)
			if tt.laterBlob {
				c.reads <- peripheralTestRead{b: append([]byte{attOpReadBlobRsp}, chunk...)}
				peripheralWriteForTest(t, c)
			}
			c.reads <- peripheralTestRead{b: attErrorRsp(tt.op, tt.handle, tt.code)}
			got := peripheralReadResultForTest(t, c, result)
			if got.value != nil || got.err == nil {
				t.Fatalf("descriptor error returned partial success: %x, %v", got.value, got.err)
			}
			if tt.op == attOpReadBlobReq && !errors.Is(got.err, tt.code) {
				t.Fatalf("descriptor error = %v, want %v", got.err, tt.code)
			}
		})
	}
}

func TestPeripheralReadDescriptorUsesTransactionMTU(t *testing.T) {
	for _, attributeNotLong := range []bool{false, true} {
		t.Run(fmt.Sprintf("attributeNotLong=%t", attributeNotLong), func(t *testing.T) {
			c := newPeripheralTestConn()
			defer c.Close()
			p := newPeripheralForTest(c)
			p.mtu = 64
			result := make(chan peripheralReadResult, 1)
			go func() {
				b, err := p.ReadDescriptor(&Descriptor{h: 4})
				result <- peripheralReadResult{value: b, err: err}
			}()
			var first message
			select {
			case first = <-p.reqc:
			case <-time.After(time.Second):
				t.Fatal("initial descriptor read was not submitted")
			}
			prefix := bytes.Repeat([]byte{'x'}, 22)
			first.rspc <- peripheralResult{b: append([]byte{attOpReadRsp}, prefix...), mtu: 23}
			var blob message
			select {
			case blob = <-p.reqc:
			case got := <-result:
				t.Fatalf("old-MTU descriptor response was considered complete: %x, %v", got.value, got.err)
			case <-time.After(time.Second):
				t.Fatal("descriptor continuation was not submitted")
			}
			if !bytes.Equal(blob.b, []byte{attOpReadBlobReq, 4, 0, 22, 0}) {
				t.Fatalf("descriptor continuation = %x", blob.b)
			}
			response := peripheralResult{b: []byte{attOpReadBlobRsp, 't'}, mtu: 64}
			if attributeNotLong {
				response.b = attErrorRsp(attOpReadBlobReq, 4, attEcodeAttrNotLong)
				response.err = fmt.Errorf("scripted descriptor response: %w", attEcodeAttrNotLong)
			}
			blob.rspc <- response
			got := peripheralReadResultForTest(t, c, result)
			if attributeNotLong {
				if got.value != nil || !errors.Is(got.err, attEcodeAttrNotLong) {
					t.Fatalf("uncertain descriptor completeness after MTU change = %x, %v", got.value, got.err)
				}
			} else if !bytes.Equal(got.value, append(prefix, 't')) || got.err != nil {
				t.Fatalf("descriptor read across MTU change = %x, %v", got.value, got.err)
			}
		})
	}
}

func TestPeripheralReadDescriptorCanceledContinuation(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprintf("timeout=%t", timeout), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newPeripheralTestConn()
				p := newPeripheralForTest(c)
				done := make(chan struct{})
				go func() { defer close(done); p.loop() }()
				defer func() { c.Close(); <-done }()
				result := make(chan peripheralReadResult, 1)
				go func() {
					b, err := p.ReadDescriptor(&Descriptor{h: 4})
					result <- peripheralReadResult{value: b, err: err}
				}()
				peripheralWriteForTest(t, c)
				c.reads <- peripheralTestRead{b: append([]byte{attOpReadRsp}, bytes.Repeat([]byte{'x'}, 22)...)}
				if packet := peripheralWriteForTest(t, c); !bytes.Equal(packet, []byte{attOpReadBlobReq, 4, 0, 22, 0}) {
					t.Fatalf("descriptor continuation = %x", packet)
				}
				if timeout {
					time.Sleep(attTransactionTimeout + time.Nanosecond)
				} else {
					c.Close()
				}
				got := peripheralReadResultForTest(t, c, result)
				if got.value != nil || got.err == nil {
					t.Fatalf("canceled descriptor returned partial success: %x, %v", got.value, got.err)
				}
				if timeout && !errors.Is(got.err, context.DeadlineExceeded) {
					t.Fatalf("descriptor timeout = %v", got.err)
				}
			})
		})
	}
}
