package gatt

import (
	"bytes"
	"testing"
)

func TestReadRequestsRejectWriteOnlyAttributes(t *testing.T) {
	for _, descriptor := range []bool{false, true} {
		for _, op := range []byte{attOpReadReq, attOpReadBlobReq, attOpReadByTypeReq} {
			s := NewService(UUID16(0xff00))
			char := s.AddCharacteristic(UUID16(0xff01))
			var desc *Descriptor
			if descriptor {
				desc = char.AddDescriptor(UUID16(0x2901))
				desc.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess })
			} else {
				char.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess })
			}
			c := newCentral(generateAttributes([]*Service{s}, 1), nil, nil)
			handle, uuid := char.vh, char.uuid
			if descriptor {
				handle, uuid = desc.h, desc.uuid
			}
			req := []byte{op, byte(handle), byte(handle >> 8)}
			if op == attOpReadBlobReq {
				req = append(req, 0, 0)
			} else if op == attOpReadByTypeReq {
				req = append([]byte{op, byte(handle), byte(handle >> 8), byte(handle), byte(handle >> 8)}, uuid.b...)
			}
			want := attErrorRsp(op, handle, attEcodeReadNotPerm)
			got := c.handleReq(req)
			c.cancel()
			if !bytes.Equal(got, want) {
				t.Fatalf("descriptor=%t/op=%02x response = %x, want %x", descriptor, op, got, want)
			}
		}
	}
}

func TestCharacteristicDeclarationIsReadOnly(t *testing.T) {
	s := NewService(UUID16(0xff00))
	char := s.AddCharacteristic(UUID16(0xff01))
	calls := 0
	char.HandleWriteFunc(func(Request, []byte) byte { calls++; return StatusSuccess })
	c := newCentral(generateAttributes([]*Service{s}, 1), nil, nil)
	defer c.cancel()
	h := char.h
	if got, want := c.handleReq([]byte{attOpReadReq, byte(h), byte(h >> 8)}),
		append([]byte{attOpReadRsp, byte(char.props), byte(char.vh), byte(char.vh >> 8)}, char.uuid.b...); !bytes.Equal(got, want) {
		t.Fatalf("declaration read = %x, want %x", got, want)
	}
	for _, op := range []byte{attOpWriteReq, attOpWriteCmd} {
		got := c.handleReq([]byte{op, byte(h), byte(h >> 8), 0x42})
		if op == attOpWriteReq {
			if want := attErrorRsp(op, h, attEcodeWriteNotPerm); !bytes.Equal(got, want) {
				t.Fatalf("declaration write = %x, want %x", got, want)
			}
		} else if got != nil {
			t.Fatalf("declaration write command responded: %x", got)
		}
	}
	if calls != 0 {
		t.Fatalf("declaration writes invoked the business handler %d times", calls)
	}
	if got := c.handleReq([]byte{attOpWriteReq, byte(char.vh), byte(char.vh >> 8), 0x42}); !bytes.Equal(got, []byte{attOpWriteRsp}) || calls != 1 {
		t.Fatalf("value write = %x, handler calls = %d", got, calls)
	}
	request := append([]byte{attOpReadByTypeReq, 1, 0, 0xff, 0xff}, attrCharacteristicUUID.b...)
	want := append([]byte{attOpReadByTypeRsp, 7, byte(h), byte(h >> 8), byte(char.props), byte(char.vh), byte(char.vh >> 8)}, char.uuid.b...)
	if got := c.handleReq(request); !bytes.Equal(got, want) {
		t.Fatalf("write-only characteristic discovery = %x, want %x", got, want)
	}
}

func TestReadByTypePreservesValuesBeforePermissionError(t *testing.T) {
	first := NewService(UUID16(0xff00))
	readable := first.AddCharacteristic(UUID16(0xff01))
	readable.SetValue([]byte("ok"))
	second := NewService(UUID16(0xff02))
	writeOnly := second.AddCharacteristic(readable.UUID())
	writeOnly.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess })
	c := newCentral(generateAttributes([]*Service{first, second}, 1), nil, nil)
	defer c.cancel()
	req := append([]byte{attOpReadByTypeReq, 1, 0, 0xff, 0xff}, readable.UUID().b...)
	want := []byte{attOpReadByTypeRsp, 4, byte(readable.vh), byte(readable.vh >> 8), 'o', 'k'}
	if got := c.handleReq(req); !bytes.Equal(got, want) {
		t.Fatalf("page before permission error = %x, want %x", got, want)
	}
	req[1], req[2] = byte(writeOnly.vh), byte(writeOnly.vh>>8)
	want = attErrorRsp(attOpReadByTypeReq, writeOnly.vh, attEcodeReadNotPerm)
	if got := c.handleReq(req); !bytes.Equal(got, want) {
		t.Fatalf("page starting at permission error = %x, want %x", got, want)
	}
}

func TestReadableAttributeWithoutHandlerReturnsError(t *testing.T) {
	for _, descriptor := range []bool{false, true} {
		var target interface{} = &Characteristic{props: CharRead}
		if descriptor {
			target = &Descriptor{props: CharRead}
		}
		c := newCentral(&attrRange{base: 1, aa: []attr{{h: 1, typ: UUID16(0xff01), props: CharRead, pvt: target}}}, nil, nil)
		got := c.handleReq([]byte{attOpReadReq, 1, 0})
		c.cancel()
		if want := attErrorRsp(attOpReadReq, 1, attEcodeUnlikely); !bytes.Equal(got, want) {
			t.Fatalf("descriptor=%t missing handler response = %x, want %x", descriptor, got, want)
		}
	}
}
