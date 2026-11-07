package gatt

import (
	"bytes"
	"fmt"
	"testing"
)

func TestReadRequestsReportHandlerStatus(t *testing.T) {
	for _, descriptor := range []bool{false, true} {
		for _, op := range []byte{attOpReadReq, attOpReadBlobReq, attOpReadByTypeReq} {
			for _, status := range []byte{StatusSuccess, StatusInvalidOffset, StatusUnexpectedError, 0xff} {
				t.Run(fmt.Sprintf("descriptor=%t/op=%02x/status=%02x", descriptor, op, status), func(t *testing.T) {
					s := NewService(UUID16(0xff00))
					char := s.AddCharacteristic(UUID16(0xff01))
					desc := char.AddDescriptor(UUID16(0x2901))
					value := []byte("result")
					offset := 0
					if op == attOpReadBlobReq {
						offset = 5
					}
					handler := func(rsp ResponseWriter, req *ReadRequest) {
						if req.Offset != offset {
							t.Errorf("handler offset = %d, want %d", req.Offset, offset)
						}
						if _, err := rsp.Write(value); err != nil {
							t.Errorf("buffer read value: %v", err)
						}
						rsp.SetStatus(status)
					}
					if descriptor {
						desc.HandleReadFunc(handler)
					} else {
						char.HandleReadFunc(handler)
					}
					c := newCentral(generateAttributes([]*Service{s}, 0x1201), nil, nil)
					defer c.cancel()
					handle, uuid := char.VHandle(), char.UUID()
					if descriptor {
						handle, uuid = desc.Handle(), desc.UUID()
					}
					req := []byte{op, byte(handle), byte(handle >> 8)}
					want := append([]byte{attOpReadRsp}, value...)
					switch op {
					case attOpReadBlobReq:
						req = append(req, byte(offset), 0)
						want[0] = attOpReadBlobRsp
					case attOpReadByTypeReq:
						req = append([]byte{op, 1, 0, 0xff, 0xff}, uuid.b...)
						want = append([]byte{attOpReadByTypeRsp, byte(len(value) + 2), byte(handle), byte(handle >> 8)}, value...)
					}
					if status != StatusSuccess {
						code := attEcodeUnlikely
						if status == StatusInvalidOffset {
							code = attEcodeInvalidOffset
						}
						want = attErrorRsp(op, handle, code)
					}
					if got := c.handleReq(req); !bytes.Equal(got, want) {
						t.Fatalf("response = %x, want %x", got, want)
					}
				})
			}
		}
	}
}

func TestReadByTypePreservesValuesBeforeHandlerError(t *testing.T) {
	first := NewService(UUID16(0xff00))
	firstChar := first.AddCharacteristic(UUID16(0xff01))
	firstChar.SetValue([]byte("ok"))
	second := NewService(UUID16(0xff02))
	secondChar := second.AddCharacteristic(firstChar.UUID())
	secondChar.HandleReadFunc(func(rsp ResponseWriter, _ *ReadRequest) {
		rsp.SetStatus(StatusUnexpectedError)
	})
	c := newCentral(generateAttributes([]*Service{first, second}, 1), nil, nil)
	defer c.cancel()
	req := append([]byte{attOpReadByTypeReq, 1, 0, 0xff, 0xff}, firstChar.UUID().b...)
	want := []byte{attOpReadByTypeRsp, 4, byte(firstChar.vh), byte(firstChar.vh >> 8), 'o', 'k'}
	if got := c.handleReq(req); !bytes.Equal(got, want) {
		t.Fatalf("response before rejected attribute = %x, want %x", got, want)
	}
	req[1], req[2] = byte(secondChar.vh), byte(secondChar.vh>>8)
	want = attErrorRsp(attOpReadByTypeReq, secondChar.vh, attEcodeUnlikely)
	if got := c.handleReq(req); !bytes.Equal(got, want) {
		t.Fatalf("retry rejected attribute = %x, want %x", got, want)
	}
}
