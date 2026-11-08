package gatt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestMTUNegotiationUpTo517(t *testing.T) {
	for _, tt := range []struct {
		requested uint16
		want      uint16
	}{
		{0, 23},
		{22, 23},
		{23, 23},
		{247, 247},
		{256, 256},
		{512, 512},
		{517, 517},
		{518, 517},
		{65535, 517},
	} {
		t.Run(fmt.Sprint(tt.requested), func(t *testing.T) {
			conn := &notificationTestConn{}
			c := newCentral(nil, nil, conn)
			request := []byte{attOpMtuReq, byte(tt.requested), byte(tt.requested >> 8)}
			want := []byte{attOpMtuRsp, byte(tt.want), byte(tt.want >> 8)}
			if !c.handleAndRespond(request) {
				t.Fatal("MTU response was not sent")
			}
			if got := conn.packets[0]; !bytes.Equal(got, want) {
				t.Errorf("MTU response = %x, want %x", got, want)
			}
			if got := c.MTU(); got != int(tt.want) {
				t.Errorf("MTU() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNotificationValueLimitAtMTU517(t *testing.T) {
	n, conn := newNotificationTestNotifier(t)
	if !n.central.handleAndRespond([]byte{attOpMtuReq, 0x05, 0x02}) {
		t.Fatal("MTU response was not sent")
	}
	conn.packets = nil
	if got := n.Cap(); got != 512 {
		t.Fatalf("Cap() at MTU 517 = %d, want 512", got)
	}
	payload := bytes.Repeat([]byte{0x5a}, 512)
	if count, err := n.Write(payload); count != len(payload) || err != nil {
		t.Fatalf("Write(512 bytes) = (%d, %v), want (512, nil)", count, err)
	}
	handle := n.a.pvt.(*Descriptor).char.vh
	want := append([]byte{attOpHandleNotify, byte(handle), byte(handle >> 8)}, payload...)
	if len(conn.packets) != 1 || !bytes.Equal(conn.packets[0], want) {
		t.Fatalf("notification packets = %x, want [%x]", conn.packets, want)
	}
	for _, size := range []int{513, 514} {
		if count, err := n.Write(bytes.Repeat([]byte{0x5a}, size)); count != 0 || !errors.Is(err, io.ErrShortWrite) {
			t.Errorf("Write(%d bytes) = (%d, %v), want (0, short write)", size, count, err)
		}
		if len(conn.packets) != 1 {
			t.Fatalf("oversized %d-byte value reached transport", size)
		}
	}
}

func TestReadByTypeLengthAtMTU517(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		t.Run(fmt.Sprintf("dynamic=%t", dynamic), func(t *testing.T) {
			service := NewService(UUID16(0xff00))
			characteristic := service.AddCharacteristic(UUID16(0xff01))
			value := bytes.Repeat([]byte{0x5a}, 512)
			var calls, capacity int
			if dynamic {
				characteristic.HandleReadFunc(func(rsp ResponseWriter, req *ReadRequest) {
					calls++
					capacity = req.Cap
					if _, err := rsp.Write(value[:min(len(value), req.Cap)]); err != nil {
						t.Errorf("write read response: %v", err)
					}
				})
			} else {
				characteristic.SetValue(value)
			}
			c := newCentral(generateAttributes([]*Service{service}, 0x1201), nil, &notificationTestConn{})
			if !c.handleAndRespond([]byte{attOpMtuReq, 0x05, 0x02}) {
				t.Fatal("MTU response was not sent")
			}
			handle := characteristic.VHandle()
			request := []byte{attOpReadByTypeReq, byte(handle), byte(handle >> 8), byte(handle), byte(handle >> 8), 0x01, 0xff}
			want := append([]byte{attOpReadByTypeRsp, 255, byte(handle), byte(handle >> 8)}, value[:253]...)
			if got := c.handleReq(request); !bytes.Equal(got, want) {
				t.Errorf("read-by-type response = %x, want %x", got, want)
			}
			if dynamic && (calls != 1 || capacity != 253) {
				t.Errorf("read handler calls=%d, capacity=%d, want 1 and 253", calls, capacity)
			}
		})
	}
}

func TestReadValueCapacityAtMTU517(t *testing.T) {
	for _, tt := range []struct {
		name     string
		opcode   byte
		response byte
		offset   uint16
	}{
		{"read", attOpReadReq, attOpReadRsp, 0},
		{"read blob", attOpReadBlobReq, attOpReadBlobRsp, 0},
		{"read blob suffix", attOpReadBlobReq, attOpReadBlobRsp, 500},
	} {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(UUID16(0xff00))
			characteristic := service.AddCharacteristic(UUID16(0xff01))
			value := bytes.Repeat([]byte{0x5a}, 512)
			var calls int
			characteristic.HandleReadFunc(func(rsp ResponseWriter, req *ReadRequest) {
				calls++
				if req.Cap != 512 || req.Offset != int(tt.offset) {
					t.Errorf("read request capacity=%d, offset=%d, want 512 and %d", req.Cap, req.Offset, tt.offset)
				}
				end := min(req.Offset+req.Cap, len(value))
				if _, err := rsp.Write(value[req.Offset:end]); err != nil {
					t.Errorf("write read response: %v", err)
				}
			})
			c := newCentral(generateAttributes([]*Service{service}, 0x1201), nil, &notificationTestConn{})
			if !c.handleAndRespond([]byte{attOpMtuReq, 0x05, 0x02}) {
				t.Fatal("MTU response was not sent")
			}
			handle := characteristic.VHandle()
			request := []byte{tt.opcode, byte(handle), byte(handle >> 8)}
			if tt.opcode == attOpReadBlobReq {
				request = append(request, byte(tt.offset), byte(tt.offset>>8))
			}
			want := append([]byte{tt.response}, value[tt.offset:]...)
			if got := c.handleReq(request); !bytes.Equal(got, want) {
				t.Errorf("read response = %x, want %x", got, want)
			}
			if calls != 1 {
				t.Errorf("read handler calls = %d, want 1", calls)
			}
		})
	}
}

func TestWriteValueLimitAtMTU517(t *testing.T) {
	for _, opcode := range []byte{attOpWriteReq, attOpWriteCmd} {
		for _, size := range []int{512, 513} {
			t.Run(fmt.Sprintf("opcode=%02x/size=%d", opcode, size), func(t *testing.T) {
				service := NewService(UUID16(0xff00))
				characteristic := service.AddCharacteristic(UUID16(0xff01))
				var calls int
				var written []byte
				characteristic.HandleWriteFunc(func(_ Request, data []byte) byte {
					calls++
					written = append([]byte(nil), data...)
					return StatusSuccess
				})
				c := newCentral(generateAttributes([]*Service{service}, 0x1201), nil, &notificationTestConn{})
				if !c.handleAndRespond([]byte{attOpMtuReq, 0x05, 0x02}) {
					t.Fatal("MTU response was not sent")
				}
				handle := characteristic.VHandle()
				payload := bytes.Repeat([]byte{0x5a}, size)
				request := append([]byte{opcode, byte(handle), byte(handle >> 8)}, payload...)
				var want []byte
				if opcode == attOpWriteReq {
					want = []byte{attOpWriteRsp}
					if size > 512 {
						want = []byte{attOpError, opcode, byte(handle), byte(handle >> 8), 0x0d}
					}
				}
				if got := c.handleReq(request); !bytes.Equal(got, want) {
					t.Errorf("write response = %x, want %x", got, want)
				}
				if size == 512 {
					if calls != 1 || !bytes.Equal(written, payload) {
						t.Errorf("write handler calls=%d, value length=%d, want one complete %d-byte write", calls, len(written), size)
					}
				} else if calls != 0 {
					t.Errorf("oversized value reached write handler %d times", calls)
				}
			})
		}
	}
}
