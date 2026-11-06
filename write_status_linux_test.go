package gatt

import (
	"bytes"
	"testing"
)

func TestWriteRequestReportsHandlerStatus(t *testing.T) {
	statuses := []struct {
		name   string
		status byte
		code   byte
	}{
		{name: "success", status: StatusSuccess},
		{name: "invalid_offset", status: StatusInvalidOffset, code: 0x07},
		{name: "unexpected_error", status: StatusUnexpectedError, code: 0x0e},
		{name: "unknown_status", status: 0xff, code: 0x0e},
	}
	for _, target := range []string{"characteristic", "descriptor"} {
		for _, tt := range statuses {
			t.Run(target+"/"+tt.name, func(t *testing.T) {
				service := NewService(UUID16(0xff00))
				characteristic := service.AddCharacteristic(UUID16(0xff01))
				descriptor := characteristic.AddDescriptor(UUID16(0x2901))
				var calls [2]int
				var values [2][]byte
				characteristic.HandleWriteFunc(func(_ Request, data []byte) byte {
					calls[0]++
					values[0] = append([]byte(nil), data...)
					return tt.status
				})
				descriptor.HandleWriteFunc(func(_ Request, data []byte) byte {
					calls[1]++
					values[1] = append([]byte(nil), data...)
					return tt.status
				})
				c := newCentral(generateAttributes([]*Service{service}, 0x1201), nil, nil)
				handle, index := characteristic.VHandle(), 0
				if target == "descriptor" {
					handle, index = descriptor.Handle(), 1
				}
				value := []byte{0x61, 0x00, 0xff}
				request := append([]byte{attOpWriteReq, byte(handle), byte(handle >> 8)}, value...)
				got := c.handleReq(request)
				want := []byte{attOpWriteRsp}
				if tt.code != 0 {
					want = []byte{attOpError, attOpWriteReq, byte(handle), byte(handle >> 8), tt.code}
				}
				if !bytes.Equal(got, want) {
					t.Errorf("response = %x, want %x", got, want)
				}
				wantCalls := [2]int{}
				wantCalls[index] = 1
				if calls != wantCalls {
					t.Errorf("handler calls = %v, want %v", calls, wantCalls)
				}
				if !bytes.Equal(values[index], value) {
					t.Errorf("handler value = %x, want %x", values[index], value)
				}
			})
		}
	}
}

func TestWriteCommandNeverReplies(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		status     byte
		value      []byte
		wantCalls  [2]int
		wantWrites int
	}{
		{name: "characteristic_success", target: "characteristic", value: []byte{1}, wantCalls: [2]int{1, 0}, wantWrites: 1},
		{name: "descriptor_success", target: "descriptor", value: []byte{2}, wantCalls: [2]int{0, 1}, wantWrites: 1},
		{name: "characteristic_rejected", target: "characteristic", status: StatusUnexpectedError, value: []byte{3}, wantCalls: [2]int{1, 0}},
		{name: "descriptor_rejected", target: "descriptor", status: StatusInvalidOffset, value: []byte{4}, wantCalls: [2]int{0, 1}},
		{name: "invalid_handle", target: "invalid", value: []byte{5}},
		{name: "write_not_permitted", target: "read_only", value: []byte{6}},
		{name: "invalid_cccd_length", target: "cccd", value: []byte{1}},
		{name: "missing_handle", target: "malformed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(UUID16(0xff00))
			characteristic := service.AddCharacteristic(UUID16(0xff01))
			descriptor := characteristic.AddDescriptor(UUID16(0x2901))
			var calls [2]int
			var values [2][]byte
			writes, notifications := 0, 0
			characteristic.HandleWriteFunc(func(_ Request, data []byte) byte {
				calls[0]++
				values[0] = append([]byte(nil), data...)
				if tt.status == StatusSuccess {
					writes++
				}
				return tt.status
			})
			descriptor.HandleWriteFunc(func(_ Request, data []byte) byte {
				calls[1]++
				values[1] = append([]byte(nil), data...)
				if tt.status == StatusSuccess {
					writes++
				}
				return tt.status
			})
			readOnly := service.AddCharacteristic(UUID16(0xff02))
			readOnly.SetValue([]byte("read only"))
			notify := service.AddCharacteristic(UUID16(0xff03))
			notify.HandleNotifyFunc(func(Request, Notifier) { notifications++ })
			c := newCentral(generateAttributes([]*Service{service}, 1), nil, nil)
			handles := map[string]uint16{
				"characteristic": characteristic.VHandle(),
				"descriptor":     descriptor.Handle(),
				"read_only":      readOnly.VHandle(),
				"cccd":           notify.Descriptor().Handle(),
				"invalid":        0xffff,
			}
			handle := handles[tt.target]
			request := append([]byte{attOpWriteCmd, byte(handle), byte(handle >> 8)}, tt.value...)
			if tt.target == "malformed" {
				request = []byte{attOpWriteCmd, 0x01}
			}
			if response := c.handleReq(request); response != nil {
				t.Errorf("write command returned response %x", response)
			}
			if calls != tt.wantCalls {
				t.Errorf("handler calls = %v, want %v", calls, tt.wantCalls)
			}
			for i, count := range tt.wantCalls {
				if count != 0 && !bytes.Equal(values[i], tt.value) {
					t.Errorf("handler %d value = %x, want %x", i, values[i], tt.value)
				}
			}
			if writes != tt.wantWrites {
				t.Errorf("business writes = %d, want %d", writes, tt.wantWrites)
			}
			if notifications != 0 || len(c.notifiers) != 0 {
				t.Errorf("unexpected notification subscription: calls=%d, notifiers=%d", notifications, len(c.notifiers))
			}
		})
	}
}
