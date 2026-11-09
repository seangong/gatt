//go:build gattxpc_test

package gatt

import (
	"testing"

	"github.com/seangong/gatt/v2/xpc"
)

func TestDarwinReadRSSIResponseValidation(t *testing.T) {
	for _, tt := range []struct {
		name string
		rsp  xpc.Dict
		want int
	}{
		{"legacy success", xpc.Dict{"kCBMsgArgData": int64(-55)}, -55},
		{"success negative", xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgData": int64(-80)}, -80},
		{"success zero", xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgData": int64(0)}, 0},
		{"success positive", xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgData": int64(5)}, 5},
		{"failure without data", xpc.Dict{"kCBMsgArgResult": int64(0x0e)}, -1},
		{"failure with data", xpc.Dict{"kCBMsgArgResult": int64(0x0e), "kCBMsgArgData": int64(-55)}, -1},
		{"invalid result", xpc.Dict{"kCBMsgArgResult": "bad", "kCBMsgArgData": int64(-55)}, -1},
		{"out of range result", xpc.Dict{"kCBMsgArgResult": int64(0x100), "kCBMsgArgData": int64(-55)}, -1},
		{"missing data", xpc.Dict{"kCBMsgArgResult": int64(0)}, -1},
		{"legacy missing data", xpc.Dict{}, -1},
		{"invalid data", xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgData": "bad"}, -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				if id != 43 {
					t.Errorf("ReadRSSI command = %d, want 43", id)
				}
				return rssiRead, tt.rsp
			})
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Errorf("ReadRSSI response %v panicked: %v", tt.rsp, recovered)
				}
			}()
			if got := p.ReadRSSI(); got != tt.want {
				t.Fatalf("ReadRSSI = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDarwinReadRSSIClosedSessionOrTransport(t *testing.T) {
	for _, transport := range []bool{false, true} {
		name := "session"
		if transport {
			name = "transport"
		}
		t.Run(name, func(t *testing.T) {
			d, p := newDarwinClientTest(t, func(int, xpc.Dict) (int, xpc.Dict) {
				t.Error("closed ReadRSSI reached native transport")
				return 0, nil
			})
			var err error
			if transport {
				err = d.conn.Close()
			} else {
				err = d.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := p.ReadRSSI(); got != -1 {
				t.Fatalf("closed ReadRSSI = %d, want -1", got)
			}
		})
	}
}
