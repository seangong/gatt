//go:build gattxpc_test

package gatt

import (
	"testing"

	"github.com/seangong/gatt/v2/xpc"
)

func newDarwinServerTestDevice(t *testing.T) (*device, chan xpc.Dict) {
	t.Helper()
	d := newDarwinLifecycleTestDevice()
	d.ready = make(chan struct{})
	messages := make(chan xpc.Dict, 16)
	conn, cleanup := xpc.NewTestEchoConnection(darwinBridgeHandler(func(event xpc.Dict, err error) {
		if err != nil {
			d.HandleXpcEvent(nil, err)
			return
		}
		messages <- event
	}))
	d.conn = conn
	close(d.ready)
	t.Cleanup(func() { _ = d.Close(); cleanup() })
	return d, messages
}

func darwinServerReplies(t *testing.T, d *device, messages <-chan xpc.Dict) []xpc.Dict {
	t.Helper()
	if err := d.conn.SendError(xpc.Dict{"testBarrier": int64(1)}, false); err != nil {
		t.Fatal(err)
	}
	var replies []xpc.Dict
	for {
		message := receiveDarwin(t, messages)
		if message.Contains("testBarrier") {
			return replies
		}
		if id := message.MustGetInt("kCBMsgId"); id != 13 {
			t.Fatalf("unexpected server command %d", id)
		}
		replies = append(replies, message.MustGetDict("kCBMsgArgs"))
	}
}

func darwinWriteForTest(attribute, offset, ignoreResponse int) xpc.Dict {
	return xpc.Dict{
		"kCBMsgArgAttributeID": int64(attribute), "kCBMsgArgOffset": int64(offset),
		"kCBMsgArgIgnoreResponse": int64(ignoreResponse), "kCBMsgArgData": []byte("value"),
	}
}

func darwinRequestForTest() xpc.Dict {
	return xpc.Dict{
		"kCBMsgArgDeviceUUID":    xpc.MakeUUID("00112233445566778899aabbccddeeff"),
		"kCBMsgArgTransactionID": int64(7),
	}
}

func TestDarwinServerApplicationStatus(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			status byte
			want   attEcode
		}{
			{"success", StatusSuccess, attEcodeSuccess},
			{"invalid-offset", StatusInvalidOffset, attEcodeInvalidOffset},
			{"unexpected", StatusUnexpectedError, attEcodeUnlikely},
			{"unknown", 0xff, attEcodeUnlikely},
		} {
			name := "write/" + tc.name
			if read {
				name = "read/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				d, messages := newDarwinServerTestDevice(t)
				char := &Characteristic{}
				calls := 0
				if read {
					char.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
						calls++
						_, _ = w.Write([]byte("value"))
						w.SetStatus(tc.status)
					})
				} else {
					char.HandleWriteFunc(func(Request, []byte) byte { calls++; return tc.status })
				}
				d.attrs[1] = &attr{h: 1, pvt: char}
				args := darwinRequestForTest()
				if read {
					args["kCBMsgArgAttributeID"], args["kCBMsgArgOffset"] = int64(1), int64(0)
					d.respondToRequest(19, args)
				} else {
					args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(1, 0, 0)}
					d.respondToRequest(20, args)
				}
				replies := darwinServerReplies(t, d, messages)
				if calls != 1 || len(replies) != 1 {
					t.Fatalf("calls=%d replies=%v", calls, replies)
				}
				if got := replies[0].MustGetInt("kCBMsgArgResult"); got != int(tc.want) {
					t.Fatalf("native status=%d, want %d", got, tc.want)
				}
				if tc.want != attEcodeSuccess && len(replies[0].GetBytes("kCBMsgArgData", nil)) != 0 {
					t.Fatal("failed response contains successful value data")
				}
			})
		}
	}
}

func TestDarwinServerRejectsUnsafeWritesBeforeCallbacks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		writes xpc.Array
		want   attEcode
		noRsp  bool
	}{
		{"two-attributes", xpc.Array{darwinWriteForTest(1, 0, 0), darwinWriteForTest(2, 0, 0)}, attEcodeReqNotSupp, false},
		{"same-attribute", xpc.Array{darwinWriteForTest(1, 0, 0), darwinWriteForTest(1, 0, 0)}, attEcodeReqNotSupp, false},
		{"invalid-second", xpc.Array{darwinWriteForTest(1, 0, 0), darwinWriteForTest(99, 0, 0)}, attEcodeReqNotSupp, false},
		{"offset", xpc.Array{darwinWriteForTest(1, 1, 0)}, attEcodeInvalidOffset, false},
		{"empty", xpc.Array{}, attEcodeInvalidPDU, false},
		{"no-response-batch", xpc.Array{darwinWriteForTest(1, 0, 1), darwinWriteForTest(2, 0, 1)}, attEcodeReqNotSupp, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, messages := newDarwinServerTestDevice(t)
			calls := 0
			for _, h := range []uint16{1, 2} {
				char := &Characteristic{}
				char.HandleWriteFunc(func(Request, []byte) byte { calls++; return StatusSuccess })
				d.attrs[int(h)] = &attr{h: h, pvt: char}
			}
			args := darwinRequestForTest()
			args["kCBMsgArgATTWrites"] = tc.writes
			d.respondToRequest(20, args)
			replies := darwinServerReplies(t, d, messages)
			if calls != 0 {
				t.Fatalf("unsafe write executed %d callbacks", calls)
			}
			if tc.noRsp {
				if len(replies) != 0 {
					t.Fatal("write command received a response")
				}
				return
			}
			if len(replies) != 1 || replies[0].MustGetInt("kCBMsgArgResult") != int(tc.want) {
				t.Fatalf("replies=%v, want status %d", replies, tc.want)
			}
		})
	}
}

func TestDarwinServerWriteCommandDoesNotReply(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	char := &Characteristic{}
	calls := 0
	char.HandleWriteFunc(func(Request, []byte) byte { calls++; return StatusUnexpectedError })
	d.attrs[1] = &attr{h: 1, pvt: char}
	args := darwinRequestForTest()
	args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(1, 0, 1)}
	d.respondToRequest(20, args)
	if replies := darwinServerReplies(t, d, messages); calls != 1 || len(replies) != 0 {
		t.Fatalf("calls=%d replies=%v", calls, replies)
	}
}

func TestDarwinServerWriteUsesNativeAttributeID(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	const id = 70001 // blued attribute IDs are local integers, not ATT handles.
	char := &Characteristic{}
	calls := 0
	char.HandleWriteFunc(func(Request, []byte) byte { calls++; return StatusSuccess })
	d.attrs[id] = &attr{pvt: char}
	args := darwinRequestForTest()
	args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(id, 0, 0)}
	d.respondToRequest(20, args)
	replies := darwinServerReplies(t, d, messages)
	if calls != 1 || len(replies) != 1 || replies[0].MustGetInt("kCBMsgArgResult") != 0 ||
		replies[0].MustGetInt("kCBMsgArgAttributeID") != id {
		t.Fatalf("native attribute write: calls=%d replies=%v", calls, replies)
	}
}

func TestDarwinServerMixedBatchRepliesToRequestWithoutSideEffects(t *testing.T) {
	d, messages := newDarwinServerTestDevice(t)
	calls := 0
	for _, id := range []int{1, 2} {
		char := &Characteristic{}
		char.HandleWriteFunc(func(Request, []byte) byte { calls++; return StatusSuccess })
		d.attrs[id] = &attr{pvt: char}
	}
	args := darwinRequestForTest()
	args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(1, 0, 1), darwinWriteForTest(2, 0, 0)}
	d.respondToRequest(20, args)
	replies := darwinServerReplies(t, d, messages)
	if calls != 0 || len(replies) != 1 || replies[0].MustGetInt("kCBMsgArgAttributeID") != 2 ||
		replies[0].MustGetInt("kCBMsgArgResult") != int(attEcodeReqNotSupp) {
		t.Fatalf("mixed batch: calls=%d replies=%v", calls, replies)
	}
}

func TestDarwinServerMissingAttributeOrHandlerReturnsError(t *testing.T) {
	for _, read := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			d, messages := newDarwinServerTestDevice(t)
			if present {
				d.attrs[1] = &attr{pvt: &Characteristic{}}
			}
			args := darwinRequestForTest()
			want := attEcodeInvalidHandle
			if read {
				args["kCBMsgArgAttributeID"], args["kCBMsgArgOffset"] = int64(1), int64(0)
				d.respondToRequest(19, args)
				if present {
					want = attEcodeReadNotPerm
				}
			} else {
				args["kCBMsgArgATTWrites"] = xpc.Array{darwinWriteForTest(1, 0, 0)}
				d.respondToRequest(20, args)
				if present {
					want = attEcodeWriteNotPerm
				}
			}
			replies := darwinServerReplies(t, d, messages)
			if len(replies) != 1 || replies[0].MustGetInt("kCBMsgArgResult") != int(want) {
				t.Fatalf("read=%t present=%t replies=%v, want %d", read, present, replies, want)
			}
		}
	}
}
