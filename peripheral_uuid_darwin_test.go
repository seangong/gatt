//go:build gattxpc_test

package gatt

import (
	"strings"
	"testing"

	"github.com/seangong/gatt/v2/xpc"
)

func TestDarwinKnownPeripheralUUIDRoundTrip(t *testing.T) {
	for _, value := range []string{
		"00112233445566778899aabbccddeeff",
		"9A3D0001-7F31-4EB2-9F66-14B489BDF001",
	} {
		t.Run(value, func(t *testing.T) {
			u := MustParseUUID(value)
			p := NewPeripheral(u)
			want := strings.ToLower(strings.ReplaceAll(value, "-", ""))
			if p.ID() != want || u.String() != want {
				t.Fatalf("NewPeripheral(ParseUUID(%q)).ID() = %s, UUID = %s", value, p.ID(), u.String())
			}
			commands := make(chan xpc.Dict, 2)
			d, old := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
				commands <- xpc.Dict{"id": int64(id), "uuid": args["kCBMsgArgDeviceUUID"]}
				return 0, nil
			})
			delete(d.plist, old.ID())
			ended := make(chan Peripheral, 1)
			d.peripheralDisconnected = func(got Peripheral, _ error) { ended <- got }
			d.Connect(p)
			command := receiveDarwin(t, commands)
			if command.MustGetInt("id") != 31 || command["uuid"].(xpc.UUID).String() != want {
				t.Fatalf("known UUID Connect command = %v, want UUID %s", command, want)
			}
			d.CancelConnection(p)
			command = receiveDarwin(t, commands)
			if command.MustGetInt("id") != 32 || command["uuid"].(xpc.UUID).String() != want {
				t.Fatalf("known UUID cancel command = %v, want UUID %s", command, want)
			}
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{
				"kCBMsgArgDeviceUUID": command["uuid"],
			}}, nil)
			if receiveDarwin(t, ended) != p {
				t.Fatal("pending cancellation changed the original argument")
			}
		})
	}
}

func TestDarwinKnownPeripheralZeroUUID(t *testing.T) {
	if p := NewPeripheral(UUID{}); p.ID() != "" {
		t.Fatalf("zero UUID peripheral ID = %q", p.ID())
	}
}
