package gatt

import (
	"bytes"
	"testing"
)

func TestAdvertisementUnmarshall32BitUUIDs(t *testing.T) {
	for _, typ := range []byte{typeSomeUUID32, typeAllUUID32, typeServiceSol32} {
		t.Run(map[byte]string{typeSomeUUID32: "some", typeAllUUID32: "all", typeServiceSol32: "solicited"}[typ], func(t *testing.T) {
			data := []byte{9, typ, 0x0d, 0x18, 0, 0, 0x78, 0x56, 0x34, 0x12}
			var a Advertisement
			if err := a.unmarshall(data); err != nil {
				t.Fatal(err)
			}
			got := a.Services
			if typ == typeServiceSol32 {
				got = a.SolicitedService
			}
			want := []UUID{
				MustParseUUID("0000180d-0000-1000-8000-00805f9b34fb"),
				MustParseUUID("12345678-0000-1000-8000-00805f9b34fb"),
			}
			if len(got) != len(want) {
				t.Fatalf("UUIDs = %v, want %v", got, want)
			}
			for i, u := range got {
				if u.Len() != 16 || !u.Equal(want[i]) {
					t.Errorf("UUID[%d] = %s (%d bytes), want %s (16 bytes)", i, u, u.Len(), want[i])
				}
				parsed, err := ParseUUID(u.String())
				if err != nil || !parsed.Equal(u) {
					t.Errorf("UUID[%d] failed public round trip: %v", i, err)
				}
				packet := new(AdvPacket)
				if !packet.AppendUUIDFit([]UUID{u}) {
					t.Errorf("UUID[%d] cannot be used by the advertising builder", i)
				}
			}
		})
	}
}

func TestAdvertisementUnmarshallOwnsUUIDs(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  byte
		wire []byte
		want UUID
	}{
		{"services16", typeAllUUID16, []byte{0x0d, 0x18}, UUID16(0x180d)},
		{"services32", typeAllUUID32, []byte{0x78, 0x56, 0x34, 0x12}, MustParseUUID("12345678-0000-1000-8000-00805f9b34fb")},
		{"services128", typeAllUUID128, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00").b, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00")},
		{"solicited16", typeServiceSol16, []byte{0x0d, 0x18}, UUID16(0x180d)},
		{"solicited32", typeServiceSol32, []byte{0x78, 0x56, 0x34, 0x12}, MustParseUUID("12345678-0000-1000-8000-00805f9b34fb")},
		{"solicited128", typeServiceSol128, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00").b, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := append([]byte{byte(len(tt.wire) + 1), tt.typ}, tt.wire...)
			var a Advertisement
			if err := a.unmarshall(data); err != nil {
				t.Fatal(err)
			}
			got := a.Services
			if len(a.SolicitedService) != 0 {
				got = a.SolicitedService
			}
			for i := range data {
				data[i] = 0
			}
			if len(got) != 1 || !got[0].Equal(tt.want) {
				t.Fatalf("UUID changed with the input buffer: %v, want [%s]", got, tt.want)
			}
		})
	}
}

func TestAdvertisementUnmarshallServiceData(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  byte
		wire []byte
		want UUID
	}{
		{"uuid16", typeServiceData16, []byte{0xaa, 0xfe}, UUID16(0xfeaa)},
		{"uuid32", typeServiceData32, []byte{0x78, 0x56, 0x34, 0x12}, MustParseUUID("12345678-0000-1000-8000-00805f9b34fb")},
		{"uuid128", typeServiceData128, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00").b, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, payload := range [][]byte{nil, {0x10, 0x01, 0x02}} {
				data := append([]byte{byte(len(tt.wire) + len(payload) + 1), tt.typ}, tt.wire...)
				data = append(data, payload...)
				data = append(data, 2, typeCompleteName, 'n', 2, typeTxPower, 0xc5, 4, typeManufacturerData, 0x34, 0x12, 0xab)
				var a Advertisement
				if err := a.unmarshall(data); err != nil {
					t.Fatal(err)
				}
				if len(a.ServiceData) != 1 {
					t.Fatalf("ServiceData = %+v, want one entry", a.ServiceData)
				}
				sd := a.ServiceData[0]
				if !sd.UUID.Equal(tt.want) || !bytes.Equal(sd.Data, payload) {
					t.Errorf("ServiceData = UUID %s, data %x; want UUID %s, data %x", sd.UUID, sd.Data, tt.want, payload)
				}
				if a.LocalName != "n" || a.TxPowerLevel != -59 || !bytes.Equal(a.ManufacturerData, []byte{0x34, 0x12, 0xab}) {
					t.Errorf("other fields changed: %+v", a)
				}
				for i := range data {
					data[i] = 0
				}
				if !sd.UUID.Equal(tt.want) || !bytes.Equal(sd.Data, payload) {
					t.Errorf("ServiceData aliases the input buffer: %+v", sd)
				}
				if len(sd.Data) != 0 {
					sd.Data[0] = 0xff
					if payload[0] != 0x10 {
						t.Error("ServiceData aliases the caller's payload")
					}
				}
			}
		})
	}
}

func TestAdvertisementUnmarshallSkipsShortServiceData(t *testing.T) {
	for _, tt := range []struct {
		typ  byte
		size int
	}{{typeServiceData16, 2}, {typeServiceData32, 4}, {typeServiceData128, 16}} {
		for size := 0; size < tt.size; size++ {
			data := append([]byte{byte(size + 1), tt.typ}, bytes.Repeat([]byte{0xaa}, size)...)
			data = append(data, 4, typeServiceData16, 0x0d, 0x18, 0x42, 2, typeCompleteName, 'n')
			var a Advertisement
			if err := a.unmarshall(data); err != nil {
				t.Fatalf("type %02x length %d: %v", tt.typ, size, err)
			}
			if len(a.ServiceData) != 1 || !a.ServiceData[0].UUID.Equal(UUID16(0x180d)) || !bytes.Equal(a.ServiceData[0].Data, []byte{0x42}) || a.LocalName != "n" {
				t.Errorf("type %02x length %d discarded valid fields: %+v", tt.typ, size, a)
			}
		}
	}
}

func TestAdvertisementUnmarshallServiceDataPreservesOrder(t *testing.T) {
	data := []byte{
		4, typeServiceData16, 0xaa, 0xfe, 0x01,
		6, typeServiceData32, 0x78, 0x56, 0x34, 0x12, 0x02,
		4, typeServiceData16, 0xaa, 0xfe, 0x03,
	}
	var a Advertisement
	if err := a.unmarshall(data); err != nil {
		t.Fatal(err)
	}
	if len(a.ServiceData) != 3 {
		t.Fatalf("ServiceData = %+v, want three ordered entries", a.ServiceData)
	}
	want := []UUID{UUID16(0xfeaa), MustParseUUID("12345678-0000-1000-8000-00805f9b34fb"), UUID16(0xfeaa)}
	for i, sd := range a.ServiceData {
		if !sd.UUID.Equal(want[i]) || !bytes.Equal(sd.Data, []byte{byte(i + 1)}) {
			t.Errorf("ServiceData[%d] = %+v", i, sd)
		}
	}
	a.ServiceData[0].Data[0] = 0xff
	if a.ServiceData[2].Data[0] != 3 {
		t.Error("duplicate ServiceData entries share payload storage")
	}
}
