package gatt

import (
	"bytes"
	"testing"
)

func TestAdvertisementUnmarshallBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name    string
		data    []byte
		wantErr bool
		nameOut string
	}{
		{name: "empty"},
		{name: "zero_terminator", data: []byte{0}},
		{name: "zero_padding", data: []byte{2, typeCompleteName, 'a', 0, 0}, nameOut: "a"},
		{name: "zero_ends_payload", data: []byte{0, 2, typeCompleteName, 'b'}},
		{name: "truncated_field", data: []byte{3, typeCompleteName, 'a'}, wantErr: true},
		{name: "overflowing_length", data: []byte{0xff, typeCompleteName}, wantErr: true},
		{name: "valid_prefix_before_bad_tail", data: []byte{2, typeCompleteName, 'a', 0xff}, wantErr: true, nameOut: "a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if got := recover(); got != nil {
					t.Errorf("unmarshall panicked: %v", got)
				}
			}()
			var a Advertisement
			if err := a.unmarshall(tt.data); (err != nil) != tt.wantErr {
				t.Fatalf("unmarshall() = %v, want error %t", err, tt.wantErr)
			}
			if a.LocalName != tt.nameOut {
				t.Errorf("LocalName = %q, want %q", a.LocalName, tt.nameOut)
			}
		})
	}
}

func TestAdvertisementUnmarshallUUIDFields(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  byte
		size int
		sol  bool
	}{
		{"some16", typeSomeUUID16, 2, false},
		{"all16", typeAllUUID16, 2, false},
		{"some32", typeSomeUUID32, 4, false},
		{"all32", typeAllUUID32, 4, false},
		{"some128", typeSomeUUID128, 16, false},
		{"all128", typeAllUUID128, 16, false},
		{"solicited16", typeServiceSol16, 2, true},
		{"solicited32", typeServiceSol32, 4, true},
		{"solicited128", typeServiceSol128, 16, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, length := range []int{0, tt.size, tt.size * 2, tt.size + 1} {
				data := append([]byte{byte(length + 1), tt.typ}, bytes.Repeat([]byte{0xaa}, length)...)
				data = append(data, 2, typeCompleteName, 'n')
				var a Advertisement
				if err := a.unmarshall(data); err != nil {
					t.Fatalf("unmarshall(%x) = %v", data, err)
				}
				uuids := a.Services
				if tt.sol {
					uuids = a.SolicitedService
				}
				want := 0
				if length%tt.size == 0 {
					want = length / tt.size
				}
				if len(uuids) != want || a.LocalName != "n" {
					t.Fatalf("length %d: got %d UUIDs and name %q, want %d UUIDs and name n", length, len(uuids), a.LocalName, want)
				}
			}
		})
	}
}

func TestAdvertisementUnmarshallSkipsEmptyTxPower(t *testing.T) {
	for _, tt := range []struct {
		data []byte
		want int
	}{
		{[]byte{1, typeTxPower}, 0},
		{[]byte{2, typeTxPower, 0}, 0},
		{[]byte{2, typeTxPower, 127}, 127},
		{[]byte{2, typeTxPower, 0xff}, -1},
		{[]byte{2, typeTxPower, 0x81}, -127},
		{[]byte{2, typeTxPower, 0xc5}, -59},
		{[]byte{3, typeTxPower, 0xc5, 1}, -59},
	} {
		data := append(append([]byte(nil), tt.data...), 2, typeCompleteName, 'n')
		var a Advertisement
		if err := a.unmarshall(data); err != nil {
			t.Fatalf("unmarshall(%x) = %v", data, err)
		}
		if a.TxPowerLevel != tt.want || a.LocalName != "n" {
			t.Errorf("unmarshall(%x) = power %d, name %q; want power %d, name n", data, a.TxPowerLevel, a.LocalName, tt.want)
		}
	}
}

func FuzzAdvertisementUnmarshall(f *testing.F) {
	for _, seed := range [][]byte{nil, {0, 0}, {2, 2, 0xaa}, {1, typeTxPower}, {0xff, typeCompleteName}, {2, typeCompleteName, 'a'}, {5, typeAllUUID32, 0x78, 0x56, 0x34, 0x12}, {4, typeServiceData16, 0xaa, 0xfe, 0x42}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var a Advertisement
		_ = a.unmarshall(data)
		uuids := append([]UUID(nil), a.Services...)
		uuids = append(uuids, a.SolicitedService...)
		for _, sd := range a.ServiceData {
			uuids = append(uuids, sd.UUID)
		}
		for _, u := range uuids {
			if u.Len() != 2 && u.Len() != 16 {
				t.Fatalf("decoded UUID has unsupported length %d", u.Len())
			}
			parsed, err := ParseUUID(u.String())
			if err != nil || !parsed.Equal(u) {
				t.Fatalf("decoded UUID cannot round trip: %s, %v", u, err)
			}
		}
	})
}
