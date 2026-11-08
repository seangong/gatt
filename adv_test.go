package gatt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
)

func TestAppendFieldWithoutHeaderSpace(t *testing.T) {
	for _, size := range []int{30, 31} {
		for _, data := range [][]byte{nil, []byte("extra")} {
			t.Run(fmt.Sprintf("size_%d_data_%d", size, len(data)), func(t *testing.T) {
				a := (&AdvPacket{}).AppendField(typeCompleteName, bytes.Repeat([]byte{'x'}, size-2))
				before := a.Bytes()
				if got := a.AppendField(typeManufacturerData, data); got != a {
					t.Fatal("AppendField returned a different packet")
				}
				if a.Bytes() != before || a.Len() != size {
					t.Fatal("AppendField changed the packet without header space")
				}
				if !errors.Is(a.Err(), ErrEIRPacketTooLong) {
					t.Fatalf("Err() = %v, want ErrEIRPacketTooLong", a.Err())
				}
				for _, appendAgain := range []func() *AdvPacket{
					func() *AdvPacket { return a.AppendFlags(flagLEOnly) },
					func() *AdvPacket { return a.AppendName("name") },
					func() *AdvPacket { return a.AppendManufacturerData(0x004c, nil) },
				} {
					if got := appendAgain(); got != a || a.Bytes() != before || a.Len() != size {
						t.Fatal("a later append changed the failed packet")
					}
					if !errors.Is(a.Err(), ErrEIRPacketTooLong) {
						t.Fatalf("a later append cleared the error: %v", a.Err())
					}
				}
			})
		}
	}
}

func TestAppendField(t *testing.T) {
	for _, tt := range []struct {
		name  string
		size  int
		data  []byte
		field []byte
	}{
		{name: "empty", field: []byte{1, typeManufacturerData}},
		{name: "exact_fit", data: bytes.Repeat([]byte{'v'}, 29), field: append([]byte{30, typeManufacturerData}, bytes.Repeat([]byte{'v'}, 29)...)},
		{name: "truncated", data: bytes.Repeat([]byte{'v'}, 30), field: append([]byte{30, typeManufacturerData}, bytes.Repeat([]byte{'v'}, 29)...)},
		{name: "empty_at_28", size: 28, field: []byte{1, typeManufacturerData}},
		{name: "fits_at_28", size: 28, data: []byte("v"), field: []byte{2, typeManufacturerData, 'v'}},
		{name: "truncated_at_28", size: 28, data: []byte("vw"), field: []byte{2, typeManufacturerData, 'v'}},
		{name: "empty_at_29", size: 29, field: []byte{1, typeManufacturerData}},
		{name: "truncated_at_29", size: 29, data: []byte("value"), field: []byte{1, typeManufacturerData}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			prefix := bytes.Repeat([]byte{'p'}, tt.size)
			a := &AdvPacket{b: bytes.Clone(prefix)}
			if got := a.AppendField(typeManufacturerData, tt.data); got != a {
				t.Fatal("AppendField returned a different packet")
			}
			var want [31]byte
			copy(want[:], append(prefix, tt.field...))
			if a.Bytes() != want || a.Len() != tt.size+len(tt.field) {
				t.Fatalf("got %x (length %d), want %x (length %d)", a.Bytes(), a.Len(), want, tt.size+len(tt.field))
			}
			if a.Err() != nil {
				t.Fatalf("Err() = %v, want nil", a.Err())
			}
		})
	}
}

func TestAppendFlags(t *testing.T) {
	a := (&AdvPacket{}).AppendFlags(flagGeneralDiscoverable | flagLEOnly)
	want := [31]byte{2, typeFlags, 6}
	if a.Bytes() != want || a.Len() != 3 || a.Err() != nil {
		t.Fatalf("AppendFlags: got %x (length %d, error %v), want %x", a.Bytes(), a.Len(), a.Err(), want)
	}
}

func TestAppendName(t *testing.T) {
	cases := []struct {
		curr      []byte
		name      string
		wantBytes []byte
		wantLen   int
	}{
		{
			curr:      []byte{},
			name:      "ABCDE",
			wantBytes: []byte{0x06, typeCompleteName, 'A', 'B', 'C', 'D', 'E'},
			wantLen:   7,
		},
		{
			curr:      []byte("111111111122222222223333"),
			name:      "ABCDE",
			wantBytes: append([]byte("111111111122222222223333"), []byte{0x06, typeCompleteName, 'A', 'B', 'C', 'D', 'E'}...),
			wantLen:   31,
		},
		{
			curr:      []byte("1111111111222222222233333"),
			name:      "ABCDE",
			wantBytes: append([]byte("1111111111222222222233333"), []byte{0x05, typeShortName, 'A', 'B', 'C', 'D'}...),
			wantLen:   31,
		},
	}
	for _, tt := range cases {
		a := (&AdvPacket{b: tt.curr}).AppendName(tt.name)
		wantBytes := [31]byte{}
		copy(wantBytes[:], tt.wantBytes)
		if a.Bytes() != wantBytes {
			t.Errorf("%q a.AppendName(%q) got %x want %x", tt.curr, tt.name, a.Bytes(), tt.wantBytes)
		}
		if a.Len() != tt.wantLen {
			t.Errorf("%q a.AppendName(%q) got %d want %d", tt.curr, tt.name, a.Len(), tt.wantLen)
		}
		if a.Err() != nil {
			t.Errorf("%q a.AppendName(%q) error: %v", tt.curr, tt.name, a.Err())
		}
	}
}

func TestAppendManufacturerData(t *testing.T) {
	for _, size := range []int{23, 24, 25} {
		t.Run(fmt.Sprintf("data_%d", size), func(t *testing.T) {
			a := (&AdvPacket{}).AppendFlags(flagGeneralDiscoverable | flagLEOnly)
			a.AppendManufacturerData(0x004c, bytes.Repeat([]byte{0xab}, size))
			dataSize := min(size, 24)
			wantData := append([]byte{2, typeFlags, 6, byte(dataSize + 3), typeManufacturerData, 0x4c, 0}, bytes.Repeat([]byte{0xab}, dataSize)...)
			var want [31]byte
			copy(want[:], wantData)
			if a.Bytes() != want || a.Len() != len(wantData) || a.Err() != nil {
				t.Fatalf("AppendManufacturerData: got %x (length %d, error %v), want %x", a.Bytes(), a.Len(), a.Err(), want)
			}
		})
	}
}

func TestAppendUUIDFit(t *testing.T) {
	cases := []struct {
		uu      []UUID
		want    string
		wantFit bool
	}{
		{
			want:    "020106",
			wantFit: true,
		},
		{
			uu:      []UUID{UUID16(0xFAFE)},
			want:    "0201060303fefa",
			wantFit: true,
		},
		{
			uu:      []UUID{UUID16(0xFAFE), UUID16(0xFAF9)},
			want:    "0201060303fefa0303f9fa",
			wantFit: true,
		},
		{
			uu:      []UUID{MustParseUUID("ABABABABABABABABABABABABABABABAB")},
			want:    "0201061107abababababababababababababababab",
			wantFit: true,
		},
		{
			uu: []UUID{
				MustParseUUID("ABABABABABABABABABABABABABABABAB"),
				MustParseUUID("CDCDCDCDCDCDCDCDCDCDCDCDCDCDCDCD"),
			},
			want: "0201061106abababababababababababababababab",
		},
		{
			uu: []UUID{
				UUID16(0xaaaa), UUID16(0xbbbb),
				UUID16(0xcccc), UUID16(0xdddd),
				UUID16(0xeeee), UUID16(0xffff),
				UUID16(0xaaaa), UUID16(0xbbbb),
			},
			want: "0201060302aaaa0302bbbb0302cccc0302dddd0302eeee0302ffff0302aaaa",
		},
		{
			uu:      []UUID{attrGAPUUID, UUID16(0xFAFE), attrGATTUUID},
			want:    "0201060303fefa",
			wantFit: true,
		},
	}

	for _, tt := range cases {
		t.Run(fmt.Sprint(tt.uu), func(t *testing.T) {
			a := (&AdvPacket{}).AppendFlags(flagGeneralDiscoverable | flagLEOnly)
			if fit := a.AppendUUIDFit(tt.uu); fit != tt.wantFit {
				t.Errorf("AppendUUIDFit(%v) = %t, want %t", tt.uu, fit, tt.wantFit)
			}
			got := a.Bytes()
			if hex.EncodeToString(got[:a.Len()]) != tt.want {
				t.Errorf("AppendUUIDFit(%v) packet = %x, want %s", tt.uu, got[:a.Len()], tt.want)
			}
			if a.Err() != nil {
				t.Errorf("AppendUUIDFit(%v) error: %v", tt.uu, a.Err())
			}
		})
	}
}

func TestAppendUUIDFitWithoutSpace(t *testing.T) {
	a := (&AdvPacket{}).AppendName(string(bytes.Repeat([]byte{'n'}, 29)))
	before := a.Bytes()
	if a.AppendUUIDFit([]UUID{UUID16(0xFAFE)}) {
		t.Fatal("AppendUUIDFit reported a UUID fitting in a full packet")
	}
	if a.Bytes() != before || a.Len() != 31 || a.Err() != nil {
		t.Fatalf("AppendUUIDFit changed the full packet: %x, length %d, error %v", a.Bytes(), a.Len(), a.Err())
	}
}

func FuzzAdvPacket(f *testing.F) {
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{2, 0}, 16))
	f.Add(append(append([]byte{0, 29}, bytes.Repeat([]byte{'x'}, 29)...), 3, 0))
	f.Add(append([]byte{4, 0, 1, 30}, bytes.Repeat([]byte{'n'}, 30)...))
	u128 := MustParseUUID("ABABABABABABABABABABABABABABABAB")
	f.Fuzz(func(t *testing.T, data []byte) {
		data = data[:min(len(data), 1024)]
		a := &AdvPacket{}
		for operations := 0; len(data) > 0 && operations < 32; operations++ {
			op := data[0]
			data = data[1:]
			size := 0
			if len(data) > 0 {
				size = int(data[0]) % 41
				data = data[1:]
			}
			size = min(size, len(data))
			payload := data[:size]
			data = data[size:]
			before, beforeLen, beforeErr := a.Bytes(), a.Len(), a.Err()
			switch op % 5 {
			case 0:
				a.AppendField(op, payload)
			case 1:
				a.AppendName(string(payload))
			case 2:
				a.AppendFlags(op)
			case 3:
				a.AppendManufacturerData(uint16(op), payload)
			case 4:
				a.AppendUUIDFit([]UUID{UUID16(uint16(op)), u128})
			}
			if a.Len() > MaxEIRPacketLength || len(a.b) > MaxEIRPacketLength {
				t.Fatalf("packet exceeded %d bytes: %d", MaxEIRPacketLength, len(a.b))
			}
			if a.Err() != nil && (a.Bytes() != before || a.Len() != beforeLen) {
				t.Fatal("failed append changed existing packet data")
			}
			if beforeErr != nil && a.Err() != beforeErr {
				t.Fatalf("append changed existing error: %v -> %v", beforeErr, a.Err())
			}
		}
	})
}
