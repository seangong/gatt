package gatt

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestAppendNamePreservesUTF8Characters(t *testing.T) {
	for _, tt := range []struct {
		name     string
		prefix   int
		value    string
		want     string
		wantType byte
		wantErr  bool
	}{
		{name: "empty", wantType: typeCompleteName},
		{name: "ASCII", value: strings.Repeat("a", 40), want: strings.Repeat("a", 29), wantType: typeShortName},
		{name: "two_byte_boundary", value: strings.Repeat("a", 28) + "é", want: strings.Repeat("a", 28), wantType: typeShortName},
		{name: "Chinese", value: strings.Repeat("中", 10), want: strings.Repeat("中", 9), wantType: typeShortName},
		{name: "Chinese_after_flags", prefix: 3, value: strings.Repeat("中", 10), want: strings.Repeat("中", 8), wantType: typeShortName},
		{name: "four_byte_boundary", value: strings.Repeat("😀", 8), want: strings.Repeat("😀", 7), wantType: typeShortName},
		{name: "exact_fit", value: strings.Repeat("中", 9) + "ab", want: strings.Repeat("中", 9) + "ab", wantType: typeCompleteName},
		{name: "no_character_fits", prefix: 27, value: "中", wantType: typeShortName},
		{name: "header_only", prefix: 29, value: "中", wantType: typeShortName},
		{name: "no_header_space_30", prefix: 30, value: "中", wantErr: true},
		{name: "no_header_space_31", prefix: 31, value: "中", wantErr: true},
		{name: "invalid_input_compatibility", value: strings.Repeat("a", 28) + "\xfftail", want: strings.Repeat("a", 28) + "\xff", wantType: typeShortName},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := &AdvPacket{}
			if tt.prefix != 0 {
				a.AppendField(typeShortName, bytes.Repeat([]byte{'p'}, tt.prefix-2))
			}
			before := append([]byte(nil), a.b...)
			if got := a.AppendName(tt.value); got != a {
				t.Fatal("AppendName returned a different packet")
			}
			if tt.wantErr {
				if !errors.Is(a.Err(), ErrEIRPacketTooLong) || !bytes.Equal(a.b, before) {
					t.Fatalf("name without header space changed packet: packet=%x error=%v", a.b, a.Err())
				}
				return
			}
			if a.Err() != nil || a.Len() > MaxEIRPacketLength || !bytes.Equal(a.b[:tt.prefix], before) {
				t.Fatalf("AppendName changed prefix or failed: length=%d error=%v", a.Len(), a.Err())
			}
			field := a.b[tt.prefix:]
			if field[1] != tt.wantType || int(field[0])+1 != len(field) || string(field[2:]) != tt.want {
				t.Fatalf("name field = %x, want type %x value %q", field, tt.wantType, tt.want)
			}
			if utf8.ValidString(tt.value) && !utf8.Valid(field[2:]) {
				t.Fatalf("valid name became invalid UTF-8: %x", field[2:])
			}
		})
	}
}

func TestAppendManufacturerDataPreservesCompanyID(t *testing.T) {
	for _, prefix := range []int{0, 25, 26, 27, 28, 29, 30, 31} {
		for _, payload := range [][]byte{nil, {0xa1, 0xa2, 0xa3}} {
			t.Run(fmt.Sprintf("prefix_%d_payload_%d", prefix, len(payload)), func(t *testing.T) {
				a := &AdvPacket{}
				if prefix != 0 {
					a.AppendName(strings.Repeat("p", prefix-2))
				}
				before := append([]byte(nil), a.b...)
				if got := a.AppendManufacturerData(0x1234, payload); got != a {
					t.Fatal("AppendManufacturerData returned a different packet")
				}
				if prefix > MaxEIRPacketLength-4 {
					if !errors.Is(a.Err(), ErrEIRPacketTooLong) || !bytes.Equal(a.b, before) {
						t.Fatalf("incomplete company ID was accepted: packet=%x error=%v", a.b, a.Err())
					}
					return
				}
				wantData := append([]byte{0x34, 0x12}, payload[:min(len(payload), MaxEIRPacketLength-prefix-4)]...)
				want := append(append([]byte(nil), before...), byte(len(wantData)+1), typeManufacturerData)
				want = append(want, wantData...)
				if a.Err() != nil || !bytes.Equal(a.b, want) {
					t.Fatalf("manufacturer field = %x, error %v, want %x", a.b, a.Err(), want)
				}
			})
		}
	}
}

func TestManufacturerPrefixFailureKeepsPacketFrozen(t *testing.T) {
	for _, prefix := range []int{28, 29} {
		t.Run(fmt.Sprint(prefix), func(t *testing.T) {
			a := (&AdvPacket{}).AppendName(strings.Repeat("p", prefix-2))
			before := a.Bytes()
			a.AppendManufacturerData(0x1234, nil)
			if !errors.Is(a.Err(), ErrEIRPacketTooLong) || a.Bytes() != before || a.Len() != prefix {
				t.Fatal("company ID rejection changed the packet or did not report an error")
			}
			cause := a.Err()
			for _, appendAgain := range []func(){
				func() { a.AppendField(typeTxPower, []byte{1}) },
				func() { a.AppendName("") },
				func() { a.AppendFlags(flagLEOnly) },
				func() { a.AppendManufacturerData(0x4321, nil) },
				func() { a.AppendUUIDFit([]UUID{UUID16(0x180f)}) },
			} {
				appendAgain()
				if a.Bytes() != before || a.Len() != prefix || a.Err() != cause {
					t.Fatalf("later append changed failed packet: packet=%x error=%v", a.b, a.Err())
				}
			}
		})
	}
}

func TestRawFieldRetainsPayloadTruncation(t *testing.T) {
	a := (&AdvPacket{}).AppendName(strings.Repeat("p", 26))
	a.AppendField(typeManufacturerData, []byte{0x34, 0x12, 0xa1})
	if a.Err() != nil || a.Len() != MaxEIRPacketLength || !bytes.Equal(a.b[28:], []byte{2, typeManufacturerData, 0x34}) {
		t.Fatalf("raw field truncation changed: packet=%x error=%v", a.b, a.Err())
	}
}

func TestAdvertiseRejectsIncompleteCompanyPrefix(t *testing.T) {
	a := (&AdvPacket{}).AppendName(strings.Repeat("p", 26)).AppendManufacturerData(0x1234, nil)
	// Reaching controller or native IO on this uninitialized device would fail.
	if err := (&device{}).Advertise(a); !errors.Is(err, ErrEIRPacketTooLong) {
		t.Fatalf("Advertise error = %v, want ErrEIRPacketTooLong", err)
	}
}
