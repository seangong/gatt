package gatt

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func checkAdvertisedUUIDFields(t *testing.T, a *AdvPacket) {
	t.Helper()
	if len(a.b) > MaxEIRPacketLength {
		t.Fatalf("packet length = %d", len(a.b))
	}
	seen := make(map[int]bool)
	for b := a.b; len(b) != 0; {
		n := int(b[0]) + 1
		if n < 2 || n > len(b) {
			t.Fatalf("malformed advertising field: %x", b)
		}
		width := 0
		switch b[1] {
		case typeAllUUID16, typeSomeUUID16:
			width = 2
		case typeAllUUID128, typeSomeUUID128:
			width = 16
		}
		if width != 0 {
			if seen[width] || (n-2)%width != 0 {
				t.Fatalf("repeated or partial %d-bit UUID list: %x", width*8, a.b)
			}
			seen[width] = true
		}
		b = b[n:]
	}
}

func TestAppendUUIDFitGroupsWidthsAndCompleteness(t *testing.T) {
	u, v := MustParseUUID(strings.Repeat("ab", 16)), MustParseUUID(strings.Repeat("cd", 16))
	for _, tc := range []struct {
		name string
		uu   []UUID
		wire string
		fit  bool
	}{
		{"two16", []UUID{UUID16(0xfafe), UUID16(0xfaf9)}, "0201060503fefaf9fa", true},
		{"eight16", []UUID{UUID16(0xaaaa), UUID16(0xbbbb), UUID16(0xcccc), UUID16(0xdddd), UUID16(0xeeee), UUID16(0xffff), UUID16(0xaaaa), UUID16(0xbbbb)}, "0201061103aaaabbbbccccddddeeeeffffaaaabbbb", true},
		{"complete16-incomplete128", []UUID{UUID16(0xaaaa), UUID16(0xbbbb), u, v}, "0201060503aaaabbbb1106" + strings.Repeat("ab", 16), false},
		{"incomplete16-complete128", []UUID{UUID16(0xaaaa), UUID16(0xbbbb), u, UUID16(0xcccc), UUID16(0xdddd), UUID16(0xeeee)}, "0201060902aaaabbbbccccdddd1107" + strings.Repeat("ab", 16), false},
		{"128-first-exact-capacity", []UUID{u, UUID16(0xaaaa), UUID16(0xbbbb), UUID16(0xcccc), UUID16(0xdddd)}, "0201061107" + strings.Repeat("ab", 16) + "0903aaaabbbbccccdddd", true},
		{"stop-at-first-capacity-failure", []UUID{u, v, UUID16(0xaaaa)}, "0201061106" + strings.Repeat("ab", 16), false},
		{"filter-gap-gatt", []UUID{attrGAPUUID, UUID16(0xfafe), attrGATTUUID}, "0201060303fefa", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := (&AdvPacket{}).AppendFlags(flagGeneralDiscoverable | flagLEOnly)
			if got := a.AppendUUIDFit(tc.uu); got != tc.fit {
				t.Errorf("fit = %t, want %t", got, tc.fit)
			}
			if got := hex.EncodeToString(a.b); got != tc.wire || a.Err() != nil {
				t.Fatalf("wire=%s error=%v, want %s", got, a.Err(), tc.wire)
			}
			checkAdvertisedUUIDFields(t, a)
		})
	}
}

func TestAppendUUIDFitMergesExistingRawListsAndRepeatedCalls(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		typ := byte(typeAllUUID16)
		if incomplete {
			typ = typeSomeUUID16
		}
		a := (&AdvPacket{}).AppendFlags(flagLEOnly).
			AppendField(typ, UUID16(0xaaaa).b).
			AppendManufacturerData(0x004c, nil).
			AppendField(typeAllUUID16, UUID16(0xbbbb).b)
		before := a.Bytes()
		if !a.AppendUUIDFit(nil) || !a.AppendUUIDFit([]UUID{attrGAPUUID, attrGATTUUID}) || a.Bytes() != before {
			t.Fatal("empty or filtered input changed an existing packet")
		}
		for _, u := range []UUID{UUID16(0xcccc), UUID16(0xdddd)} {
			if !a.AppendUUIDFit([]UUID{u}) {
				t.Fatal("UUID did not fit after merging an existing list")
			}
		}
		want := append([]byte{2, typeFlags, flagLEOnly, 9, typ}, []byte{0xaa, 0xaa, 0xbb, 0xbb, 0xcc, 0xcc, 0xdd, 0xdd}...)
		want = append(want, 3, typeManufacturerData, 0x4c, 0)
		if !bytes.Equal(a.b, want) || a.Err() != nil {
			t.Fatalf("merged lists = %x error=%v, want %x", a.b, a.Err(), want)
		}
		checkAdvertisedUUIDFields(t, a)
	}
	// Existing complete lists can explicitly contain no UUIDs of their width.
	u := MustParseUUID(strings.Repeat("ab", 16))
	a := (&AdvPacket{}).AppendField(typeAllUUID128, nil).AppendName("name").AppendField(typeAllUUID128, u.b)
	if !a.AppendUUIDFit([]UUID{UUID16(0xaaaa)}) {
		t.Fatal("mixed UUID did not fit after merging raw 128-bit lists")
	}
	want := append([]byte{17, typeAllUUID128}, u.b...)
	want = append(want, 5, typeCompleteName, 'n', 'a', 'm', 'e', 3, typeAllUUID16, 0xaa, 0xaa)
	if !bytes.Equal(a.b, want) {
		t.Fatalf("128-bit raw lists = %x, want %x", a.b, want)
	}
	checkAdvertisedUUIDFields(t, a)
}

func TestAppendUUIDFitDoesNotTruncateOrMislabelAtCapacity(t *testing.T) {
	a := (&AdvPacket{}).AppendField(typeManufacturerData, bytes.Repeat([]byte{'x'}, 24))
	if a.AppendUUIDFit([]UUID{UUID16(0xaaaa), UUID16(0xbbbb)}) {
		t.Fatal("partial UUID list reported complete fit")
	}
	if got := a.b[26:]; !bytes.Equal(got, []byte{3, typeSomeUUID16, 0xaa, 0xaa}) || a.Len() != 30 || a.Err() != nil {
		t.Fatalf("partial list = %x length=%d error=%v", got, a.Len(), a.Err())
	}
	checkAdvertisedUUIDFields(t, a)

	a = (&AdvPacket{}).AppendFlags(flagLEOnly)
	uu := make([]UUID, 13)
	for i := range uu {
		uu[i] = UUID16(uint16(0xff00 + i))
	}
	if !a.AppendUUIDFit(uu) || a.Len() != 31 {
		t.Fatal("13 packed 16-bit UUIDs did not fit exactly")
	}
	if a.AppendUUIDFit([]UUID{UUID16(0xffff)}) || a.b[4] != typeSomeUUID16 || a.Len() != 31 || a.Err() != nil {
		t.Fatalf("omitted UUID retained complete list: %x error=%v", a.b, a.Err())
	}
	checkAdvertisedUUIDFields(t, a)
}

func TestAppendUUIDFitRejectsUnsupportedAndMalformedInputWithoutPacketError(t *testing.T) {
	for _, invalid := range []UUID{{}, {b: []byte{1}}, {b: []byte{1, 2, 3, 4}}} {
		a := (&AdvPacket{}).AppendFlags(flagLEOnly)
		before := a.Bytes()
		if a.AppendUUIDFit([]UUID{invalid}) || a.Bytes() != before || a.Err() != nil {
			t.Fatalf("invalid UUID length %d changed packet or error: %x %v", invalid.Len(), a.b, a.Err())
		}
		if a.AppendUUIDFit([]UUID{invalid, UUID16(0xaaaa)}) || !bytes.Equal(a.b, []byte{2, typeFlags, flagLEOnly, 3, typeAllUUID16, 0xaa, 0xaa}) || a.Err() != nil {
			t.Fatalf("mixed invalid UUID length %d = %x error=%v", invalid.Len(), a.b, a.Err())
		}
		checkAdvertisedUUIDFields(t, a)
	}
	for _, data := range [][]byte{
		{2, typeAllUUID16, 0xaa},
		{3, typeAllUUID16, 0xaa},
		{2, typeAllUUID128, 0xaa},
	} {
		a := &AdvPacket{b: bytes.Clone(data)}
		if a.AppendUUIDFit([]UUID{UUID16(0xbbbb)}) || !bytes.Equal(a.b, data) || a.Err() != nil {
			t.Fatalf("malformed existing packet was rewritten: %x error=%v", a.b, a.Err())
		}
	}
}

func TestAppendUUIDFitPreservesStickyPacketError(t *testing.T) {
	a := (&AdvPacket{}).AppendField(typeAllUUID16, UUID16(0xaaaa).b).
		AppendField(typeManufacturerData, bytes.Repeat([]byte{'x'}, 21)).
		AppendField(typeAllUUID16, UUID16(0xbbbb).b).AppendFlags(flagLEOnly)
	if !errors.Is(a.Err(), ErrEIRPacketTooLong) || a.Len() != 31 {
		t.Fatal("fixture did not fill the packet and fail its final field")
	}
	before := a.Bytes()
	if a.AppendUUIDFit([]UUID{UUID16(0xcccc)}) || a.Bytes() != before || !errors.Is(a.Err(), ErrEIRPacketTooLong) {
		t.Fatal("UUID regrouping modified a packet with an existing error")
	}
	if !a.AppendUUIDFit(nil) || a.Bytes() != before || !errors.Is(a.Err(), ErrEIRPacketTooLong) {
		t.Fatal("empty input changed sticky error or existing packet")
	}
}

func FuzzAppendUUIDFit(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6})
	f.Add(bytes.Repeat([]byte{255}, 32))
	f.Fuzz(func(t *testing.T, data []byte) {
		data = data[:min(len(data), 128)]
		a := (&AdvPacket{}).AppendFlags(flagLEOnly).AppendField(typeAllUUID16, UUID16(0xaaaa).b)
		if len(data) != 0 {
			a.AppendName(strings.Repeat("n", int(data[0])%13))
		}
		a.AppendField(typeSomeUUID16, UUID16(0xbbbb).b)
		a.AppendUUIDFit([]UUID{UUID16(0xcccc)})
		checkAdvertisedUUIDFields(t, a)
		u128 := MustParseUUID(strings.Repeat("ab", 16))
		for _, v := range data {
			uu := []UUID{UUID16(uint16(v)), u128}
			if v&1 != 0 {
				uu[0], uu[1] = uu[1], uu[0]
			}
			if v%3 == 0 {
				uu = append(uu, UUID{})
			}
			a.AppendUUIDFit(uu)
			checkAdvertisedUUIDFields(t, a)
			if a.Err() != nil {
				t.Fatalf("bounded UUID append added packet error: %v", a.Err())
			}
		}
	})
}
