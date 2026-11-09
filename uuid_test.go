package gatt

import (
	"bytes"
	"testing"
)

func TestUUID16(t *testing.T) {
	if want, got := (UUID{[]byte{0x00, 0x18}}), UUID16(0x1800); !got.Equal(want) {
		t.Errorf("UUID16: got %x, want %x", got, want)
	}
}

func TestUUIDRepresentationCompatibility(t *testing.T) {
	short := UUID16(0x180d)
	long := MustParseUUID("0000180d-0000-1000-8000-00805f9b34fb")
	if short.Equal(long) || long.Equal(short) {
		t.Fatal("Equal must retain its representation-sensitive comparison")
	}
	for _, u := range []UUID{short, long} {
		parsed, err := ParseUUID(u.String())
		if err != nil || !parsed.Equal(u) || parsed.Len() != u.Len() {
			t.Errorf("ParseUUID(%q) = %v, %v; want the original representation", u.String(), parsed, err)
		}
	}
	if _, err := ParseUUID("0000180d"); err == nil {
		t.Fatal("ParseUUID must continue rejecting unsupported 32-bit representations")
	}
}

func TestReverse(t *testing.T) {
	cases := []struct {
		fwd  []byte
		back []byte
	}{
		{fwd: []byte{0, 1}, back: []byte{1, 0}},
		{fwd: []byte{0, 1, 2}, back: []byte{2, 1, 0}},
		{fwd: []byte{0, 1, 2, 3}, back: []byte{3, 2, 1, 0}},
		{
			fwd:  []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			back: []byte{15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
	}

	for _, tt := range cases {
		got := reverse(tt.fwd)
		if !bytes.Equal(got, tt.back) {
			t.Errorf("reverse(%x): got %x want %x", tt.fwd, got, tt.back)
		}

		u := UUID{tt.fwd}
		got = reverse(u.b)
		if !bytes.Equal(got, tt.back) {
			t.Errorf("UUID.reverse(%x): got %x want %x", tt.fwd, got, tt.back)
		}
	}
}

func BenchmarkReverseBytes16(b *testing.B) {
	u := UUID{make([]byte, 2)}
	for i := 0; i < b.N; i++ {
		reverse(u.b)
	}
}

func BenchmarkReverseBytes128(b *testing.B) {
	u := UUID{make([]byte, 16)}
	for i := 0; i < b.N; i++ {
		reverse(u.b)
	}
}
