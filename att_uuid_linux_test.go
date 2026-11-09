package gatt

import (
	"bytes"
	"testing"
)

func TestReadByTypeUUIDAliases(t *testing.T) {
	short := UUID16(0x2a19)
	long := MustParseUUID("00002a19-0000-1000-8000-00805f9b34fb")
	custom := MustParseUUID("12345678-1234-5678-90ab-cdef12345678")
	nonBase := MustParseUUID("00002a19-0000-1000-8000-00805f9b34fa")
	for _, tt := range []struct {
		name              string
		registered, query UUID
		match             bool
	}{
		{"short_to_short", short, short, true},
		{"long_to_short", short, long, true},
		{"short_to_long", long, short, true},
		{"long_to_long", long, long, true},
		{"custom128", custom, custom, true},
		{"different_short", short, UUID16(0x2a1a), false},
		{"different_long", short, MustParseUUID("00002a1a-0000-1000-8000-00805f9b34fb"), false},
		{"nonzero_high_bits", short, MustParseUUID("00012a19-0000-1000-8000-00805f9b34fb"), false},
		{"nonbase_query", short, nonBase, false},
		{"nonbase_attribute", nonBase, short, false},
		{"different_custom128", custom, nonBase, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewService(UUID16(0x180f))
			s.AddCharacteristic(tt.registered).SetValue([]byte{'v'})
			c := newCentral(generateAttributes([]*Service{s}, 1), nil, &notificationTestConn{})
			defer c.Close()
			req := append([]byte{attOpReadByTypeReq, 1, 0, 0xff, 0xff}, tt.query.b...)
			want := attErrorRsp(attOpReadByTypeReq, 1, attEcodeAttrNotFound)
			if tt.match {
				want = []byte{attOpReadByTypeRsp, 3, 3, 0, 'v'}
			}
			if got := c.handleReq(req); !bytes.Equal(got, want) {
				t.Fatalf("query %s for registered %s = % X, want % X", tt.query, tt.registered, got, want)
			}
		})
	}
}

func TestReadByGroupUUIDAliases(t *testing.T) {
	for _, tt := range []struct {
		name  string
		query UUID
		match bool
	}{
		{"short_primary", UUID16(0x2800), true},
		{"long_primary", MustParseUUID("00002800-0000-1000-8000-00805f9b34fb"), true},
		{"unsupported_short", UUID16(0x2801), false},
		{"unsupported_long", MustParseUUID("00002801-0000-1000-8000-00805f9b34fb"), false},
		{"nonbase_primary", MustParseUUID("00002800-0000-1000-8000-00805f9b34fa"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCentral(generateAttributes([]*Service{NewService(UUID16(0x180f))}, 1), nil, &notificationTestConn{})
			defer c.Close()
			req := append([]byte{attOpReadByGroupReq, 1, 0, 0xff, 0xff}, tt.query.b...)
			want := attErrorRsp(attOpReadByGroupReq, 1, attEcodeUnsuppGrpType)
			if tt.match {
				want = []byte{attOpReadByGroupRsp, 6, 1, 0, 0xff, 0xff, 0x0f, 0x18}
			}
			if got := c.handleReq(req); !bytes.Equal(got, want) {
				t.Fatalf("query %s = % X, want % X", tt.query, got, want)
			}
		})
	}
}

func TestFindByTypeValuePreservesBinaryUUIDValueMatching(t *testing.T) {
	short := UUID16(0x180f)
	long := MustParseUUID("0000180f-0000-1000-8000-00805f9b34fb")
	for _, tt := range []struct {
		name              string
		registered, query UUID
		match             bool
	}{
		{"short_value", short, short, true},
		{"long_value", long, long, true},
		{"long_query_for_short_value", short, long, false},
		{"short_query_for_long_value", long, short, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newCentral(generateAttributes([]*Service{NewService(tt.registered)}, 1), nil, &notificationTestConn{})
			defer c.Close()
			req := append([]byte{attOpFindByTypeValueReq, 1, 0, 0xff, 0xff, 0, 0x28}, tt.query.b...)
			want := attErrorRsp(attOpFindByTypeValueReq, 1, attEcodeAttrNotFound)
			if tt.match {
				want = []byte{attOpFindByTypeValueRsp, 1, 0, 0xff, 0xff}
			}
			if got := c.handleReq(req); !bytes.Equal(got, want) {
				t.Fatalf("query %s for service value %s = % X, want % X", tt.query, tt.registered, got, want)
			}
		})
	}
}
