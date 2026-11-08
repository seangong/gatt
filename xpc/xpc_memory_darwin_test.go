//go:build gattxpc_test

package xpc

import (
	"reflect"
	"strings"
	"testing"
)

func TestCStringConversionReleasesTemporaryMemory(t *testing.T) {
	large := strings.Repeat("x", 1<<20)
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"string", large},
		{"dictionary-key", Dict{large: int64(42)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			convert := func() {
				native := goToXpc(tc.value)
				xpc_release(native)
			}
			for i := 0; i < 3; i++ {
				convert()
			}
			before := cHeapBytesForTest()
			for i := 0; i < 20; i++ {
				convert()
			}
			after := cHeapBytesForTest()
			// Allow allocator and SDK caches to vary without accepting one leaked
			// megabyte per conversion. This does not require exactly zero growth.
			if after > before && after-before > 2<<20 {
				t.Fatalf("released conversions retained %d bytes of C heap", after-before)
			}
		})
	}
}

func TestCStringConversionRetainsNativeValues(t *testing.T) {
	want := Dict{
		"text":   "persistent value",
		"nested": Array{"second value", Dict{"inner key": "inner value"}},
	}
	native := goToXpc(want)
	defer xpc_release(native)
	for i := 0; i < 100; i++ {
		churn := goToXpc(Dict{"another key": "overwrite temporary strings"})
		xpc_release(churn)
	}
	if got := xpcToGo(native); !reflect.DeepEqual(got, want) {
		t.Fatalf("native value after temporary buffers are freed = %v, want %v", got, want)
	}
}
