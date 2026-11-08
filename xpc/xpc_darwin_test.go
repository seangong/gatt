package xpc

import (
	"bytes"
	"reflect"
	"testing"
)

func CheckUUID(t *testing.T, v interface{}) UUID {
	if uuid, ok := v.(UUID); ok {
		return uuid
	} else {
		t.Errorf("not a UUID: %#v\n", v)
		return uuid
	}
}

func TestConvertUUID(t *testing.T) {
	uuid := MakeUUID("00112233445566778899aabbccddeeff")

	xv := goToXpc(uuid)
	v := xpcToGo(xv)

	xpc_release(xv)

	uuid2 := CheckUUID(t, v)

	if !bytes.Equal(uuid, uuid2) {
		t.Errorf("expected %#v got %#v\n", uuid, uuid2)
	}
}

func TestConvertSlice(t *testing.T) {
	arr := []string{"one", "two", "three"}

	xv := goToXpc(arr)
	v := xpcToGo(xv)

	xpc_release(xv)

	if arr2, ok := v.(Array); !ok {
		t.Errorf("not an array: %#v\n", v)
	} else if len(arr) != len(arr2) {
		t.Errorf("expected %#v got %#v\n", arr, arr2)
	} else {
		for i := range arr {
			if arr[i] != arr2[i] {
				t.Errorf("expected array[%d]: %#v got %#v\n", i, arr[i], arr2[i])
			}
		}
	}
}

func TestConvertSliceUUID(t *testing.T) {
	arr := []UUID{MakeUUID("0000000000000000"), MakeUUID("1111111111111111"), MakeUUID("2222222222222222")}

	xv := goToXpc(arr)
	v := xpcToGo(xv)

	xpc_release(xv)

	if arr2, ok := v.(Array); !ok {
		t.Errorf("not an array: %#v\n", v)
	} else if len(arr) != len(arr2) {
		t.Errorf("expected %#v got %#v\n", arr, arr2)
	} else {
		for i := range arr {
			uuid1 := CheckUUID(t, arr[i])
			uuid2 := CheckUUID(t, arr2[i])

			if !bytes.Equal(uuid1, uuid2) {
				t.Errorf("expected array[%d]: %#v got %#v\n", i, arr[i], arr2[i])
			}
		}
	}
}

func TestConvertMap(t *testing.T) {
	d := Dict{
		"number": int64(42),
		"text":   "hello gopher",
		"uuid":   MakeUUID("aabbccddeeff00112233445566778899"),
	}

	xv := goToXpc(d)
	v := xpcToGo(xv)

	xpc_release(xv)

	if d2, ok := v.(Dict); !ok {
		t.Errorf("not a map: %#v", v)
	} else if len(d) != len(d2) {
		t.Errorf("expected %#v got %#v\n", d, d2)
	} else {
		fail := false

		for k, v := range d {
			if !reflect.DeepEqual(v, d2[k]) {
				t.Logf("expected map[%s]: %#v got %#v\n", k, v, d2[k])
				fail = true
			}
		}

		if fail {
			t.Error("test failed")
		}
	}
}

func TestConvertContainers(t *testing.T) {
	uuid := MakeUUID("00112233445566778899aabbccddeeff")
	for _, tt := range []struct {
		name  string
		value interface{}
	}{
		{"empty array", Array{}},
		{"empty dictionary", Dict{}},
		{"single array element", Array{int64(42)}},
		{"single dictionary value", Dict{"uuid": uuid}},
		{"nested array", Array{
			uuid,
			Dict{
				"entries": Array{"value", int64(42), []byte{1, 2, 3}},
				"empty":   Dict{},
			},
		}},
		{"nested dictionary", Dict{
			"uuid": uuid,
			"entries": Array{
				Dict{"bytes": []byte{0, 1, 255}, "name": "nested", "number": int64(7)},
				Array{uuid, Array{}, Dict{}},
			},
			"empty": Array{},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := func() interface{} {
				xv := goToXpc(tt.value)
				defer xpc_release(xv)
				return xpcToGo(xv)
			}()
			// Check types, keys and values after releasing all native objects.
			if !reflect.DeepEqual(got, tt.value) {
				t.Fatalf("expected %#v got %#v", tt.value, got)
			}
		})
	}
}

func TestGetUUIDBytes(t *testing.T) {
	for _, input := range [][]byte{nil, {}, {1}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}} {
		got := GetUUID(input)
		if got == nil || !bytes.Equal(got, input) {
			t.Fatalf("GetUUID(%v) = %v", input, got)
		}
		if len(input) != 0 {
			input[0] ^= 0xff
			if got[0] == input[0] {
				t.Fatal("GetUUID returned a slice aliasing its []byte input")
			}
		}
	}
	if got := GetUUID(nil); got == nil || len(got) != 0 {
		t.Fatalf("GetUUID(nil) = %#v", got)
	}
	uuid := UUID{1, 2, 3}
	if got := GetUUID(uuid); &got[0] != &uuid[0] {
		t.Fatal("GetUUID changed existing UUID identity")
	}
}
