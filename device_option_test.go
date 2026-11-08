package gatt

import (
	"errors"
	"io/fs"
	"reflect"
	"strconv"
	"testing"
)

func TestOptionStopsAtFirstError(t *testing.T) {
	wantErr := &fs.PathError{Op: "configure", Path: "device", Err: errors.New("invalid option")}
	for _, failed := range []int{0, 1, 2} {
		t.Run(strconv.Itoa(failed+1), func(t *testing.T) {
			d := &device{}
			var calls []int
			var applied int
			opts := make([]Option, 3)
			for i := range opts {
				opts[i] = func(got Device) error {
					if got != d {
						t.Fatalf("option received %p, want %p", got, d)
					}
					calls = append(calls, i)
					if i == failed {
						return wantErr
					}
					applied++
					return nil
				}
			}
			err := d.Option(opts...)
			if !errors.Is(err, wantErr) {
				t.Fatalf("Option error = %v, want %v", err, wantErr)
			}
			var gotErr *fs.PathError
			if !errors.As(err, &gotErr) || gotErr != wantErr {
				t.Fatalf("Option error = %v, want original typed error", err)
			}
			if !reflect.DeepEqual(calls, []int{0, 1, 2}[:failed+1]) {
				t.Fatalf("option calls = %v, want prefix ending at %d", calls, failed)
			}
			if applied != failed {
				t.Fatalf("applied options = %d, want %d", applied, failed)
			}
		})
	}
}

func TestNewDeviceRejectsOptionError(t *testing.T) {
	wantErr := errors.New("invalid device option")
	var applied, skipped bool
	d, err := NewDevice(
		func(Device) error { applied = true; return nil },
		func(Device) error { return wantErr },
		func(Device) error { skipped = true; return nil },
	)
	if d != nil || !errors.Is(err, wantErr) {
		t.Fatalf("NewDevice = (%v, %v), want (nil, %v)", d, err, wantErr)
	}
	if !applied || skipped {
		t.Fatalf("NewDevice applied = %t, skipped option called = %t", applied, skipped)
	}
}

func TestOptionPreservesSuccessfulOrder(t *testing.T) {
	d := &device{}
	var calls []int
	if err := d.Option(
		func(Device) error { calls = append(calls, 1); return nil },
		func(Device) error { calls = append(calls, 2); return nil },
	); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []int{1, 2}) {
		t.Fatalf("option calls = %v, want [1 2]", calls)
	}
	if err := d.Option(); err != nil {
		t.Fatalf("empty Option error = %v", err)
	}
}
