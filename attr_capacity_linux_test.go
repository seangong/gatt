package gatt

import (
	"errors"
	"fmt"
	"io"
	"log"
	"testing"
)

func TestServiceRegistrationHandleCapacity(t *testing.T) {
	previous := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(previous)

	s := NewService(UUID16(0xfff0))
	chars := make([]*Characteristic, 32767)
	for i := range chars {
		u := MustParseUUID(fmt.Sprintf("12345678-1234-5678-9abc-%012x", i))
		chars[i] = NewCharacteristic(u, s, CharRead, 0, 0)
		chars[i].SetValue([]byte{1})
	}
	s.SetCharacteristics(chars)
	d := &device{}
	if err := d.SetServices([]*Service{s}); err != nil {
		t.Fatalf("maximum valid registration: %v", err)
	}
	if len(d.attrs.aa) != 65535 || chars[len(chars)-1].VHandle() != 0xffff {
		t.Fatal("maximum valid table did not use all valid handles")
	}
	previousTable := d.attrs
	last := chars[len(chars)-1]
	descriptor := last.AddDescriptor(UUID16(0x2901))
	descriptor.SetValue([]byte{1})
	if err := d.SetServices([]*Service{s}); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("oversized replacement = %v, want invalid length", err)
	}
	if d.attrs != previousTable || s.Handle() != 1 || s.EndHandle() != 0xffff || last.VHandle() != 0xffff || descriptor.Handle() != 0 {
		t.Fatal("rejected replacement changed the published table or handles")
	}

	old := NewService(UUID16(0x180f))
	old.AddCharacteristic(UUID16(0x2a19)).SetValue([]byte{80})
	if err := d.SetServices([]*Service{old}); err != nil {
		t.Fatal(err)
	}
	previousTable = d.attrs
	previousHandle := s.Handle()
	if err := d.AddService(s); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("oversized addition = %v, want invalid length", err)
	}
	if d.attrs != previousTable || len(d.svcs) != 1 || d.svcs[0] != old || old.EndHandle() != 0xffff || s.Handle() != previousHandle {
		t.Fatal("rejected addition changed the published table or handles")
	}
	if err := d.SetServices(nil); err != nil || len(d.attrs.aa) != 0 {
		t.Fatalf("empty registration after rejection = %v", err)
	}
}
