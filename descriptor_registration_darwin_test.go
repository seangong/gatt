//go:build gattxpc_test

package gatt

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

func darwinDynamicDescriptorService(configure func(*Descriptor), uuid UUID) *Service {
	s := NewService(UUID16(0xff00))
	c := s.AddCharacteristic(UUID16(0xff01))
	c.SetValue([]byte("characteristic"))
	configure(c.AddDescriptor(uuid))
	return s
}

func TestDarwinAddServiceRejectsDynamicDescriptors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		uuid      UUID
		configure func(*Descriptor)
	}{
		{"read", UUID16(0x2901), func(d *Descriptor) { d.HandleReadFunc(func(ResponseWriter, *ReadRequest) {}) }},
		{"write", UUID16(0x2901), func(d *Descriptor) { d.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess }) }},
		{"static with write", UUID16(0x2901), func(d *Descriptor) {
			d.SetValue([]byte("static"))
			d.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess })
		}},
		{"custom cccd", attrClientCharacteristicConfigUUID, func(d *Descriptor) { d.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess }) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commands := make(chan int, 4)
			barrier := make(chan struct{}, 1)
			d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				switch id {
				case 10:
					commands <- id
					return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
				case 12:
					commands <- id
				case 999:
					barrier <- struct{}{}
				}
				return 0, nil
			})
			if err := d.AddService(darwinServiceForTest(nil)); err != nil {
				t.Fatal(err)
			}
			receiveDarwin(t, commands)
			previousID := d.attrN
			previous := d.attrs[previousID-1]
			err := d.AddService(darwinDynamicDescriptorService(tc.configure, tc.uuid))
			if !errors.Is(err, errors.ErrUnsupported) {
				t.Fatalf("dynamic descriptor registration = %v, want errors.ErrUnsupported", err)
			}
			if !strings.Contains(err.Error(), "macOS") || !strings.Contains(err.Error(), tc.uuid.String()) {
				t.Fatalf("registration error lacks operation or descriptor UUID: %v", err)
			}
			if d.attrN != previousID || len(d.attrs) != 1 || d.attrs[previousID-1] != previous {
				t.Fatal("local rejection changed the existing attributes or allocated native IDs")
			}
			if err := d.sendCmd(999, xpc.Dict{}); err != nil {
				t.Fatal(err)
			}
			receiveDarwin(t, barrier)
			select {
			case id := <-commands:
				t.Fatalf("local rejection sent native service command %d", id)
			default:
			}
		})
	}
}

func TestDarwinSetServicesPreflightsBeforeRemovingExistingServices(t *testing.T) {
	commands, added, notifications := make(chan int, 4), make(chan int, 2), make(chan int, 1)
	barrier, notifiers := make(chan struct{}, 1), make(chan Notifier, 1)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 10:
			commands <- id
			added <- darwinServiceAttributeID(args)
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		case 12:
			commands <- id
		case 15:
			notifications <- args.MustGetInt("kCBMsgArgAttributeID")
		case 999:
			barrier <- struct{}{}
		}
		return 0, nil
	})
	if err := d.AddService(darwinServiceForTest(notifiers)); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, commands)
	id := receiveDarwin(t, added)
	darwinSubscribeForTest(d, id)
	n := receiveDarwin(t, notifiers)
	previous, previousID := d.attrs[id], d.attrN
	invalid := darwinDynamicDescriptorService(func(d *Descriptor) {
		d.HandleReadFunc(func(ResponseWriter, *ReadRequest) {})
	}, UUID16(0x2901))
	err := d.SetServices([]*Service{darwinServiceForTest(nil), invalid})
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("SetServices with dynamic descriptor = %v, want errors.ErrUnsupported", err)
	}
	if d.attrN != previousID || len(d.attrs) != 1 || d.attrs[id] != previous || n.Done() {
		t.Fatal("preflight rejection retired the existing service or its subscription")
	}
	if err := d.sendCmd(999, xpc.Dict{}); err != nil {
		t.Fatal(err)
	}
	receiveDarwin(t, barrier)
	select {
	case command := <-commands:
		t.Fatalf("preflight rejection sent native service command %d", command)
	default:
	}
	if _, err := n.Write([]byte("still active")); err != nil {
		t.Fatal(err)
	}
	if got := receiveDarwin(t, notifications); got != id {
		t.Fatalf("existing subscription now targets %d, want %d", got, id)
	}
}

func TestDarwinStaticDescriptorsAndAutomaticCCCDStillRegister(t *testing.T) {
	registered := make(chan xpc.Dict, 1)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		if id == 10 {
			registered <- args
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	s := NewService(UUID16(0xff00))
	c := s.AddCharacteristic(UUID16(0xff01))
	c.HandleReadFunc(func(ResponseWriter, *ReadRequest) {})
	c.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess })
	c.HandleNotifyFunc(func(Request, Notifier) {})
	c.AddDescriptor(UUID16(0x2901)).SetValue([]byte("description"))
	if err := d.AddService(s); err != nil {
		t.Fatal(err)
	}
	args := receiveDarwin(t, registered)
	native := args.MustGetArray("kCBMsgArgCharacteristics")[0].(xpc.Dict)
	descriptors := native.MustGetArray("kCBMsgArgDescriptors")
	if len(descriptors) != 1 {
		t.Fatalf("native descriptor count=%d, want only the static descriptor", len(descriptors))
	}
	desc := descriptors[0].(xpc.Dict)
	if !bytes.Equal(desc.GetBytes("kCBMsgArgUUID", nil), reverse(UUID16(0x2901).b)) ||
		!bytes.Equal(desc.GetBytes("kCBMsgArgData", nil), []byte("description")) {
		t.Fatalf("static descriptor changed: %v", desc)
	}
	if len(d.attrs) != 1 || d.attrs[darwinServiceAttributeID(args)].pvt != c || c.Descriptor() == nil {
		t.Fatal("characteristic callbacks or automatic CCCD were not preserved")
	}
}

func TestDarwinDescriptorPreflightPreservesSkippedServicesAndCloseCause(t *testing.T) {
	for _, uuid := range []UUID{attrGAPUUID, attrGATTUUID} {
		t.Run(uuid.String(), func(t *testing.T) {
			commands := make(chan int, 4)
			barrier := make(chan struct{}, 1)
			d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				if id == 999 {
					barrier <- struct{}{}
				} else {
					commands <- id
				}
				return 0, nil
			})
			s := NewService(uuid)
			s.AddCharacteristic(UUID16(0xff01)).AddDescriptor(UUID16(0x2901)).HandleReadFunc(func(ResponseWriter, *ReadRequest) {})
			previousID := d.attrN
			if err := d.AddService(s); err != nil {
				t.Fatal(err)
			}
			if d.attrN != previousID || len(d.attrs) != 0 {
				t.Fatal("skipped service allocated attributes")
			}
			if err := d.SetServices([]*Service{s}); err != nil {
				t.Fatal(err)
			}
			if err := d.sendCmd(999, xpc.Dict{}); err != nil {
				t.Fatal(err)
			}
			receiveDarwin(t, barrier)
			if command := receiveDarwin(t, commands); command != 12 {
				t.Fatalf("SetServices native command=%d, want removal only", command)
			}
			select {
			case command := <-commands:
				t.Fatalf("skipped service sent additional native command %d", command)
			default:
			}
			cause := errors.New("session ended")
			d.cancel(cause)
			for _, operation := range []func() error{
				func() error { return d.AddService(s) },
				func() error { return d.SetServices([]*Service{s}) },
				func() error {
					return d.SetServices([]*Service{darwinDynamicDescriptorService(func(d *Descriptor) {
						d.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess })
					}, UUID16(0x2901))})
				},
			} {
				if err := operation(); !errors.Is(err, cause) {
					t.Fatalf("closed registration error=%v, want original session cause", err)
				}
			}
		})
	}
}

func TestDarwinDescriptorPreflightDoesNotWaitForNativeRegistration(t *testing.T) {
	entered := make(chan struct{}, 1)
	d, _ := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 10 {
			entered <- struct{}{}
		}
		return 0, nil // Keep the first native AddService response pending.
	})
	first := make(chan error, 1)
	go func() { first <- d.AddService(darwinServiceForTest(nil)) }()
	receiveDarwin(t, entered)
	invalid := darwinDynamicDescriptorService(func(d *Descriptor) {
		d.HandleWriteFunc(func(Request, []byte) byte { return StatusSuccess })
	}, UUID16(0x2901))
	result := make(chan error, 1)
	go func() { result <- d.AddService(invalid) }()
	select {
	case err := <-result:
		if !errors.Is(err, errors.ErrUnsupported) {
			t.Fatalf("local preflight error=%v, want errors.ErrUnsupported", err)
		}
	case <-time.After(time.Second):
		t.Fatal("local descriptor preflight waited for a native registration response")
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(18), "kCBMsgArgs": xpc.Dict{"kCBMsgArgResult": int64(0)}}, nil)
	if err := receiveDarwin(t, first); err != nil {
		t.Fatal(err)
	}
}
