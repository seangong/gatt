package gatt

import (
	"runtime"
	"sync/atomic"
	"testing"
)

func TestRegistrationPublicHandlesConcurrentReads(t *testing.T) {
	first := NewService(UUID16(0xff00))
	characteristic := first.AddCharacteristic(UUID16(0xff01))
	characteristic.SetValue([]byte("value"))
	descriptor := characteristic.AddDescriptor(UUID16(0x2901))
	descriptor.SetValue([]byte("description"))
	second := NewService(UUID16(0xff10))
	d := &device{}
	if err := d.SetServices([]*Service{first, second}); err != nil {
		t.Fatal(err)
	}

	ready, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var reads atomic.Uint64
	go func() {
		defer close(done)
		close(ready)
		for {
			_ = first.Handle()
			_ = first.EndHandle()
			_ = characteristic.Handle()
			_ = characteristic.VHandle()
			_ = characteristic.EndHandle()
			_ = descriptor.Handle()
			reads.Add(1)
			select {
			case <-stop:
				return
			default:
			}
			runtime.Gosched()
		}
	}()
	<-ready
	t.Cleanup(func() { close(stop); <-done })
	for i := 0; i < 100; i++ {
		services := []*Service{first, second}
		if i%2 == 0 {
			services = []*Service{second, first}
		}
		if err := d.SetServices(services); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	if reads.Load() == 0 {
		t.Fatal("no public handles were read during registration")
	}
	if first.Handle() != 1 || first.EndHandle() != 4 || characteristic.Handle() != 2 ||
		characteristic.VHandle() != 3 || descriptor.Handle() != 4 {
		t.Fatalf("public handles do not report the last registration: service=%d..%d characteristic=%d/%d descriptor=%d",
			first.Handle(), first.EndHandle(), characteristic.Handle(), characteristic.VHandle(), descriptor.Handle())
	}
}

func TestDescriptorCopyKeepsIndependentHandle(t *testing.T) {
	original := NewDescriptor(UUID16(0x2901), 4, nil)
	copied := *original
	original.SetHandle(5)
	copied.SetHandle(6)
	if original.Handle() != 5 || copied.Handle() != 6 {
		t.Fatalf("descriptor copy shared its handle: original=%d copy=%d", original.Handle(), copied.Handle())
	}
}
