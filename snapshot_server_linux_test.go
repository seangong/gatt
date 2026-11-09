package gatt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCentralEmptyServicesDatabase(t *testing.T) {
	for _, mode := range []string{"unconfigured", "removed", "empty-set"} {
		t.Run(mode, func(t *testing.T) {
			d := &device{}
			if mode != "unconfigured" {
				s := NewService(UUID16(0xff00))
				s.AddCharacteristic(UUID16(0xff01)).SetValue([]byte("before"))
				if err := d.SetServices([]*Service{s}); err != nil {
					t.Fatal(err)
				}
				var err error
				if mode == "removed" {
					err = d.RemoveAllServices()
				} else {
					err = d.SetServices(nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			conn := &notificationTestConn{}
			c := newCentral(d.attrs, nil, conn)
			defer c.Close()
			if !c.handleAndRespond([]byte{attOpMtuReq, 64, 0}) || c.MTU() != 64 {
				t.Fatal("empty database prevented MTU exchange")
			}
			for _, request := range [][]byte{
				{attOpFindInfoReq, 1, 0, 255, 255},
				{attOpFindByTypeValueReq, 1, 0, 255, 255, 0, 0x28, 0, 0xff},
				{attOpReadByTypeReq, 1, 0, 255, 255, 0, 0x28},
				{attOpReadByGroupReq, 1, 0, 255, 255, 0, 0x28},
			} {
				if got, want := c.handleReq(request), attErrorRsp(request[0], 1, attEcodeAttrNotFound); !bytes.Equal(got, want) {
					t.Errorf("empty database response to %x = %x, want %x", request, got, want)
				}
			}
			for _, request := range [][]byte{
				{attOpReadReq, 1, 0},
				{attOpReadBlobReq, 1, 0, 0, 0},
				{attOpWriteReq, 1, 0, 'x'},
			} {
				if got, want := c.handleReq(request), attErrorRsp(request[0], 1, attEcodeInvalidHandle); !bytes.Equal(got, want) {
					t.Errorf("empty database response to %x = %x, want %x", request, got, want)
				}
			}
			if got := c.handleReq([]byte{attOpWriteCmd, 1, 0, 'x'}); got != nil || c.Context().Err() != nil {
				t.Fatalf("empty database command = %x, context=%v", got, c.Context().Err())
			}
		})
	}
}

func TestCentralServiceRegistrationSurvivesReconfiguration(t *testing.T) {
	first := NewService(UUID16(0xff00))
	ch := first.AddCharacteristic(UUID16(0xff01))
	ch.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) { _, _ = w.Write([]byte("old char")) })
	var writes []string
	ch.HandleWriteFunc(func(Request, []byte) byte { writes = append(writes, "old char"); return StatusSuccess })
	desc := ch.AddDescriptor(UUID16(0x2901))
	desc.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) { _, _ = w.Write([]byte("old desc")) })
	desc.HandleWriteFunc(func(Request, []byte) byte { writes = append(writes, "old desc"); return StatusSuccess })
	oldNotifiers, newNotifiers := make(chan Notifier, 2), make(chan Notifier, 2)
	ch.HandleNotifyFunc(func(_ Request, n Notifier) { oldNotifiers <- n })
	second := NewService(UUID16(0xff10))
	second.AddCharacteristic(UUID16(0xff11)).HandleNotifyFunc(func(Request, Notifier) {})
	d := &device{}
	if err := d.SetServices([]*Service{first, second}); err != nil {
		t.Fatal(err)
	}
	oldService, oldValue, oldDesc, oldCCCD := first.Handle(), ch.VHandle(), desc.Handle(), ch.Descriptor().Handle()
	conn := &notificationTestConn{}
	old := newCentral(d.attrs, nil, conn)
	defer old.Close()
	groupRequest := []byte{attOpReadByGroupReq, 1, 0, 255, 255, 0, 0x28}
	findRequest := append([]byte{attOpFindByTypeValueReq, 1, 0, 255, 255, 0, 0x28}, first.UUID().b...)
	groupBefore, findBefore := old.handleReq(groupRequest), old.handleReq(findRequest)

	ch.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) { _, _ = w.Write([]byte("new char")) })
	ch.HandleWriteFunc(func(Request, []byte) byte { writes = append(writes, "new char"); return StatusSuccess })
	desc.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) { _, _ = w.Write([]byte("new desc")) })
	desc.HandleWriteFunc(func(Request, []byte) byte { writes = append(writes, "new desc"); return StatusSuccess })
	// Configure the next registration with a different notification handler and
	// supported mode. The old connection must keep its complete registration.
	ch.nhandler = NotifyHandlerFunc(func(_ Request, n Notifier) { newNotifiers <- n })
	ch.props &^= CharIndicate
	if err := d.SetServices([]*Service{second, first}); err != nil {
		t.Fatal(err)
	}
	live := newCentral(d.attrs, nil, &notificationTestConn{})
	defer live.Close()
	if first.Handle() == oldService || ch.VHandle() == oldValue || desc.Handle() == oldDesc || ch.Descriptor().Handle() == oldCCCD {
		t.Fatal("public handles did not update to the new registration")
	}
	if a, ok := d.attrs.At(ch.VHandle()); !ok || !a.typ.Equal(ch.UUID()) || first.EndHandle() != 0xffff {
		t.Fatal("public handles no longer describe the current registration")
	}
	for _, query := range []struct{ request, before []byte }{{groupRequest, groupBefore}, {findRequest, findBefore}} {
		if got := old.handleReq(query.request); !bytes.Equal(got, query.before) {
			t.Errorf("old discovery changed after reconfiguration: %x, want %x", got, query.before)
		}
		if got := live.handleReq(query.request); bytes.Equal(got, query.before) {
			t.Errorf("new discovery retained old registration: %x", got)
		}
	}
	for _, registration := range []struct {
		central     *central
		value, desc uint16
		prefix      string
	}{{old, oldValue, oldDesc, "old"}, {live, ch.VHandle(), desc.Handle(), "new"}} {
		for _, target := range []struct {
			handle uint16
			uuid   UUID
			name   string
		}{{registration.value, ch.UUID(), "char"}, {registration.desc, desc.UUID(), "desc"}} {
			for _, op := range []byte{attOpReadReq, attOpReadBlobReq, attOpReadByTypeReq} {
				request := []byte{op, byte(target.handle), byte(target.handle >> 8)}
				want := append([]byte{attRspFor[op]}, []byte(registration.prefix+" "+target.name)...)
				if op == attOpReadBlobReq {
					request = append(request, 0, 0)
				} else if op == attOpReadByTypeReq {
					request = append(request, byte(target.handle), byte(target.handle>>8))
					request = append(request, target.uuid.b...)
					want = append([]byte{attOpReadByTypeRsp, byte(len(want) + 1), byte(target.handle), byte(target.handle >> 8)}, want[1:]...)
				}
				if got := registration.central.handleReq(request); !bytes.Equal(got, want) {
					t.Errorf("%s %s read %02x = %x, want %x", registration.prefix, target.name, op, got, want)
				}
			}
			if got := registration.central.handleReq([]byte{attOpWriteReq, byte(target.handle), byte(target.handle >> 8), 'x'}); !bytes.Equal(got, []byte{attOpWriteRsp}) {
				t.Fatalf("%s %s write = %x", registration.prefix, target.name, got)
			}
		}
	}
	if got := fmt.Sprint(writes); got != "[old char old desc new char new desc]" {
		t.Fatalf("registration write handlers = %s", got)
	}
	if got := old.handleReq([]byte{attOpWriteReq, byte(oldCCCD), byte(oldCCCD >> 8), 2, 0}); !bytes.Equal(got, []byte{attOpWriteRsp}) {
		t.Fatalf("old registration lost indication support: %x", got)
	}
	var oldNotifier Notifier
	select {
	case oldNotifier = <-oldNotifiers:
	case <-time.After(time.Second):
		t.Fatal("old registration did not use its notification handler")
	}
	if _, err := oldNotifier.Write([]byte("old value")); err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(conn.packets[0][1:3]); got != oldValue {
		t.Fatalf("old notification handle = %d, want %d", got, oldValue)
	}
	old.handleReq([]byte{attOpHandleCnf})
	cccd := ch.Descriptor().Handle()
	if got, want := live.handleReq([]byte{attOpWriteReq, byte(cccd), byte(cccd >> 8), 2, 0}), attErrorRsp(attOpWriteReq, cccd, attEcodeValueNotAllowed); !bytes.Equal(got, want) {
		t.Fatalf("new registration accepted unsupported indication: %x", got)
	}
	if got := live.handleReq([]byte{attOpWriteReq, byte(cccd), byte(cccd >> 8), 1, 0}); !bytes.Equal(got, []byte{attOpWriteRsp}) {
		t.Fatalf("new registration subscription = %x", got)
	}
	var newNotifier Notifier
	select {
	case newNotifier = <-newNotifiers:
	case <-time.After(time.Second):
		t.Fatal("new registration did not use its notification handler")
	}
	if err := d.RemoveAllServices(); err != nil {
		t.Fatal(err)
	}
	if oldNotifier.Done() || newNotifier.Done() || old.Context().Err() != nil || live.Context().Err() != nil {
		t.Fatal("removing services retired an existing connection or notifier")
	}
}

func TestCentralConcurrentServiceReconfiguration(t *testing.T) {
	first, second := NewService(UUID16(0xff00)), NewService(UUID16(0xff10))
	ch := first.AddCharacteristic(UUID16(0xff01))
	notifiers := make(chan Notifier, 1)
	ch.HandleNotifyFunc(func(_ Request, n Notifier) { notifiers <- n })
	second.AddCharacteristic(UUID16(0xff11)).SetValue([]byte("second"))
	d := &device{}
	if err := d.SetServices([]*Service{first, second}); err != nil {
		t.Fatal(err)
	}
	value, cccd := ch.VHandle(), ch.Descriptor().Handle()
	conn := &notificationTestConn{}
	c := newCentral(d.attrs, nil, conn)
	defer c.Close()
	c.handleReq([]byte{attOpWriteReq, byte(cccd), byte(cccd >> 8), 1, 0})
	n := <-notifiers
	query := []byte{attOpReadByGroupReq, 1, 0, 255, 255, 0, 0x28}
	want := c.handleReq(query)
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 100 {
			services := []*Service{first, second}
			if i%2 == 0 {
				services = []*Service{second, first}
			}
			if err := d.SetServices(services); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for range 100 {
		if got := c.handleReq(query); !bytes.Equal(got, want) {
			t.Errorf("concurrent reconfiguration changed old discovery: %x, want %x", got, want)
			break
		}
		if _, err := n.Write([]byte("old")); err != nil {
			t.Error(err)
			break
		}
	}
	wg.Wait()
	for _, packet := range conn.packets {
		if h := binary.LittleEndian.Uint16(packet[1:3]); h != value {
			t.Fatalf("concurrent reconfiguration changed old notification handle: %d, want %d", h, value)
		}
	}
}

func TestCentralUnsupportedCommandsHaveNoResponse(t *testing.T) {
	s := NewService(UUID16(0xff00))
	ch := s.AddCharacteristic(UUID16(0xff01))
	ch.SetValue([]byte("ready"))
	writes := 0
	ch.HandleWriteFunc(func(Request, []byte) byte { writes++; return StatusSuccess })
	conn := &notificationTestConn{}
	c := newCentral(generateAttributes([]*Service{s}, 1), nil, conn)
	defer c.Close()
	for _, op := range []byte{attOpSignedWriteCmd, 0xff, attOpReadReq | 0x40} {
		for _, size := range []int{1, 2, 15, 24} {
			request := make([]byte, size)
			request[0] = op
			if len(request) >= 3 {
				binary.LittleEndian.PutUint16(request[1:3], ch.VHandle())
			}
			if !c.handleAndRespond(request) || len(conn.packets) != 0 || writes != 0 {
				t.Fatalf("unsupported command %x sent a response or invoked business code", request)
			}
		}
	}
	if !c.handleAndRespond([]byte{attOpReadReq, byte(ch.VHandle()), byte(ch.VHandle() >> 8)}) || !bytes.Equal(conn.packets[0], append([]byte{attOpReadRsp}, []byte("ready")...)) {
		t.Fatal("unsupported command interrupted the following read")
	}
	if got, want := c.handleReq([]byte{0x3f}), attErrorRsp(0x3f, 0, attEcodeReqNotSupp); !bytes.Equal(got, want) {
		t.Fatalf("unsupported request response = %x, want %x", got, want)
	}
	if got := c.handleReq([]byte{attOpWriteCmd, byte(ch.VHandle()), byte(ch.VHandle() >> 8), 'x'}); got != nil || writes != 1 {
		t.Fatalf("supported write command response=%x, business calls=%d", got, writes)
	}
}
