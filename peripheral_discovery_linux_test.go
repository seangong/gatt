package gatt

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

type discoveryExchange struct {
	request  []byte
	response []byte
}

func runDiscoveryForTest(t *testing.T, c *peripheralTestConn, call func() error, exchanges ...discoveryExchange) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- call() }()
	for _, exchange := range exchanges {
		if request := peripheralWriteForTest(t, c); !bytes.Equal(request, exchange.request) {
			t.Fatalf("discovery request = %x, want %x", request, exchange.request)
		}
		c.reads <- peripheralTestRead{b: exchange.response}
		select {
		case <-c.readPackets:
		case <-time.After(time.Second):
			t.Fatal("discovery response was not read")
		}
	}
	select {
	case err := <-result:
		return err
	case request := <-c.writes:
		t.Fatalf("unexpected discovery request: %x", request)
	case <-time.After(time.Second):
		t.Fatal("discovery did not finish")
	}
	return nil
}

func serviceDiscoveryRequest(start uint16) []byte {
	return []byte{attOpReadByGroupReq, byte(start), byte(start >> 8), 0xff, 0xff, 0x00, 0x28}
}

func characteristicDiscoveryRequest(start, end uint16) []byte {
	return []byte{attOpReadByTypeReq, byte(start), byte(start >> 8), byte(end), byte(end >> 8), 0x03, 0x28}
}

func descriptorDiscoveryRequest(start, end uint16) []byte {
	return []byte{attOpFindInfoReq, byte(start), byte(start >> 8), byte(end), byte(end >> 8)}
}

func TestPeripheralDiscoveryNoDescriptors(t *testing.T) {
	for _, end := range []uint16{3, 0xffff} {
		t.Run(fmt.Sprintf("end_%04x", end), func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			s := &Service{h: 1, endh: end}
			char := &Characteristic{svc: s, h: 2, vh: end, endh: end}
			err := runDiscoveryForTest(t, c, func() error {
				ds, err := p.DiscoverDescriptors(nil, char)
				if len(ds) != 0 {
					return fmt.Errorf("descriptors = %v", ds)
				}
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPeripheralDiscoveryRejectsEmptySuccess(t *testing.T) {
	for _, kind := range []string{"services", "characteristics", "descriptors"} {
		t.Run(kind, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			s := &Service{h: 1, endh: 5}
			char := &Characteristic{svc: s, h: 2, vh: 3, endh: 5}
			var call func() error
			var exchange discoveryExchange
			switch kind {
			case "services":
				call = func() error { _, err := p.DiscoverServices(nil); return err }
				exchange = discoveryExchange{serviceDiscoveryRequest(1), []byte{attOpReadByGroupRsp, 6}}
			case "characteristics":
				call = func() error { _, err := p.DiscoverCharacteristics(nil, s); return err }
				exchange = discoveryExchange{characteristicDiscoveryRequest(1, 5), []byte{attOpReadByTypeRsp, 7}}
			case "descriptors":
				call = func() error { _, err := p.DiscoverDescriptors(nil, char); return err }
				exchange = discoveryExchange{descriptorDiscoveryRequest(4, 5), []byte{attOpFindInfoRsp, 1}}
			}
			if err := runDiscoveryForTest(t, c, call, exchange); !errors.Is(err, ErrInvalidLength) {
				t.Fatalf("empty success = %v, want invalid length", err)
			}
		})
	}
}

func TestPeripheralDiscoveryRepeatedSnapshotPreservesSubscription(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	var originalService *Service
	var originalCharacteristic *Characteristic
	var originalDescriptor *Descriptor
	notified := make(chan *Characteristic, 1)
	for round := 0; round < 2; round++ {
		var s *Service
		err := runDiscoveryForTest(t, c, func() error {
			ss, err := p.DiscoverServices(nil)
			if err == nil && len(ss) != 1 {
				return fmt.Errorf("services = %d, want 1", len(ss))
			}
			if err == nil {
				s = ss[0]
			}
			return err
		}, discoveryExchange{serviceDiscoveryRequest(1), []byte{attOpReadByGroupRsp, 6, 1, 0, 5, 0, 0x0f, 0x18}},
			discoveryExchange{serviceDiscoveryRequest(6), attErrorRsp(attOpReadByGroupReq, 6, attEcodeAttrNotFound)})
		if err != nil {
			t.Fatal(err)
		}
		var char *Characteristic
		err = runDiscoveryForTest(t, c, func() error {
			cs, err := p.DiscoverCharacteristics(nil, s)
			if err == nil && len(cs) != 1 {
				return fmt.Errorf("characteristics = %d, want 1", len(cs))
			}
			if err == nil {
				char = cs[0]
			}
			return err
		}, discoveryExchange{characteristicDiscoveryRequest(1, 5), []byte{attOpReadByTypeRsp, 7, 2, 0, byte(CharNotify), 3, 0, 0x19, 0x2a}},
			discoveryExchange{characteristicDiscoveryRequest(3, 5), attErrorRsp(attOpReadByTypeReq, 3, attEcodeAttrNotFound)})
		if err != nil {
			t.Fatal(err)
		}
		err = runDiscoveryForTest(t, c, func() error {
			ds, err := p.DiscoverDescriptors(nil, char)
			if err == nil && len(ds) != 1 {
				return fmt.Errorf("descriptors = %d, want 1", len(ds))
			}
			return err
		}, discoveryExchange{descriptorDiscoveryRequest(4, 5), []byte{attOpFindInfoRsp, 1, 4, 0, 2, 0x29}},
			discoveryExchange{descriptorDiscoveryRequest(5, 5), attErrorRsp(attOpFindInfoReq, 5, attEcodeAttrNotFound)})
		if err != nil {
			t.Fatal(err)
		}
		if round == 0 {
			originalService, originalCharacteristic, originalDescriptor = s, char, char.cccd
			err = runDiscoveryForTest(t, c, func() error {
				return p.SetNotifyValue(char, func(c *Characteristic, _ []byte, err error) {
					if err == nil {
						notified <- c
					}
				})
			}, discoveryExchange{[]byte{attOpWriteReq, 4, 0, 1, 0}, []byte{attOpWriteRsp}})
			if err != nil {
				t.Fatal(err)
			}
		} else if s != originalService || char != originalCharacteristic || char.cccd != originalDescriptor || char.descs[0] != originalDescriptor {
			t.Fatal("repeated discovery replaced cached objects")
		}
	}
	c.reads <- peripheralTestRead{b: []byte{attOpHandleNotify, 3, 0, 42}}
	select {
	case got := <-notified:
		if got != originalCharacteristic {
			t.Fatal("notification callback received a replacement characteristic")
		}
	case <-time.After(time.Second):
		t.Fatal("notification subscription was lost")
	}
}

func TestPeripheralDiscoveryFailureDoesNotPublishPartialResults(t *testing.T) {
	for _, kind := range []string{"services", "characteristics", "descriptors"} {
		t.Run(kind, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			s := &Service{h: 1, endh: 9}
			char := &Characteristic{svc: s, h: 2, vh: 3, endh: 9}
			d := &Descriptor{char: char, h: 4, uuid: attrClientCharacteristicConfigUUID}
			p.svcs, s.chars, char.descs, char.cccd = []*Service{s}, []*Characteristic{char}, []*Descriptor{d}, d
			var call func() error
			var exchanges []discoveryExchange
			switch kind {
			case "services":
				call = func() error { _, err := p.DiscoverServices(nil); return err }
				exchanges = []discoveryExchange{
					{serviceDiscoveryRequest(1), []byte{attOpReadByGroupRsp, 6, 1, 0, 9, 0, 0x0f, 0x18}},
					{serviceDiscoveryRequest(10), attErrorRsp(attOpReadByGroupReq, 10, attEcodeReadNotPerm)},
				}
			case "characteristics":
				call = func() error { _, err := p.DiscoverCharacteristics(nil, s); return err }
				exchanges = []discoveryExchange{
					{characteristicDiscoveryRequest(1, 9), []byte{attOpReadByTypeRsp, 7, 2, 0, byte(CharNotify), 3, 0, 0x19, 0x2a}},
					{characteristicDiscoveryRequest(3, 9), attErrorRsp(attOpReadByTypeReq, 3, attEcodeReadNotPerm)},
				}
			case "descriptors":
				call = func() error { _, err := p.DiscoverDescriptors(nil, char); return err }
				exchanges = []discoveryExchange{
					{descriptorDiscoveryRequest(4, 9), []byte{attOpFindInfoRsp, 1, 4, 0, 2, 0x29}},
					{descriptorDiscoveryRequest(5, 9), attErrorRsp(attOpFindInfoReq, 5, attEcodeReadNotPerm)},
				}
			}
			if err := runDiscoveryForTest(t, c, call, exchanges...); !errors.Is(err, attEcodeReadNotPerm) {
				t.Fatalf("failed discovery = %v", err)
			}
			if len(p.svcs) != 1 || p.svcs[0] != s || len(s.chars) != 1 || s.chars[0] != char || char.endh != 9 ||
				len(char.descs) != 1 || char.descs[0] != d || char.cccd != d {
				t.Fatal("failed discovery changed cached metadata")
			}
			if p.servicesDiscovered || p.characteristicsDiscovered[s] || p.descriptorsDiscovered[char] {
				t.Fatal("failed discovery marked an incomplete cache as complete")
			}
		})
	}
}

func TestPeripheralDiscoveryRejectsInvalidHandles(t *testing.T) {
	for _, tt := range []struct {
		name     string
		kind     string
		response []byte
	}{
		{"zero service", "services", []byte{attOpReadByGroupRsp, 6, 0, 0, 5, 0, 0x0f, 0x18}},
		{"reversed service", "services", []byte{attOpReadByGroupRsp, 6, 5, 0, 3, 0, 0x0f, 0x18}},
		{"overlapping services", "services", []byte{attOpReadByGroupRsp, 6, 1, 0, 5, 0, 0x0f, 0x18, 4, 0, 9, 0, 0x0f, 0x18}},
		{"zero characteristic", "characteristics", []byte{attOpReadByTypeRsp, 7, 0, 0, 2, 3, 0, 0x19, 0x2a}},
		{"reversed value handle", "characteristics", []byte{attOpReadByTypeRsp, 7, 2, 0, 2, 2, 0, 0x19, 0x2a}},
		{"out of range value", "characteristics", []byte{attOpReadByTypeRsp, 7, 2, 0, 2, 10, 0, 0x19, 0x2a}},
		{"overlapping characteristics", "characteristics", []byte{attOpReadByTypeRsp, 7, 2, 0, 2, 5, 0, 0x19, 0x2a, 4, 0, 2, 6, 0, 0x19, 0x2a}},
		{"zero descriptor", "descriptors", []byte{attOpFindInfoRsp, 1, 0, 0, 2, 0x29}},
		{"backward descriptor", "descriptors", []byte{attOpFindInfoRsp, 1, 3, 0, 2, 0x29}},
		{"out of range descriptor", "descriptors", []byte{attOpFindInfoRsp, 1, 10, 0, 2, 0x29}},
		{"duplicate descriptor", "descriptors", []byte{attOpFindInfoRsp, 1, 4, 0, 2, 0x29, 4, 0, 3, 0x29}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			s := &Service{h: 1, endh: 9}
			char := &Characteristic{svc: s, h: 2, vh: 3, endh: 9}
			var call func() error
			var request []byte
			switch tt.kind {
			case "services":
				call = func() error { _, err := p.DiscoverServices(nil); return err }
				request = serviceDiscoveryRequest(1)
			case "characteristics":
				call = func() error { _, err := p.DiscoverCharacteristics(nil, s); return err }
				request = characteristicDiscoveryRequest(1, 9)
			case "descriptors":
				call = func() error { _, err := p.DiscoverDescriptors(nil, char); return err }
				request = descriptorDiscoveryRequest(4, 9)
			}
			if err := runDiscoveryForTest(t, c, call, discoveryExchange{request, tt.response}); err == nil {
				t.Fatal("invalid handles returned success")
			}
			if len(p.svcs) != 0 || len(s.chars) != 0 || len(char.descs) != 0 || char.cccd != nil {
				t.Fatal("invalid discovery published partial metadata")
			}
		})
	}
}

func TestPeripheralDiscoveryAllowsGapsAndDuplicateUUIDs(t *testing.T) {
	c := newPeripheralTestConn()
	p := newPeripheralForTest(c)
	startPeripheralForTest(t, p, c)
	// The explicitly supplied parent need not be looked up in p.svcs.
	s := &Service{h: 1, endh: 10}
	err := runDiscoveryForTest(t, c, func() error {
		cs, err := p.DiscoverCharacteristics(nil, s)
		if err != nil {
			return err
		}
		if len(cs) != 2 || cs[0].endh != 6 || cs[1].endh != 10 || cs[0].svc != s || cs[1].svc != s {
			return fmt.Errorf("unexpected characteristic ranges: %v", cs)
		}
		return nil
	}, discoveryExchange{characteristicDiscoveryRequest(1, 10), []byte{attOpReadByTypeRsp, 7, 2, 0, 2, 4, 0, 0x19, 0x2a, 7, 0, 2, 10, 0, 0x19, 0x2a}},
		discoveryExchange{characteristicDiscoveryRequest(8, 10), attErrorRsp(attOpReadByTypeReq, 8, attEcodeAttrNotFound)})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeripheralDiscoveryDatabaseChangeClosesConnection(t *testing.T) {
	for _, kind := range []string{"services", "characteristics", "descriptors"} {
		t.Run(kind, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			s := &Service{h: 1, endh: 5}
			char := &Characteristic{svc: s, h: 2, vh: 3, endh: 5}
			var call func() error
			var empty, changed []discoveryExchange
			switch kind {
			case "services":
				call = func() error { _, err := p.DiscoverServices(nil); return err }
				empty = []discoveryExchange{{serviceDiscoveryRequest(1), attErrorRsp(attOpReadByGroupReq, 1, attEcodeAttrNotFound)}}
				changed = []discoveryExchange{
					{serviceDiscoveryRequest(1), []byte{attOpReadByGroupRsp, 6, 1, 0, 0xff, 0xff, 0x0f, 0x18}},
				}
			case "characteristics":
				call = func() error { _, err := p.DiscoverCharacteristics(nil, s); return err }
				empty = []discoveryExchange{{characteristicDiscoveryRequest(1, 5), attErrorRsp(attOpReadByTypeReq, 1, attEcodeAttrNotFound)}}
				changed = []discoveryExchange{
					{characteristicDiscoveryRequest(1, 5), []byte{attOpReadByTypeRsp, 7, 2, 0, 2, 3, 0, 0x19, 0x2a}},
					{characteristicDiscoveryRequest(3, 5), attErrorRsp(attOpReadByTypeReq, 3, attEcodeAttrNotFound)},
				}
			case "descriptors":
				call = func() error { _, err := p.DiscoverDescriptors(nil, char); return err }
				empty = []discoveryExchange{{descriptorDiscoveryRequest(4, 5), attErrorRsp(attOpFindInfoReq, 4, attEcodeAttrNotFound)}}
				changed = []discoveryExchange{
					{descriptorDiscoveryRequest(4, 5), []byte{attOpFindInfoRsp, 1, 5, 0, 2, 0x29}},
				}
			}
			if err := runDiscoveryForTest(t, c, call, empty...); err != nil {
				t.Fatal(err)
			}
			if err := runDiscoveryForTest(t, c, call, changed...); !errors.Is(err, ErrDatabaseChanged) {
				t.Fatalf("database change = %v", err)
			}
			select {
			case <-c.Context().Done():
			default:
				t.Fatal("database change left stale handles usable")
			}
			if len(p.svcs) != 0 || len(s.chars) != 0 || len(char.descs) != 0 || char.cccd != nil {
				t.Fatal("database change replaced the old snapshot")
			}
			if _, err := p.ReadCharacteristic(char); !errors.Is(err, ErrDatabaseChanged) {
				t.Fatalf("operation after database change = %v", err)
			}
		})
	}
}

func TestPeripheralDiscoveryGateCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		startPeripheralForTest(t, p, c)
		result := make(chan error, 2)
		go func() { _, err := p.DiscoverServices(nil); result <- err }()
		peripheralWriteForTest(t, c)
		go func() { _, err := p.DiscoverServices(nil); result <- err }()
		synctest.Wait()
		c.Close()
		for i := 0; i < 2; i++ {
			if err := peripheralResultForTest(t, result); err == nil {
				t.Fatal("canceled discovery returned success")
			}
		}
		if len(c.writes) != 0 {
			t.Fatal("queued discovery reached transport after cancellation")
		}
	})
}

func TestPeripheralDiscoveryRejectsBackwardPages(t *testing.T) {
	for _, kind := range []string{"services", "characteristics", "descriptors"} {
		t.Run(kind, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			s := &Service{h: 1, endh: 9}
			char := &Characteristic{svc: s, h: 2, vh: 3, endh: 9}
			var call func() error
			var requests [][]byte
			var response []byte
			switch kind {
			case "services":
				call = func() error { _, err := p.DiscoverServices(nil); return err }
				requests = [][]byte{serviceDiscoveryRequest(1), serviceDiscoveryRequest(10)}
				response = []byte{attOpReadByGroupRsp, 6, 1, 0, 9, 0, 0x0f, 0x18}
			case "characteristics":
				call = func() error { _, err := p.DiscoverCharacteristics(nil, s); return err }
				requests = [][]byte{characteristicDiscoveryRequest(1, 9), characteristicDiscoveryRequest(3, 9)}
				response = []byte{attOpReadByTypeRsp, 7, 2, 0, 2, 3, 0, 0x19, 0x2a}
			case "descriptors":
				call = func() error { _, err := p.DiscoverDescriptors(nil, char); return err }
				requests = [][]byte{descriptorDiscoveryRequest(4, 9), descriptorDiscoveryRequest(5, 9)}
				response = []byte{attOpFindInfoRsp, 1, 4, 0, 2, 0x29}
			}
			if err := runDiscoveryForTest(t, c, call,
				discoveryExchange{requests[0], response}, discoveryExchange{requests[1], response}); err == nil {
				t.Fatal("backward page returned success")
			}
		})
	}
}

func TestPeripheralDiscovery128BitUUIDAtHandleLimit(t *testing.T) {
	uuid := UUID{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}}
	for _, kind := range []string{"services", "characteristics", "descriptors"} {
		t.Run(kind, func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			s := &Service{h: 1, endh: 0xffff}
			char := &Characteristic{svc: s, h: 2, vh: 0xfffe, endh: 0xffff}
			var call func() error
			var exchanges []discoveryExchange
			switch kind {
			case "services":
				call = func() error {
					ss, err := p.DiscoverServices(nil)
					if err == nil && (len(ss) != 1 || !ss[0].uuid.Equal(uuid) || ss[0].endh != 0xffff) {
						return fmt.Errorf("128-bit services = %v", ss)
					}
					return err
				}
				exchanges = []discoveryExchange{{serviceDiscoveryRequest(1), append([]byte{attOpReadByGroupRsp, 20, 1, 0, 0xff, 0xff}, uuid.b...)}}
			case "characteristics":
				call = func() error {
					cs, err := p.DiscoverCharacteristics(nil, s)
					if err == nil && (len(cs) != 1 || !cs[0].uuid.Equal(uuid) || cs[0].endh != 0xffff) {
						return fmt.Errorf("128-bit characteristics = %v", cs)
					}
					return err
				}
				exchanges = []discoveryExchange{
					{characteristicDiscoveryRequest(1, 0xffff), append([]byte{attOpReadByTypeRsp, 21, 0xfd, 0xff, 2, 0xff, 0xff}, uuid.b...)},
					{characteristicDiscoveryRequest(0xfffe, 0xffff), attErrorRsp(attOpReadByTypeReq, 0xfffe, attEcodeAttrNotFound)},
				}
			case "descriptors":
				call = func() error {
					ds, err := p.DiscoverDescriptors(nil, char)
					if err == nil && (len(ds) != 1 || !ds[0].uuid.Equal(uuid) || ds[0].h != 0xffff) {
						return fmt.Errorf("128-bit descriptors = %v", ds)
					}
					return err
				}
				exchanges = []discoveryExchange{{descriptorDiscoveryRequest(0xffff, 0xffff), append([]byte{attOpFindInfoRsp, 2, 0xff, 0xff}, uuid.b...)}}
			}
			if err := runDiscoveryForTest(t, c, call, exchanges...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
