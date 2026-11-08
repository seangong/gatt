//go:build gattxpc_test

package gatt

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

func newDarwinClientTest(t *testing.T, respond func(int, xpc.Dict) (int, xpc.Dict)) (*device, *peripheral) {
	t.Helper()
	d := newDarwinLifecycleTestDevice()
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	d.ctx = ctx
	d.ready = make(chan struct{})
	p := &peripheral{d: d, id: xpc.MakeUUID("00112233445566778899aabbccddeeff"), sub: newSubscriber()}
	p.ctx, p.cancel = context.WithCancelCause(d.ctx)
	d.plist[p.ID()] = p
	conn, cleanup := xpc.NewTestEchoConnection(darwinBridgeHandler(func(event xpc.Dict, err error) {
		if err != nil {
			d.HandleXpcEvent(nil, err)
			return
		}
		id, args := respond(event.MustGetInt("kCBMsgId"), event.MustGetDict("kCBMsgArgs"))
		if id == 0 {
			return
		}
		response := make(xpc.Dict, len(args)+1)
		for k, v := range args {
			response[k] = v
		}
		response["kCBMsgArgDeviceUUID"] = p.id
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(id), "kCBMsgArgs": response}, nil)
	}))
	d.conn = conn
	close(d.ready)
	t.Cleanup(func() { _ = d.Close(); cleanup(); cancel() })
	return d, p
}

func darwinServiceRecord(h, end uint16, uuid UUID) xpc.Dict {
	return xpc.Dict{"kCBMsgArgServiceStartHandle": int64(h), "kCBMsgArgServiceEndHandle": int64(end), "kCBMsgArgUUID": reverse(uuid.b)}
}

func darwinCharacteristicRecord(h, value uint16, uuid UUID) xpc.Dict {
	return xpc.Dict{
		"kCBMsgArgCharacteristicHandle": int64(h), "kCBMsgArgCharacteristicValueHandle": int64(value),
		"kCBMsgArgCharacteristicProperties": int64(CharRead | CharNotify), "kCBMsgArgUUID": reverse(uuid.b),
	}
}

func darwinDescriptorRecord(h uint16, uuid UUID) xpc.Dict {
	return xpc.Dict{"kCBMsgArgDescriptorHandle": int64(h), "kCBMsgArgUUID": reverse(uuid.b)}
}

func TestDarwinClientDescriptorError(t *testing.T) {
	_, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		return descriptorsDiscovered, xpc.Dict{"kCBMsgArgResult": int64(attEcodeWriteNotPerm)}
	})
	s := &Service{h: 1, endh: 10}
	c := &Characteristic{h: 2, vh: 3, svc: s}
	defer func() {
		if value := recover(); value != nil {
			t.Fatalf("descriptor error response panicked: %v", value)
		}
	}()
	if _, err := p.DiscoverDescriptors(nil, c); !errors.Is(err, attEcodeWriteNotPerm) {
		t.Fatalf("descriptor discovery error = %v", err)
	}
}

func TestDarwinClientRediscoveryPreservesObjects(t *testing.T) {
	svcUUID, charUUID := UUID16(0xfff0), UUID16(0xfff1)
	_, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 45:
			return serviceDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgServices": xpc.Array{darwinServiceRecord(1, 10, svcUUID)}}
		case 62:
			return characteristicsDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgCharacteristics": xpc.Array{darwinCharacteristicRecord(2, 3, charUUID)}}
		case 70:
			return descriptorsDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgDescriptors": xpc.Array{darwinDescriptorRecord(4, attrClientCharacteristicConfigUUID)}}
		}
		return 0, nil
	})
	var firstS *Service
	var firstC *Characteristic
	var firstD *Descriptor
	for round := 0; round < 2; round++ {
		ss, err := p.DiscoverServices(nil)
		if err != nil || len(ss) != 1 {
			t.Fatalf("services = %v, %v", ss, err)
		}
		cs, err := p.DiscoverCharacteristics(nil, ss[0])
		if err != nil || len(cs) != 1 {
			t.Fatalf("characteristics = %v, %v", cs, err)
		}
		ds, err := p.DiscoverDescriptors(nil, cs[0])
		if err != nil || len(ds) != 1 {
			t.Fatalf("descriptors = %v, %v", ds, err)
		}
		if round == 0 {
			firstS, firstC, firstD = ss[0], cs[0], ds[0]
		} else if ss[0] != firstS || cs[0] != firstC || ds[0] != firstD {
			t.Fatal("rediscovery replaced existing objects")
		}
	}
	if firstC.Descriptor() != firstD {
		t.Fatal("CCCD is not the cached descriptor")
	}
}

func TestDarwinClientFailedSubscriptionPreservesCallback(t *testing.T) {
	calls := 0
	_, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id != 68 {
			return 0, nil
		}
		calls++
		res := int64(0)
		if calls == 2 {
			res = int64(attEcodeWriteNotPerm)
		}
		return notificationValueSet, xpc.Dict{"kCBMsgArgResult": res}
	})
	c := &Characteristic{h: 2, vh: 3}
	notified := make(chan int, 2)
	if err := p.SetNotifyValue(c, func(*Characteristic, []byte, error) { notified <- 1 }); err != nil {
		t.Fatal(err)
	}
	if err := p.SetNotifyValue(c, func(*Characteristic, []byte, error) { notified <- 2 }); !errors.Is(err, attEcodeWriteNotPerm) {
		t.Fatalf("replacement error = %v", err)
	}
	p.handleResponse(characteristicRead, xpc.Dict{
		"kCBMsgArgIsNotification": int64(1), "kCBMsgArgCharacteristicHandle": int64(c.h), "kCBMsgArgData": []byte{1},
	})
	if got := receiveDarwin(t, notified); got != 1 {
		t.Fatalf("failed subscription retained callback %d", got)
	}
}

func TestDarwinClientDescriptorResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		rsp  xpc.Dict
		ok   bool
	}{
		{"legacy success", xpc.Dict{"kCBMsgArgDescriptors": xpc.Array{darwinDescriptorRecord(4, UUID16(0x2901))}}, true},
		{"error with array", xpc.Dict{"kCBMsgArgResult": int64(3), "kCBMsgArgDescriptors": xpc.Array{}}, false},
		{"missing array", xpc.Dict{"kCBMsgArgResult": int64(0)}, false},
		{"bad result", xpc.Dict{"kCBMsgArgResult": "bad", "kCBMsgArgDescriptors": xpc.Array{}}, false},
		{"bad record", xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgDescriptors": xpc.Array{"bad"}}, false},
		{"bad UUID", xpc.Dict{"kCBMsgArgDescriptors": xpc.Array{darwinDescriptorRecord(4, UUID{[]byte{1}})}}, false},
		{"bad handle", xpc.Dict{"kCBMsgArgDescriptors": xpc.Array{xpc.Dict{"kCBMsgArgDescriptorHandle": int64(0x10000), "kCBMsgArgUUID": []byte{0x29, 1}}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, p := newDarwinClientTest(t, func(int, xpc.Dict) (int, xpc.Dict) { return descriptorsDiscovered, tc.rsp })
			s := &Service{h: 1, endh: 20}
			c := &Characteristic{h: 2, vh: 3, svc: s}
			old := &Descriptor{h: 5, uuid: attrClientCharacteristicConfigUUID, char: c}
			c.descs, c.cccd = []*Descriptor{old}, old
			_, err := p.DiscoverDescriptors(nil, c)
			if tc.ok {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatal("invalid discovery response succeeded")
				}
				if len(c.descs) != 1 || c.descs[0] != old || c.cccd != old || len(p.descriptorScopes) != 0 {
					t.Fatal("failed discovery mutated the existing snapshot")
				}
			}
		})
	}
}

func darwinFilterRequests(args xpc.Dict, uuid UUID) bool {
	filters := args.MustGetArray("kCBMsgArgUUIDs")
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		value := filter.([]byte)
		if len(value) >= len(uuid.b) {
			match := true
			for i, b := range reverse(uuid.b) {
				if value[i] != b {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func TestDarwinClientFilteredDiscoveryScopes(t *testing.T) {
	svcA, svcB := UUID16(0xfff0), UUID16(0xfff3)
	charA, charB, charC := UUID16(0xff01), UUID16(0xff02), UUID16(0xff03)
	userDescription := UUID16(0x2901)
	d, p := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 45:
			var records xpc.Array
			if darwinFilterRequests(args, svcA) {
				records = append(records, darwinServiceRecord(1, 20, svcA))
			}
			if darwinFilterRequests(args, svcB) {
				records = append(records, darwinServiceRecord(30, 40, svcB))
			}
			return serviceDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgServices": records}
		case 62:
			var records xpc.Array
			if darwinFilterRequests(args, charC) {
				records = append(records, darwinCharacteristicRecord(15, 16, charC))
			}
			if darwinFilterRequests(args, charA) {
				records = append(records, darwinCharacteristicRecord(2, 3, charA))
			}
			if darwinFilterRequests(args, charB) {
				records = append(records, darwinCharacteristicRecord(10, 11, charB))
			}
			return characteristicsDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgCharacteristics": records}
		case 70:
			// Native descriptor discovery may ignore UUID filters. Return an
			// extra CCCD when user description was requested.
			records := xpc.Array{darwinDescriptorRecord(5, attrClientCharacteristicConfigUUID)}
			if darwinFilterRequests(args, userDescription) {
				records = append(records, darwinDescriptorRecord(4, userDescription))
			}
			return descriptorsDiscovered, xpc.Dict{"kCBMsgArgDescriptors": records}
		case 68:
			return notificationValueSet, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	first, err := p.DiscoverServices([]UUID{svcA})
	if err != nil || len(first) != 1 {
		t.Fatalf("first service = %v, %v", first, err)
	}
	s := first[0]
	if _, err := p.DiscoverServices([]UUID{svcB}); err != nil {
		t.Fatal(err)
	}
	all, err := p.DiscoverServices(nil)
	if err != nil || len(all) != 2 || all[0] != s || len(p.Services()) != 2 {
		t.Fatalf("complete services = %v, %v", all, err)
	}
	one, err := p.DiscoverCharacteristics([]UUID{charA}, s)
	if err != nil || len(one) != 1 {
		t.Fatalf("first characteristic = %v, %v", one, err)
	}
	c := one[0]
	c.endh = 9 // A partial refresh must not replace known metadata with zero.
	if _, err := p.DiscoverCharacteristics([]UUID{charB}, s); err != nil {
		t.Fatal(err)
	}
	chars, err := p.DiscoverCharacteristics(nil, s)
	if err != nil || len(chars) != 3 || chars[1] != c || len(s.chars) != 3 || c.endh != 9 {
		t.Fatalf("complete characteristics = %v, %v", chars, err)
	}
	descs, err := p.DiscoverDescriptors([]UUID{userDescription}, c)
	if err != nil || len(descs) != 2 || c.cccd == nil {
		t.Fatalf("partial descriptors = %v, %v", descs, err)
	}
	cccd, description := descs[0], descs[1]
	if got, err := p.DiscoverDescriptors([]UUID{attrClientCharacteristicConfigUUID}, c); err != nil || len(got) != 1 || got[0] != cccd || len(c.descs) != 2 || c.cccd != cccd {
		t.Fatalf("CCCD query = %v, %v", got, err)
	}
	notified := make(chan *Characteristic, 1)
	if err := p.SetNotifyValue(c, func(ch *Characteristic, _ []byte, _ error) { notified <- ch }); err != nil {
		t.Fatal(err)
	}
	if got, err := p.DiscoverCharacteristics([]UUID{charA}, s); err != nil || len(got) != 1 || got[0] != c {
		t.Fatalf("repeated filtered characteristics = %v, %v", got, err)
	}
	if got, err := p.DiscoverDescriptors(nil, c); err != nil || len(got) != 2 || got[0] != cccd || got[1] != description || c.cccd != cccd {
		t.Fatalf("complete descriptors = %v, %v", got, err)
	}
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(characteristicRead), "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": p.id, "kCBMsgArgIsNotification": int64(1),
		"kCBMsgArgCharacteristicHandle": int64(c.h), "kCBMsgArgData": []byte{1},
	}}, nil)
	if ch := receiveDarwin(t, notified); ch != c {
		t.Fatal("rediscovery replaced the subscribed characteristic")
	}
}

func TestDarwinClientDuplicateUUIDInstances(t *testing.T) {
	uuid := UUID16(0xfff1)
	_, p := newDarwinClientTest(t, func(int, xpc.Dict) (int, xpc.Dict) {
		return characteristicsDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgCharacteristics": xpc.Array{
			darwinCharacteristicRecord(2, 3, uuid), darwinCharacteristicRecord(6, 7, uuid),
		}}
	})
	s := &Service{h: 1, endh: 10}
	first, err := p.DiscoverCharacteristics(nil, s)
	if err != nil || len(first) != 2 || first[0] == first[1] {
		t.Fatalf("first duplicate-UUID query = %v, %v", first, err)
	}
	second, err := p.DiscoverCharacteristics([]UUID{uuid}, s)
	if err != nil || len(second) != 2 || second[0] != first[0] || second[1] != first[1] || len(s.chars) != 2 {
		t.Fatalf("repeated duplicate-UUID query = %v, %v", second, err)
	}
}

func TestDarwinClientFailedSubscriptionVariants(t *testing.T) {
	for _, failure := range []xpc.Dict{
		{"kCBMsgArgResult": int64(attEcodeWriteNotPerm)},
		{"kCBMsgArgResult": "invalid"},
	} {
		calls := 0
		_, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
			if id != 68 {
				return 0, nil
			}
			calls++
			if calls == 2 || calls == 3 {
				return notificationValueSet, failure
			}
			return notificationValueSet, xpc.Dict{"kCBMsgArgResult": int64(0)}
		})
		c := &Characteristic{h: 2, vh: 3}
		notified := make(chan int, 2)
		if err := p.SetNotifyValue(c, func(*Characteristic, []byte, error) { notified <- 1 }); err != nil {
			t.Fatal(err)
		}
		if err := p.SetNotifyValue(c, nil); err == nil {
			t.Fatal("failed disable succeeded")
		}
		if err := p.SetNotifyValue(c, func(*Characteristic, []byte, error) { notified <- 2 }); err == nil {
			t.Fatal("failed replacement succeeded")
		}
		p.handleResponse(characteristicRead, xpc.Dict{"kCBMsgArgIsNotification": int64(1), "kCBMsgArgCharacteristicHandle": int64(c.h), "kCBMsgArgData": []byte{1}})
		if got := receiveDarwin(t, notified); got != 1 {
			t.Fatalf("failed operation changed callback to %d", got)
		}
		if err := p.SetNotifyValue(c, nil); err != nil || p.sub.fn(c.h) != nil {
			t.Fatalf("successful disable = %v", err)
		}
	}
}

func TestDarwinClientMetadataWaitersCancel(t *testing.T) {
	started := make(chan struct{}, 1)
	d, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 62 {
			started <- struct{}{}
		}
		return 0, nil
	})
	s := &Service{h: 1, endh: 10}
	c := &Characteristic{h: 2, vh: 3, svc: s}
	results := make(chan error, 2)
	go func() { _, err := p.DiscoverCharacteristics(nil, s); results <- err }()
	receiveDarwin(t, started)
	go func() { _, err := p.DiscoverDescriptors(nil, c); results <- err }()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := receiveDarwin(t, results); !errors.Is(err, xpc.ErrClosed) {
			t.Fatalf("canceled metadata request = %v", err)
		}
	}
	if len(s.chars) != 0 || len(c.descs) != 0 {
		t.Fatal("cancellation published partial discovery metadata")
	}
}

func TestDarwinClientDatabaseChangeWaitsForDisconnection(t *testing.T) {
	for _, kind := range []string{"services", "characteristics", "descriptors"} {
		t.Run(kind, func(t *testing.T) {
			queries := 0
			canceled := make(chan struct{}, 2)
			connected := make(chan struct{}, 2)
			d, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
				switch id {
				case 32:
					canceled <- struct{}{}
					return 0, nil
				case 31:
					connected <- struct{}{}
					return peripheralConnected, xpc.Dict{}
				}
				queries++
				var records xpc.Array
				switch kind {
				case "services":
					if queries > 1 {
						records = xpc.Array{darwinServiceRecord(1, 10, UUID16(0xfff0))}
					}
					return serviceDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgServices": records}
				case "characteristics":
					if queries > 1 {
						records = xpc.Array{darwinCharacteristicRecord(2, 3, UUID16(0xfff1))}
					}
					return characteristicsDiscovered, xpc.Dict{"kCBMsgArgResult": int64(0), "kCBMsgArgCharacteristics": records}
				default:
					if queries > 1 {
						records = xpc.Array{darwinDescriptorRecord(4, UUID16(0x2901))}
					}
					return descriptorsDiscovered, xpc.Dict{"kCBMsgArgDescriptors": records}
				}
			})
			other := &peripheral{d: d, id: xpc.MakeUUID("ffeeddccbbaa99887766554433221100"), sub: newSubscriber()}
			other.ctx, other.cancel = context.WithCancelCause(d.ctx)
			d.plistmu.Lock()
			d.plist[other.ID()] = other
			d.plistmu.Unlock()
			s := &Service{h: 1, endh: 10}
			c := &Characteristic{h: 2, vh: 3, svc: s}
			query := func() error {
				switch kind {
				case "services":
					_, err := p.DiscoverServices([]UUID{UUID16(0xfff0)})
					return err
				case "characteristics":
					_, err := p.DiscoverCharacteristics([]UUID{UUID16(0xfff1)}, s)
					return err
				default:
					_, err := p.DiscoverDescriptors([]UUID{UUID16(0x2901)}, c)
					return err
				}
			}
			if err := query(); err != nil {
				t.Fatal(err)
			}
			if err := query(); !errors.Is(err, ErrDatabaseChanged) {
				t.Fatalf("database change = %v", err)
			}
			receiveDarwin(t, canceled)
			if context.Cause(d.ctx) != nil || context.Cause(other.ctx) != nil {
				t.Fatal("one database change stopped the device or another peripheral")
			}
			if _, err := p.ReadCharacteristic(c); !errors.Is(err, ErrDatabaseChanged) {
				t.Fatalf("old handle remained usable: %v", err)
			}
			if len(p.svcs) != 0 || len(s.chars) != 0 || len(c.descs) != 0 {
				t.Fatal("database change published a new snapshot")
			}
			type connectionResult struct {
				p   Peripheral
				err error
			}
			results := make(chan connectionResult, 3)
			disconnected := make(chan error, 2)
			d.peripheralConnected = func(p Peripheral, err error) { results <- connectionResult{p, err} }
			d.peripheralDisconnected = func(got Peripheral, err error) {
				if got.ID() == p.ID() {
					disconnected <- err
				}
			}
			pending := &peripheral{id: p.id}
			d.Connect(pending)
			if result := receiveDarwin(t, results); result.p != pending || !errors.Is(result.err, ErrDatabaseChanged) {
				t.Fatalf("reconnect before disconnection = %v", result)
			}
			select {
			case <-connected:
				t.Fatal("reconnect was sent before native disconnection")
			default:
			}
			// A late connected event must not replace the retired cache owner.
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralConnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": p.id}}, nil)
			d.plistmu.Lock()
			current := d.plist[p.ID()]
			d.plistmu.Unlock()
			if current != p {
				t.Fatal("late connection event replaced the closing peripheral")
			}
			d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": p.id}}, nil)
			if err := receiveDarwin(t, disconnected); !errors.Is(err, ErrDatabaseChanged) {
				t.Fatalf("database-change disconnect callback = %v", err)
			}
			d.Connect(pending)
			receiveDarwin(t, connected)
			if result := receiveDarwin(t, results); result.err != nil || result.p == p {
				t.Fatalf("reconnect after disconnection = %v", result)
			}
			// A stale connected object cannot cancel the new connection.
			d.CancelConnection(p)
			select {
			case <-canceled:
				t.Fatal("retired peripheral canceled a newer connection")
			default:
			}
		})
	}
}

func TestDarwinClientFilteredScopesCannotOverlap(t *testing.T) {
	for _, kind := range []string{"services", "characteristics"} {
		t.Run(kind, func(t *testing.T) {
			a, b := UUID16(0xff01), UUID16(0xff02)
			_, p := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
				if id == 32 {
					return 0, nil
				}
				if kind == "services" {
					record := darwinServiceRecord(1, 5, a)
					if darwinFilterRequests(args, b) {
						record = darwinServiceRecord(3, 8, b)
					}
					return serviceDiscovered, xpc.Dict{"kCBMsgArgServices": xpc.Array{record}}
				}
				record := darwinCharacteristicRecord(2, 6, a)
				if darwinFilterRequests(args, b) {
					record = darwinCharacteristicRecord(4, 5, b)
				}
				return characteristicsDiscovered, xpc.Dict{"kCBMsgArgCharacteristics": xpc.Array{record}}
			})
			s := &Service{h: 1, endh: 10}
			if kind == "services" {
				first, err := p.DiscoverServices([]UUID{a})
				if err != nil || len(first) != 1 {
					t.Fatalf("first service = %v, %v", first, err)
				}
				if _, err := p.DiscoverServices([]UUID{b}); !errors.Is(err, ErrDatabaseChanged) {
					t.Fatalf("overlapping service scopes = %v", err)
				}
				if cache := p.Services(); len(cache) != 1 || cache[0] != first[0] {
					t.Fatal("overlapping service scope modified the cache")
				}
			} else {
				first, err := p.DiscoverCharacteristics([]UUID{a}, s)
				if err != nil || len(first) != 1 {
					t.Fatalf("first characteristic = %v, %v", first, err)
				}
				if _, err := p.DiscoverCharacteristics([]UUID{b}, s); !errors.Is(err, ErrDatabaseChanged) {
					t.Fatalf("overlapping characteristic scopes = %v", err)
				}
				if len(s.chars) != 1 || s.chars[0] != first[0] {
					t.Fatal("overlapping characteristic scope modified the cache")
				}
			}
		})
	}
}

func TestDarwinClientUUIDAliasScope(t *testing.T) {
	short := UUID16(0x180f)
	long := MustParseUUID("0000180f-0000-1000-8000-00805f9b34fb")
	queries := 0
	_, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 32 {
			return 0, nil
		}
		queries++
		var records xpc.Array
		if queries == 1 {
			records = xpc.Array{darwinServiceRecord(1, 10, short)}
		} else if queries == 2 {
			records = xpc.Array{darwinServiceRecord(1, 10, long)}
		}
		return serviceDiscovered, xpc.Dict{"kCBMsgArgServices": records}
	})
	first, err := p.DiscoverServices([]UUID{long})
	if err != nil || len(first) != 1 {
		t.Fatalf("short UUID response to base-UUID filter = %v, %v", first, err)
	}
	second, err := p.DiscoverServices([]UUID{short})
	if err != nil || len(second) != 1 || second[0] != first[0] {
		t.Fatalf("equivalent UUID representation changed the object: %v, %v", second, err)
	}
	if _, err := p.DiscoverServices([]UUID{long}); !errors.Is(err, ErrDatabaseChanged) {
		t.Fatalf("covered alias scope removal = %v", err)
	}
	if short.Equal(long) {
		t.Fatal("discovery changed the public UUID equality contract")
	}
}

func TestDarwinClientMalformedPagePreservesSnapshot(t *testing.T) {
	for _, kind := range []string{"services", "characteristics", "descriptors"} {
		t.Run(kind, func(t *testing.T) {
			queries := 0
			_, p := newDarwinClientTest(t, func(int, xpc.Dict) (int, xpc.Dict) {
				queries++
				var id int
				var key string
				var record xpc.Dict
				switch kind {
				case "services":
					id, key, record = serviceDiscovered, "kCBMsgArgServices", darwinServiceRecord(1, 10, UUID16(0xfff0))
				case "characteristics":
					id, key, record = characteristicsDiscovered, "kCBMsgArgCharacteristics", darwinCharacteristicRecord(2, 3, UUID16(0xfff1))
				default:
					id, key, record = descriptorsDiscovered, "kCBMsgArgDescriptors", darwinDescriptorRecord(4, attrClientCharacteristicConfigUUID)
				}
				records := xpc.Array{record}
				if queries == 2 {
					records = append(records, "invalid record")
				}
				return id, xpc.Dict{key: records}
			})
			s := &Service{h: 1, endh: 10}
			c := &Characteristic{h: 2, vh: 3, svc: s}
			var original interface{}
			query := func() (interface{}, error) {
				switch kind {
				case "services":
					records, err := p.DiscoverServices(nil)
					if err == nil && len(records) == 1 {
						return records[0], nil
					}
					return nil, err
				case "characteristics":
					records, err := p.DiscoverCharacteristics(nil, s)
					if err == nil && len(records) == 1 {
						return records[0], nil
					}
					return nil, err
				default:
					records, err := p.DiscoverDescriptors(nil, c)
					if err == nil && len(records) == 1 {
						return records[0], nil
					}
					return nil, err
				}
			}
			var err error
			if original, err = query(); err != nil || original == nil {
				t.Fatalf("initial query = %v, %v", original, err)
			}
			if _, err := query(); err == nil || errors.Is(err, ErrDatabaseChanged) {
				t.Fatalf("malformed page = %v", err)
			}
			if current, err := query(); err != nil || current != original {
				t.Fatalf("snapshot after malformed page = %v, %v", current, err)
			}
			if kind == "descriptors" && c.cccd != original {
				t.Fatal("malformed page replaced the CCCD")
			}
		})
	}
}

// pauseDarwinCauseContext pauses one send's cancellation check without pausing
// a concurrent native event. It exposes the identity-check/enqueue race without
// changing the production transport or contacting a system Bluetooth service.
type pauseDarwinCauseContext struct {
	context.Context
	paused  atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *pauseDarwinCauseContext) Err() error {
	if c.paused.CompareAndSwap(false, true) {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}

func TestDarwinClientCancellationCannotOvertakeReconnection(t *testing.T) {
	commands := make(chan int, 4)
	d, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 31 || id == 32 {
			commands <- id
		}
		return 0, nil
	})
	paused := &pauseDarwinCauseContext{Context: d.ctx, entered: make(chan struct{}), release: make(chan struct{})}
	d.ctx = paused
	defer func() {
		select {
		case <-paused.release:
		default:
			close(paused.release)
		}
	}()
	canceled := make(chan struct{})
	go func() { d.CancelConnection(p); close(canceled) }()
	receiveDarwin(t, paused.entered)
	traced := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(peripheralDisconnected), "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": p.id}}, nil)
		d.Connect(&peripheral{id: p.id})
		close(traced)
	}()
	receiveDarwin(t, started)
	first := 0
	select {
	case first = <-commands:
	case <-time.After(100 * time.Millisecond):
		// Correct code keeps disconnection/remapping behind the old cancel's
		// bounded local enqueue. Let that enqueue complete now.
	}
	close(paused.release)
	receiveDarwin(t, canceled)
	receiveDarwin(t, traced)
	if first == 0 {
		first = receiveDarwin(t, commands)
	}
	second := receiveDarwin(t, commands)
	if first != 32 || second != 31 {
		t.Fatalf("native command order = [%d %d], want old cancel before new connect [32 31]", first, second)
	}
}

func TestDarwinClientLifecycleEnqueueErrorReleasesRegistryLock(t *testing.T) {
	for _, operation := range []string{"connect", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			d := newDarwinLifecycleTestDevice()
			defer d.Close()
			p := &peripheral{d: d, id: xpc.MakeUUID("00112233445566778899aabbccddeeff"), sub: newSubscriber()}
			if operation == "cancel" {
				p.ctx, p.cancel = context.WithCancelCause(d.ctx)
				d.plist[p.ID()] = p
			}
			done := make(chan struct{})
			go func() {
				// The zero XPC connection makes local enqueue fail immediately.
				// fail must run after releasing the registry lock it also needs.
				if operation == "connect" {
					d.Connect(p)
				} else {
					d.CancelConnection(p)
				}
				close(done)
			}()
			receiveDarwin(t, done)
			if !errors.Is(d.Err(), xpc.ErrClosed) {
				t.Fatalf("lifecycle enqueue error = %v", d.Err())
			}
			d.plistmu.Lock()
			remaining := len(d.plist)
			d.plistmu.Unlock()
			if remaining != 0 {
				t.Fatal("failed lifecycle enqueue retained registered peripherals")
			}
		})
	}
}

type pauseSecondDarwinCauseContext struct {
	context.Context
	checks  atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (c *pauseSecondDarwinCauseContext) Err() error {
	if c.checks.Add(1) == 2 {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}

func TestDarwinClientConnectEnqueuesBeforeCancellation(t *testing.T) {
	commands := make(chan int, 4)
	d, live := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		if id == 31 || id == 32 {
			commands <- id
		}
		return 0, nil
	})
	pending := &peripheral{id: live.id}
	d.plistmu.Lock()
	delete(d.plist, live.ID())
	d.plistmu.Unlock()
	paused := &pauseSecondDarwinCauseContext{Context: d.ctx, entered: make(chan struct{}), release: make(chan struct{})}
	d.ctx = paused
	defer func() {
		select {
		case <-paused.release:
		default:
			close(paused.release)
		}
	}()
	connected := make(chan struct{})
	go func() { d.Connect(pending); close(connected) }()
	connectPaused := false
	select {
	case <-paused.entered:
		// The previous implementation checked cancellation again after
		// publishing the pending object but before enqueueing the connect.
		connectPaused = true
	case <-connected:
		// A locally enqueued connect is now visible before releasing its owner.
	case <-time.After(3 * time.Second):
		t.Fatal("connect did not reach its native enqueue")
	}
	canceled := make(chan struct{})
	go func() { d.CancelConnection(pending); close(canceled) }()
	if connectPaused {
		receiveDarwin(t, canceled)
		close(paused.release)
		receiveDarwin(t, connected)
	} else {
		receiveDarwin(t, paused.entered)
		close(paused.release)
		receiveDarwin(t, canceled)
	}
	first, second := receiveDarwin(t, commands), receiveDarwin(t, commands)
	if first != 31 || second != 32 {
		t.Fatalf("native command order = [%d %d], want connect before cancel [31 32]", first, second)
	}
}
