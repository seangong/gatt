package gatt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/seangong/gatt/v2/xpc"
)

type peripheral struct {
	// NameChanged is called whenever the peripheral GAP Device name has changed.
	NameChanged func(Peripheral)

	// ServicesModified is called when one or more service of a peripheral have changed.
	// A list of invalid service is provided in the parameter.
	ServicesModified func(Peripheral, []*Service)

	d      *device
	svcs   []*Service
	svcsMu sync.RWMutex

	metadataOnce         sync.Once
	metadataGate         chan struct{}
	serviceScope         discoveryScope
	characteristicScopes map[*Service]discoveryScope
	descriptorScopes     map[*Characteristic]discoveryScope
	closing              bool // Protected by device.plistmu.
	// connectSource preserves the caller's argument for a pending reconnect.
	// It is set before publication and is not carried into a connected session.
	connectSource *peripheral

	sub *subscriber

	id   xpc.UUID
	name string

	ctx      context.Context
	cancel   context.CancelCauseFunc
	requests xpcRequests

	callbackOnce sync.Once
	callbacks    *darwinCallbacks
}

func NewPeripheral(u UUID) Peripheral { return &peripheral{id: xpc.UUID(u.b)} }

func (p *peripheral) Device() Device { return p.d }
func (p *peripheral) ID() string     { return p.id.String() }
func (p *peripheral) Name() string   { return p.name }
func (p *peripheral) Services() []*Service {
	p.svcsMu.RLock()
	defer p.svcsMu.RUnlock()
	return p.svcs
}

func (p *peripheral) DiscoverServices(ss []UUID) ([]*Service, error) {
	if err := p.lockMetadata(); err != nil {
		return nil, err
	}
	defer p.unlockMetadata()
	rsp, err := p.sendReq(45, xpc.Dict{
		"kCBMsgArgDeviceUUID": p.id,
		"kCBMsgArgUUIDs":      uuidSlice(ss),
	})
	if err != nil {
		return nil, err
	}
	array, err := darwinDiscoveryArray(rsp, "kCBMsgArgServices")
	if err != nil {
		return nil, err
	}
	found := make([]*Service, 0, len(array))
	for _, value := range array {
		record, err := darwinRecord(value)
		if err != nil {
			return nil, err
		}
		uuid, err := darwinUUID(record)
		if err != nil {
			return nil, err
		}
		h, err := darwinHandle(record, "kCBMsgArgServiceStartHandle")
		if err != nil {
			return nil, err
		}
		end, err := darwinHandle(record, "kCBMsgArgServiceEndHandle")
		if err != nil {
			return nil, err
		}
		if end < h {
			return nil, fmt.Errorf("invalid XPC service range: %w", ErrInvalidLength)
		}
		found = append(found, &Service{uuid: uuid, h: h, endh: end})
	}
	rows := darwinServices(found)
	if err := validDarwinAttributes(rows); err != nil {
		return nil, err
	}
	if err := context.Cause(p.context()); err != nil {
		return nil, err
	}
	oldRows := darwinServices(p.svcs)
	if p.serviceScope.changed(oldRows, rows, ss) {
		return nil, p.databaseChanged("services")
	}
	byHandle := make(map[uint16]*Service, len(p.svcs)+len(found))
	for _, old := range p.svcs {
		if !inDiscoveryScope(ss, old.uuid) {
			byHandle[old.h] = old
		}
	}
	for i, current := range found {
		for j, old := range p.svcs {
			if sameDarwinAttribute(oldRows[j], rows[i]) {
				current = old
				break
			}
		}
		found[i], byHandle[current.h] = current, current
	}
	cache := make([]*Service, 0, len(byHandle))
	for _, current := range byHandle {
		cache = append(cache, current)
	}
	sort.Slice(cache, func(i, j int) bool { return cache[i].h < cache[j].h })
	if err := validDarwinAttributes(darwinServices(cache)); err != nil {
		return nil, p.databaseChanged("services")
	}
	p.svcsMu.Lock()
	p.svcs = cache
	p.svcsMu.Unlock()
	p.serviceScope.record(rows, ss)
	return found, nil
}

func (p *peripheral) DiscoverIncludedServices(ss []UUID, s *Service) ([]*Service, error) {
	rsp, err := p.sendReq(60, xpc.Dict{
		"kCBMsgArgDeviceUUID":         p.id,
		"kCBMsgArgServiceStartHandle": s.h,
		"kCBMsgArgServiceEndHandle":   s.endh,
		"kCBMsgArgUUIDs":              uuidSlice(ss),
	})
	if err != nil {
		return nil, err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return nil, attEcode(res)
	}
	// TODO
	return nil, notImplemented
}

func (p *peripheral) DiscoverCharacteristics(cs []UUID, s *Service) ([]*Characteristic, error) {
	if err := p.lockMetadata(); err != nil {
		return nil, err
	}
	defer p.unlockMetadata()
	if s == nil || s.h == 0 || s.endh < s.h {
		return nil, errors.New("invalid service range")
	}
	rsp, err := p.sendReq(62, xpc.Dict{
		"kCBMsgArgDeviceUUID":         p.id,
		"kCBMsgArgServiceStartHandle": s.h,
		"kCBMsgArgServiceEndHandle":   s.endh,
		"kCBMsgArgUUIDs":              uuidSlice(cs),
	})
	if err != nil {
		return nil, err
	}
	array, err := darwinDiscoveryArray(rsp, "kCBMsgArgCharacteristics")
	if err != nil {
		return nil, err
	}
	found := make([]*Characteristic, 0, len(array))
	for _, value := range array {
		record, err := darwinRecord(value)
		if err != nil {
			return nil, err
		}
		uuid, err := darwinUUID(record)
		if err != nil {
			return nil, err
		}
		h, err := darwinHandle(record, "kCBMsgArgCharacteristicHandle")
		if err != nil {
			return nil, err
		}
		vh, err := darwinHandle(record, "kCBMsgArgCharacteristicValueHandle")
		if err != nil {
			return nil, err
		}
		props, ok := record["kCBMsgArgCharacteristicProperties"].(int64)
		if !ok || props < 0 || h <= s.h || vh <= h || vh > s.endh {
			return nil, fmt.Errorf("invalid XPC characteristic: %w", ErrInvalidLength)
		}
		found = append(found, &Characteristic{uuid: uuid, svc: s, h: h, vh: vh, props: Property(props)})
	}
	rows := darwinCharacteristics(found)
	if err := validDarwinAttributes(rows); err != nil {
		return nil, err
	}
	if err := context.Cause(p.context()); err != nil {
		return nil, err
	}
	oldChars := s.Characteristics()
	oldRows := darwinCharacteristics(oldChars)
	scope := p.characteristicScopes[s]
	if scope.changed(oldRows, rows, cs) {
		return nil, p.databaseChanged("characteristics")
	}
	byHandle := make(map[uint16]*Characteristic, len(oldChars)+len(found))
	for _, old := range oldChars {
		if !inDiscoveryScope(cs, old.uuid) {
			byHandle[old.h] = old
		}
	}
	for i, current := range found {
		for j, old := range oldChars {
			if sameDarwinAttribute(oldRows[j], rows[i]) {
				current = old
				break
			}
		}
		found[i], byHandle[current.h] = current, current
	}
	cache := make([]*Characteristic, 0, len(byHandle))
	for _, current := range byHandle {
		cache = append(cache, current)
	}
	sort.Slice(cache, func(i, j int) bool { return cache[i].h < cache[j].h })
	if err := validDarwinAttributes(darwinCharacteristics(cache)); err != nil {
		return nil, p.databaseChanged("characteristics")
	}
	s.SetCharacteristics(cache)
	scope.record(rows, cs)
	if p.characteristicScopes == nil {
		p.characteristicScopes = make(map[*Service]discoveryScope)
	}
	p.characteristicScopes[s] = scope
	return found, nil
}

func (p *peripheral) DiscoverDescriptors(ds []UUID, c *Characteristic) ([]*Descriptor, error) {
	if err := p.lockMetadata(); err != nil {
		return nil, err
	}
	defer p.unlockMetadata()
	if c == nil || c.h == 0 || c.vh <= c.h {
		return nil, errors.New("invalid characteristic handles")
	}
	rsp, err := p.sendReq(70, xpc.Dict{
		"kCBMsgArgDeviceUUID":                p.id,
		"kCBMsgArgCharacteristicHandle":      c.h,
		"kCBMsgArgCharacteristicValueHandle": c.vh,
		"kCBMsgArgUUIDs":                     uuidSlice(ds),
	})
	if err != nil {
		return nil, err
	}
	array, err := darwinDiscoveryArray(rsp, "kCBMsgArgDescriptors")
	if err != nil {
		return nil, err
	}
	found := make([]*Descriptor, 0, len(array))
	for _, value := range array {
		record, err := darwinRecord(value)
		if err != nil {
			return nil, err
		}
		uuid, err := darwinUUID(record)
		if err != nil {
			return nil, err
		}
		h, err := darwinHandle(record, "kCBMsgArgDescriptorHandle")
		if err != nil {
			return nil, err
		}
		if h <= c.vh || c.svc != nil && c.svc.endh != 0 && h > c.svc.endh {
			return nil, fmt.Errorf("invalid XPC descriptor handle: %w", ErrInvalidLength)
		}
		found = append(found, &Descriptor{uuid: uuid, char: c, h: h})
	}
	rows := darwinDescriptors(found)
	if err := validDarwinAttributes(rows); err != nil {
		return nil, err
	}
	if err := context.Cause(p.context()); err != nil {
		return nil, err
	}
	oldDescs := c.Descriptors()
	oldRows := darwinDescriptors(oldDescs)
	scope := p.descriptorScopes[c]
	if scope.changed(oldRows, rows, ds) {
		return nil, p.databaseChanged("descriptors")
	}
	byHandle := make(map[uint16]*Descriptor, len(oldDescs)+len(found))
	for _, old := range oldDescs {
		if !inDiscoveryScope(ds, old.uuid) {
			byHandle[old.h] = old
		}
	}
	for i, current := range found {
		for j, old := range oldDescs {
			if sameDarwinAttribute(oldRows[j], rows[i]) {
				current = old
				break
			}
		}
		found[i], byHandle[current.h] = current, current
	}
	cache := make([]*Descriptor, 0, len(byHandle))
	for _, current := range byHandle {
		cache = append(cache, current)
	}
	sort.Slice(cache, func(i, j int) bool { return cache[i].h < cache[j].h })
	if err := validDarwinAttributes(darwinDescriptors(cache)); err != nil {
		return nil, p.databaseChanged("descriptors")
	}
	cccd := c.Descriptor()
	if inDiscoveryScope(ds, attrClientCharacteristicConfigUUID) {
		cccd = nil
	}
	for _, current := range cache {
		if darwinUUIDKey(current.uuid) == darwinUUIDKey(attrClientCharacteristicConfigUUID) {
			cccd = current
		}
	}
	c.descsMu.Lock()
	c.descs, c.cccd = cache, cccd
	c.descsMu.Unlock()
	scope.record(rows, ds)
	if p.descriptorScopes == nil {
		p.descriptorScopes = make(map[*Characteristic]discoveryScope)
	}
	p.descriptorScopes[c] = scope
	return found, nil
}

func (p *peripheral) ReadCharacteristic(c *Characteristic) ([]byte, error) {
	rsp, err := p.sendReq(65, xpc.Dict{
		"kCBMsgArgDeviceUUID":                p.id,
		"kCBMsgArgCharacteristicHandle":      c.h,
		"kCBMsgArgCharacteristicValueHandle": c.vh,
	})
	if err != nil {
		return nil, err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return nil, attEcode(res)
	}
	b := rsp.MustGetBytes("kCBMsgArgData")
	return b, nil
}

func (p *peripheral) ReadLongCharacteristic(c *Characteristic) ([]byte, error) {
	return nil, errors.New("Not implemented")
}

func (p *peripheral) WriteCharacteristic(c *Characteristic, b []byte, noRsp bool) error {
	args := xpc.Dict{
		"kCBMsgArgDeviceUUID":                p.id,
		"kCBMsgArgCharacteristicHandle":      c.h,
		"kCBMsgArgCharacteristicValueHandle": c.vh,
		"kCBMsgArgData":                      b,
		"kCBMsgArgType":                      map[bool]int{false: 0, true: 1}[noRsp],
	}
	if noRsp {
		return p.sendCmd(66, args)
	}
	rsp, err := p.sendReq(66, args)
	if err != nil {
		return err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return attEcode(res)
	}
	return nil
}

func (p *peripheral) ReadDescriptor(d *Descriptor) ([]byte, error) {
	rsp, err := p.sendReq(77, xpc.Dict{
		"kCBMsgArgDeviceUUID":       p.id,
		"kCBMsgArgDescriptorHandle": d.h,
	})
	if err != nil {
		return nil, err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return nil, attEcode(res)
	}
	b := rsp.MustGetBytes("kCBMsgArgData")
	return b, nil
}

func (p *peripheral) WriteDescriptor(d *Descriptor, b []byte) error {
	rsp, err := p.sendReq(78, xpc.Dict{
		"kCBMsgArgDeviceUUID":       p.id,
		"kCBMsgArgDescriptorHandle": d.h,
		"kCBMsgArgData":             b,
	})
	if err != nil {
		return err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return attEcode(res)
	}
	return nil
}

func (p *peripheral) SetNotifyValue(c *Characteristic, f func(*Characteristic, []byte, error)) error {
	if err := p.lockMetadata(); err != nil {
		return err
	}
	defer p.unlockMetadata()
	if c == nil || c.h == 0 || c.vh <= c.h {
		return errors.New("invalid characteristic handles")
	}
	previous := p.sub.fn(c.h)
	set := 0
	if f != nil {
		set = 1
		// Register before sending so an immediate native notification is not lost.
		p.sub.subscribe(c.h, func(b []byte, err error) { f(c, b, err) })
	}
	rsp, err := p.sendReq(68, xpc.Dict{
		"kCBMsgArgDeviceUUID":                p.id,
		"kCBMsgArgCharacteristicHandle":      c.h,
		"kCBMsgArgCharacteristicValueHandle": c.vh,
		"kCBMsgArgState":                     set,
	})
	if err == nil {
		err = darwinResult(rsp)
	}
	if err != nil {
		if f != nil {
			if previous == nil {
				p.sub.unsubscribe(c.h)
			} else {
				p.sub.subscribe(c.h, previous)
			}
		}
		return err
	}
	if f == nil {
		p.sub.unsubscribe(c.h)
	}
	return nil
}

func (p *peripheral) SetIndicateValue(c *Characteristic,
	f func(*Characteristic, []byte, error)) error {
	// TODO: Implement set indications logic for darwin (https://github.com/seangong/gatt/issues/32)
	return nil
}

func (p *peripheral) ReadRSSI() int {
	rsp, err := p.sendReq(43, xpc.Dict{"kCBMsgArgDeviceUUID": p.id})
	if err != nil {
		return -1
	}
	if _, present := rsp["kCBMsgArgResult"]; present {
		if err := darwinResult(rsp); err != nil {
			return -1
		}
	}
	value, ok := rsp["kCBMsgArgData"].(int64)
	if !ok {
		return -1
	}
	return int(value)
}

func (p *peripheral) SetMTU(mtu uint16) error {
	return errors.New("Not implemented")
}

func uuidSlice(uu []UUID) [][]byte {
	us := [][]byte{}
	for _, u := range uu {
		us = append(us, reverse(u.b))
	}
	return us
}

func (p *peripheral) lockMetadata() error {
	ctx := p.context()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	p.metadataOnce.Do(func() { p.metadataGate = make(chan struct{}, 1) })
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case p.metadataGate <- struct{}{}:
	}
	if err := context.Cause(ctx); err != nil {
		p.unlockMetadata()
		return err
	}
	return nil
}

func (p *peripheral) unlockMetadata() { <-p.metadataGate }

func (p *peripheral) databaseChanged(kind string) error {
	err := fmt.Errorf("discovered %s changed; reconnect before using cached handles: %w", kind, ErrDatabaseChanged)
	p.stop(err)
	if cause := context.Cause(p.context()); cause != nil {
		err = cause
	}
	p.d.CancelConnection(p)
	return err
}

func (p *peripheral) stop(err error) {
	if p.cancel != nil {
		p.cancel(err)
	}
}

func (p *peripheral) context() context.Context {
	if p.ctx != nil {
		return p.ctx
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(io.ErrClosedPipe)
	return ctx
}

func (p *peripheral) sendCmd(id int, args xpc.Dict) error {
	if err := context.Cause(p.context()); err != nil {
		return err
	}
	return p.d.sendCBMsg(id, args)
}

func (p *peripheral) sendReq(id int, args xpc.Dict) (xpc.Dict, error) {
	responseIDs := map[int]int{
		43: rssiRead,
		45: serviceDiscovered,
		60: includedServicesDiscovered,
		62: characteristicsDiscovered,
		65: characteristicRead,
		66: characteristicWritten,
		68: notificationValueSet,
		70: descriptorsDiscovered,
		77: descriptorRead,
		78: descriptorWritten,
	}
	return p.requests.request(p.context(), responseIDs[id], func() error {
		return p.d.sendCBMsg(id, args)
	})
}

func (p *peripheral) handleResponse(id int, args xpc.Dict) {
	if context.Cause(p.context()) != nil {
		return
	}
	if id == characteristicRead && args.GetInt("kCBMsgArgIsNotification", 0) != 0 {
		// While we're notified with the value's handle, blued reports the characteristic handle.
		ch := uint16(args.MustGetInt("kCBMsgArgCharacteristicHandle"))
		b := args.MustGetBytes("kCBMsgArgData")
		if f := p.sub.fn(ch); f != nil {
			p.queueNotification(func() { f(b, nil) })
		}
		return
	}
	p.requests.deliver(id, args)
}
