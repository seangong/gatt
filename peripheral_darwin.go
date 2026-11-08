package gatt

import (
	"context"
	"errors"
	"io"

	"github.com/seangong/gatt/v2/xpc"
)

type peripheral struct {
	// NameChanged is called whenever the peripheral GAP Device name has changed.
	NameChanged func(Peripheral)

	// ServicesModified is called when one or more service of a peripheral have changed.
	// A list of invalid service is provided in the parameter.
	ServicesModified func(Peripheral, []*Service)

	d    *device
	svcs []*Service

	sub *subscriber

	id   xpc.UUID
	name string

	ctx      context.Context
	cancel   context.CancelCauseFunc
	requests xpcRequests
}

func NewPeripheral(u UUID) Peripheral { return &peripheral{id: xpc.UUID(u.b)} }

func (p *peripheral) Device() Device       { return p.d }
func (p *peripheral) ID() string           { return p.id.String() }
func (p *peripheral) Name() string         { return p.name }
func (p *peripheral) Services() []*Service { return p.svcs }

func (p *peripheral) DiscoverServices(ss []UUID) ([]*Service, error) {
	rsp, err := p.sendReq(45, xpc.Dict{
		"kCBMsgArgDeviceUUID": p.id,
		"kCBMsgArgUUIDs":      uuidSlice(ss),
	})
	if err != nil {
		return nil, err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return nil, attEcode(res)
	}
	svcs := []*Service{}
	for _, xss := range rsp["kCBMsgArgServices"].(xpc.Array) {
		xs := xss.(xpc.Dict)
		u := MustParseUUID(xs.MustGetHexBytes("kCBMsgArgUUID"))
		h := uint16(xs.MustGetInt("kCBMsgArgServiceStartHandle"))
		endh := uint16(xs.MustGetInt("kCBMsgArgServiceEndHandle"))
		svcs = append(svcs, &Service{uuid: u, h: h, endh: endh})
	}
	p.svcs = svcs
	return svcs, nil
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
	rsp, err := p.sendReq(62, xpc.Dict{
		"kCBMsgArgDeviceUUID":         p.id,
		"kCBMsgArgServiceStartHandle": s.h,
		"kCBMsgArgServiceEndHandle":   s.endh,
		"kCBMsgArgUUIDs":              uuidSlice(cs),
	})
	if err != nil {
		return nil, err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return nil, attEcode(res)
	}
	for _, xcs := range rsp.MustGetArray("kCBMsgArgCharacteristics") {
		xc := xcs.(xpc.Dict)
		u := MustParseUUID(xc.MustGetHexBytes("kCBMsgArgUUID"))
		ch := uint16(xc.MustGetInt("kCBMsgArgCharacteristicHandle"))
		vh := uint16(xc.MustGetInt("kCBMsgArgCharacteristicValueHandle"))
		props := Property(xc.MustGetInt("kCBMsgArgCharacteristicProperties"))
		c := &Characteristic{uuid: u, svc: s, props: props, h: ch, vh: vh}
		s.chars = append(s.chars, c)
	}
	return s.chars, nil
}

func (p *peripheral) DiscoverDescriptors(ds []UUID, c *Characteristic) ([]*Descriptor, error) {
	rsp, err := p.sendReq(70, xpc.Dict{
		"kCBMsgArgDeviceUUID":                p.id,
		"kCBMsgArgCharacteristicHandle":      c.h,
		"kCBMsgArgCharacteristicValueHandle": c.vh,
		"kCBMsgArgUUIDs":                     uuidSlice(ds),
	})
	if err != nil {
		return nil, err
	}
	for _, xds := range rsp.MustGetArray("kCBMsgArgDescriptors") {
		xd := xds.(xpc.Dict)
		u := MustParseUUID(xd.MustGetHexBytes("kCBMsgArgUUID"))
		h := uint16(xd.MustGetInt("kCBMsgArgDescriptorHandle"))
		d := &Descriptor{uuid: u, char: c, h: h}
		c.descs = append(c.descs, d)
	}
	return c.descs, nil
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
	if err := context.Cause(p.context()); err != nil {
		return err
	}
	previous := p.sub.fn(c.h)
	set := 1
	if f == nil {
		set = 0
	}
	// To avoid race condition, registeration is handled before requesting the server.
	if f != nil {
		// Note: when notified, core bluetooth reports characteristic handle, not value's handle.
		p.sub.subscribe(c.h, func(b []byte, err error) { f(c, b, err) })
	}
	rsp, err := p.sendReq(68, xpc.Dict{
		"kCBMsgArgDeviceUUID":                p.id,
		"kCBMsgArgCharacteristicHandle":      c.h,
		"kCBMsgArgCharacteristicValueHandle": c.vh,
		"kCBMsgArgState":                     set,
	})
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
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return attEcode(res)
	}
	// To avoid race condition, unregisteration is handled after server responses.
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
	return rsp.MustGetInt("kCBMsgArgData")
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
			go f(b, nil)
		}
		return
	}
	p.requests.deliver(id, args)
}
