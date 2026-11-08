package gatt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/seangong/gatt/v2/xpc"
)

const (
	peripheralDiscovered   = 37
	peripheralConnected    = 38
	peripheralDisconnected = 40
	// below constants for Yosemite
	rssiRead                   = 55
	includedServicesDiscovered = 63
	serviceDiscovered          = 56
	characteristicsDiscovered  = 64
	characteristicRead         = 71
	characteristicWritten      = 72
	notificationValueSet       = 74
	descriptorsDiscovered      = 76
	descriptorRead             = 79
	descriptorWritten          = 80
)

type device struct {
	deviceHandler

	conn xpc.XPC

	role int // 1: peripheralManager (server), 0: centralManager (client)

	ctx      context.Context
	cancel   context.CancelCauseFunc
	ready    chan struct{}
	requests xpcRequests
	stateMu  sync.Mutex

	// Only used in client/centralManager implementation
	plist   map[string]*peripheral
	plistmu *sync.Mutex

	// Only used in server/peripheralManager implementation

	attrN  int
	attrs  map[int]*attr
	attrmu sync.RWMutex

	subscribers   map[string]*central
	subscribersmu sync.Mutex
}

func NewDevice(opts ...Option) (Device, error) {
	d := &device{
		plist:   map[string]*peripheral{},
		plistmu: &sync.Mutex{},

		attrN: 1,
		attrs: make(map[int]*attr),

		subscribers: make(map[string]*central),
	}
	if err := d.Option(opts...); err != nil {
		return nil, err
	}
	d.ctx, d.cancel = context.WithCancelCause(context.Background())
	d.ready = make(chan struct{})
	d.conn = xpc.XpcConnect("com.apple.blued", d)
	close(d.ready)
	if err := d.conn.Err(); err != nil {
		d.fail(err)
		return nil, err
	}
	return d, nil
}

func (d *device) Init(f func(Device, State)) error {
	d.stateMu.Lock()
	d.stateChanged = f
	d.stateMu.Unlock()
	rsp, err := d.sendReq(1, xpc.Dict{
		"kCBMsgArgName":    fmt.Sprintf("gopher-%v", time.Now().Unix()),
		"kCBMsgArgOptions": xpc.Dict{"kCBInitOptionShowPowerAlert": 1},
		"kCBMsgArgType":    d.role,
	})
	if err != nil {
		return err
	}
	if f != nil {
		go f(d, State(rsp.MustGetInt("kCBMsgArgState")))
	}
	return nil
}

func (d *device) Advertise(a *AdvPacket) error {
	if err := a.Err(); err != nil {
		return err
	}
	rsp, err := d.sendReq(8, xpc.Dict{
		"kCBAdvDataAppleMfgData": a.b, // not a.Bytes(). should be slice
	})

	if err != nil {
		return err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return errors.New("FIXME: Advertise error")
	}
	return nil
}

func (d *device) AdvertiseNameAndServices(name string, ss []UUID) error {
	us := uuidSlice(ss)
	rsp, err := d.sendReq(8, xpc.Dict{
		"kCBAdvDataLocalName":    name,
		"kCBAdvDataServiceUUIDs": us},
	)
	if err != nil {
		return err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return errors.New("FIXME: Advertise error")
	}
	return nil
}

func (d *device) AdvertiseIBeaconData(data []byte) error {
	var utsname xpc.Utsname
	xpc.Uname(&utsname)

	var rsp xpc.Dict
	var err error

	if utsname.Release >= "14." {
		l := len(data)
		buf := bytes.NewBuffer([]byte{byte(l + 5), 0xFF, 0x4C, 0x00, 0x02, byte(l)})
		buf.Write(data)
		rsp, err = d.sendReq(8, xpc.Dict{"kCBAdvDataAppleMfgData": buf.Bytes()})
	} else {
		rsp, err = d.sendReq(8, xpc.Dict{"kCBAdvDataAppleBeaconKey": data})
	}

	if err != nil {
		return err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return errors.New("FIXME: Advertise error")
	}

	return nil
}

func (d *device) AdvertiseIBeacon(u UUID, major, minor uint16, pwr int8) error {
	b := make([]byte, 21)
	copy(b, reverse(u.b))                     // Big endian
	binary.BigEndian.PutUint16(b[16:], major) // Big endian
	binary.BigEndian.PutUint16(b[18:], minor) // Big endian
	b[20] = uint8(pwr)                        // Measured Tx Power
	return d.AdvertiseIBeaconData(b)
}

func (d *device) StopAdvertising() error {
	rsp, err := d.sendReq(9, nil)
	if err != nil {
		return err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return errors.New("FIXME: Stop Advertise error")
	}
	return nil
}

func (d *device) RemoveAllServices() error {
	return d.sendCmd(12, nil)
}

func (d *device) AddService(s *Service) error {
	if err := context.Cause(d.ctx); err != nil {
		return err
	}
	if s.uuid.Equal(attrGAPUUID) || s.uuid.Equal(attrGATTUUID) {
		// skip GATT and GAP services
		return nil
	}

	d.attrmu.Lock()
	xs := xpc.Dict{
		"kCBMsgArgAttributeID":     d.attrN,
		"kCBMsgArgAttributeIDs":    []int{},
		"kCBMsgArgCharacteristics": nil,
		"kCBMsgArgType":            1, // 1 => primary, 0 => excluded
		"kCBMsgArgUUID":            reverse(s.uuid.b),
	}
	d.attrN++

	xcs := xpc.Array{}
	for _, c := range s.Characteristics() {
		props := 0
		perm := 0
		if c.props&CharRead != 0 {
			props |= 0x02
			if CharRead&c.secure != 0 {
				perm |= 0x04
			} else {
				perm |= 0x01
			}
		}
		if c.props&CharWriteNR != 0 {
			props |= 0x04
			if c.secure&CharWriteNR != 0 {
				perm |= 0x08
			} else {
				perm |= 0x02
			}
		}
		if c.props&CharWrite != 0 {
			props |= 0x08
			if c.secure&CharWrite != 0 {
				perm |= 0x08
			} else {
				perm |= 0x02
			}
		}
		if c.props&CharNotify != 0 {
			if c.secure&CharNotify != 0 {
				props |= 0x100
			} else {
				props |= 0x10
			}
		}
		if c.props&CharIndicate != 0 {
			if c.secure&CharIndicate != 0 {
				props |= 0x200
			} else {
				props |= 0x20
			}
		}

		xc := xpc.Dict{
			"kCBMsgArgAttributeID":              d.attrN,
			"kCBMsgArgUUID":                     reverse(c.uuid.b),
			"kCBMsgArgAttributePermissions":     perm,
			"kCBMsgArgCharacteristicProperties": props,
			"kCBMsgArgData":                     c.value,
		}
		d.attrs[d.attrN] = &attr{h: uint16(d.attrN), value: c.value, pvt: c}
		d.attrN++

		xds := xpc.Array{}
		for _, d := range c.Descriptors() {
			if d.uuid.Equal(attrClientCharacteristicConfigUUID) {
				// skip CCCD
				continue
			}
			xd := xpc.Dict{
				"kCBMsgArgData": d.value,
				"kCBMsgArgUUID": reverse(d.uuid.b),
			}
			xds = append(xds, xd)
		}
		xc["kCBMsgArgDescriptors"] = xds
		xcs = append(xcs, xc)
	}
	xs["kCBMsgArgCharacteristics"] = xcs

	d.attrmu.Unlock()
	rsp, err := d.sendReq(10, xs)
	if err != nil {
		return err
	}
	if res := rsp.MustGetInt("kCBMsgArgResult"); res != 0 {
		return errors.New("FIXME: Add Srvice error")
	}
	return nil
}

func (d *device) SetServices(ss []*Service) error {
	if err := d.RemoveAllServices(); err != nil {
		return err
	}
	for _, s := range ss {
		if err := d.AddService(s); err != nil {
			return err
		}
	}
	return nil
}

func (d *device) Scan(ss []UUID, dup bool) {
	args := xpc.Dict{
		"kCBMsgArgUUIDs": uuidSlice(ss),
		"kCBMsgArgOptions": xpc.Dict{
			"kCBScanOptionAllowDuplicates": map[bool]int{true: 1, false: 0}[dup],
		},
	}
	d.sendCmd(29, args)
}

func (d *device) StopScanning() {
	d.sendCmd(30, nil)
}

func (d *device) Connect(p Peripheral) {
	pp := p.(*peripheral)
	d.plistmu.Lock()
	if err := context.Cause(d.ctx); err != nil {
		d.plistmu.Unlock()
		if d.peripheralConnected != nil {
			go d.peripheralConnected(p, err)
		}
		return
	}
	d.plist[pp.id.String()] = pp
	d.plistmu.Unlock()
	d.sendCmd(31,
		xpc.Dict{
			"kCBMsgArgDeviceUUID": pp.id,
			"kCBMsgArgOptions": xpc.Dict{
				"kCBConnectOptionNotifyOnDisconnection": 1,
			},
		})
}

func (d *device) respondToRequest(id int, args xpc.Dict) {

	switch id {
	case 19: // ReadRequest
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		t := args.MustGetInt("kCBMsgArgTransactionID")
		a := args.MustGetInt("kCBMsgArgAttributeID")
		o := args.MustGetInt("kCBMsgArgOffset")

		d.attrmu.RLock()
		attr := d.attrs[a]
		d.attrmu.RUnlock()
		if attr == nil {
			return
		}
		v := attr.value
		if v == nil {
			c := newCentral(d, u)
			req := &ReadRequest{
				Request: Request{Central: c},
				Cap:     int(c.mtu - 1),
				Offset:  o,
			}
			rsp := newResponseWriter(int(c.mtu - 1))
			if c, ok := attr.pvt.(*Characteristic); ok {
				c.rhandler.ServeRead(rsp, req)
				v = rsp.bytes()
			}
		}

		d.sendCmd(13, xpc.Dict{
			"kCBMsgArgAttributeID":   a,
			"kCBMsgArgData":          v,
			"kCBMsgArgTransactionID": t,
			"kCBMsgArgResult":        0,
		})

	case 20: // WriteRequest
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		t := args.MustGetInt("kCBMsgArgTransactionID")
		a := 0
		noRsp := false
		xxws := args.MustGetArray("kCBMsgArgATTWrites")
		for _, xxw := range xxws {
			xw := xxw.(xpc.Dict)
			if a == 0 {
				a = xw.MustGetInt("kCBMsgArgAttributeID")
			}
			o := xw.MustGetInt("kCBMsgArgOffset")
			i := xw.MustGetInt("kCBMsgArgIgnoreResponse")
			b := xw.MustGetBytes("kCBMsgArgData")
			_ = o
			d.attrmu.RLock()
			attr := d.attrs[a]
			d.attrmu.RUnlock()
			if attr == nil {
				return
			}
			c := newCentral(d, u)
			r := Request{Central: c}
			attr.pvt.(*Characteristic).whandler.ServeWrite(r, b)
			if i == 1 {
				noRsp = true
			}

		}
		if noRsp {
			break
		}
		d.sendCmd(13, xpc.Dict{
			"kCBMsgArgAttributeID":   a,
			"kCBMsgArgData":          nil,
			"kCBMsgArgTransactionID": t,
			"kCBMsgArgResult":        0,
		})

	case 21: // subscribed
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		a := args.MustGetInt("kCBMsgArgAttributeID")
		d.attrmu.RLock()
		attr := d.attrs[a]
		d.attrmu.RUnlock()
		if attr == nil {
			return
		}
		d.subscribersmu.Lock()
		c := d.subscribers[u.String()]
		if c == nil {
			c = newCentral(d, u)
			d.subscribers[u.String()] = c
		}
		d.subscribersmu.Unlock()
		c.startNotify(attr, c.mtu)

	case 22: // unubscribed
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		a := args.MustGetInt("kCBMsgArgAttributeID")
		d.attrmu.RLock()
		attr := d.attrs[a]
		d.attrmu.RUnlock()
		if attr == nil {
			return
		}
		d.subscribersmu.Lock()
		c := d.subscribers[u.String()]
		d.subscribersmu.Unlock()
		if c != nil {
			c.stopNotify(attr)
		}

	case 23: // notificationSent
	}
}

func (d *device) CancelConnection(p Peripheral) {
	d.sendCmd(32, xpc.Dict{"kCBMsgArgDeviceUUID": p.(*peripheral).id})
}

// process device events and asynchronous errors
// (implements XpcEventHandler)
func (d *device) HandleXpcEvent(event xpc.Dict, err error) {
	<-d.ready
	if err != nil {
		d.fail(err)
		return
	}
	if context.Cause(d.ctx) != nil {
		return
	}

	id := event.MustGetInt("kCBMsgId")
	args := event.MustGetDict("kCBMsgArgs")
	//log.Printf(">> %d, %v", id, args)

	switch id {
	case // device event
		6,  // StateChanged
		16, // AdvertisingStarted
		17, // AdvertisingStopped
		18: // ServiceAdded
		if !d.requests.deliver(id, args) && id == 6 {
			d.stateMu.Lock()
			f := d.stateChanged
			d.stateMu.Unlock()
			if f != nil {
				go f(d, State(args.MustGetInt("kCBMsgArgState")))
			}
		}

	case
		19, // ReadRequest
		20, // WriteRequest
		21, // Subscribe
		22, // Unubscribe
		23: // Confirmation
		d.respondToRequest(id, args)

	case peripheralDiscovered:
		xa := args.MustGetDict("kCBMsgArgAdvertisementData")
		if len(xa) == 0 {
			return
		}
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		a := &Advertisement{
			LocalName:        xa.GetString("kCBAdvDataLocalName", args.GetString("kCBMsgArgName", "")),
			TxPowerLevel:     xa.GetInt("kCBAdvDataTxPowerLevel", 0),
			ManufacturerData: xa.GetBytes("kCBAdvDataManufacturerData", nil),
		}

		rssi := args.MustGetInt("kCBMsgArgRssi")

		if xu, ok := xa["kCBAdvDataServiceUUIDs"]; ok {
			for _, xs := range xu.(xpc.Array) {
				s := UUID{reverse(xs.([]byte))}
				a.Services = append(a.Services, s)
			}
		}
		if xsds, ok := xa["kCBAdvDataServiceData"]; ok {
			xsd := xsds.(xpc.Array)
			for i := 0; i < len(xsd); i += 2 {
				sd := ServiceData{
					UUID: UUID{xsd[i].([]byte)},
					Data: xsd[i+1].([]byte),
				}
				a.ServiceData = append(a.ServiceData, sd)
			}
		}
		if d.peripheralDiscovered != nil {
			go d.peripheralDiscovered(&peripheral{id: xpc.UUID(u.b), d: d}, a, rssi)
		}

	case peripheralConnected:
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		p := &peripheral{
			id:  xpc.UUID(u.b),
			d:   d,
			sub: newSubscriber(),
		}
		p.ctx, p.cancel = context.WithCancelCause(d.ctx)
		d.plistmu.Lock()
		if err := context.Cause(d.ctx); err != nil {
			d.plistmu.Unlock()
			p.stop(err)
			return
		}
		d.plist[xpc.UUID(u.b).String()] = p
		d.plistmu.Unlock()

		if d.peripheralConnected != nil {
			go d.peripheralConnected(p, nil)
		}

	case peripheralDisconnected:
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		d.plistmu.Lock()
		p := d.plist[xpc.UUID(u.b).String()]
		delete(d.plist, xpc.UUID(u.b).String())
		d.plistmu.Unlock()
		if p == nil {
			return
		}
		p.stop(io.ErrClosedPipe)
		if d.peripheralDisconnected != nil {
			go d.peripheralDisconnected(p, nil) // TODO: Get Result as error?
		}

	case // Peripheral events
		rssiRead,
		serviceDiscovered,
		includedServicesDiscovered,
		characteristicsDiscovered,
		characteristicRead,
		characteristicWritten,
		notificationValueSet,
		descriptorsDiscovered,
		descriptorRead,
		descriptorWritten:

		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		d.plistmu.Lock()
		p := d.plist[xpc.UUID(u.b).String()]
		d.plistmu.Unlock()
		if p != nil {
			p.handleResponse(id, args)
		}

	default:
		log.Printf("Unhandled event: %#v", event)
	}
}

// Done is closed when this macOS device session starts terminating, including
// on Close or an asynchronous XPC error. It does not wait for application
// callbacks, connection cleanup, or the native finalizer to finish.
// This method is an optional capability outside the portable Device interface.
func (d *device) Done() <-chan struct{} { return d.ctx.Done() }

// Err returns the first session termination cause, or nil while the session is
// live. After Done is closed, Err is non-nil and remains unchanged.
// This method is an optional capability outside the portable Device interface.
func (d *device) Err() error { return context.Cause(d.ctx) }

// Close starts cancellation without waiting for application callbacks. The
// native connection retains its handler until its final callback has finished.
func (d *device) Close() error {
	d.fail(xpc.ErrClosed)
	return nil
}

func (d *device) fail(err error) {
	if err == nil {
		err = io.ErrClosedPipe
	}
	d.cancel(err)
	err = context.Cause(d.ctx)
	// Cancel native delivery even if a user callback is currently blocked.
	_ = d.conn.Close()

	d.plistmu.Lock()
	peripherals := d.plist
	d.plist = make(map[string]*peripheral)
	d.plistmu.Unlock()
	for _, p := range peripherals {
		p.stop(err)
		if p.cancel == nil {
			if d.peripheralConnected != nil {
				go d.peripheralConnected(p, err)
			}
		} else if d.peripheralDisconnected != nil {
			go d.peripheralDisconnected(p, err)
		}
	}
	d.subscribersmu.Lock()
	for _, c := range d.subscribers {
		c.stopNotifiers()
	}
	d.subscribersmu.Unlock()
}

func (d *device) sendReq(id int, args xpc.Dict) (xpc.Dict, error) {
	responseIDs := map[int]int{1: 6, 8: 16, 9: 17, 10: 18}
	return d.requests.request(d.ctx, responseIDs[id], func() error {
		return d.sendCBMsg(id, args)
	})
}

func (d *device) sendCmd(id int, args xpc.Dict) error {
	return d.sendCBMsg(id, args)
}

func (d *device) sendCBMsg(id int, args xpc.Dict) error {
	if err := context.Cause(d.ctx); err != nil {
		return err
	}
	// log.Printf("<< %d, %v", id, args)
	if err := d.conn.SendError(xpc.Dict{"kCBMsgId": id, "kCBMsgArgs": args}, false); err != nil {
		d.fail(err)
		return context.Cause(d.ctx)
	}
	return nil
}
