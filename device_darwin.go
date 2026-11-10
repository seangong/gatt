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
	// State callbacks have their own bounded stream, outside native responses.
	stateReady   <-chan struct{}
	stateQueue   []darwinStateCallback
	stateRunning bool

	discoveryMu     sync.Mutex
	discoveryPaused bool
	discoveryActive int
	discoveryQueue  []darwinDiscoveryCallback

	callbackOnce  sync.Once
	callbackSlots chan struct{}

	// Only used in client/centralManager implementation
	plist   map[string]*peripheral
	plistmu *sync.Mutex

	// Only used in server/peripheralManager implementation

	attrN  int
	attrs  map[int]*attr
	attrmu sync.RWMutex

	subscribers   map[string]*central
	subscribersmu sync.Mutex

	attOnce  sync.Once
	attQueue chan darwinATTRequest
	attDone  chan struct{}
	attMu    sync.Mutex // Order local ATT response enqueue with session cancellation.
}

type darwinATTRequest struct {
	id   int
	args xpc.Dict
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
	ready := make(chan struct{})
	defer close(ready)
	_, err := d.requests.request(d.ctx, 6, func() error {
		// Register only after this Init owns the native request slot. Another
		// Init waiting for it must not replace this call's initial handler.
		d.stateMu.Lock()
		d.stateChanged = f
		d.stateReady = ready
		d.stateMu.Unlock()
		return d.sendCBMsg(1, xpc.Dict{
			"kCBMsgArgName":    fmt.Sprintf("gopher-%v", time.Now().Unix()),
			"kCBMsgArgOptions": xpc.Dict{"kCBInitOptionShowPowerAlert": 1},
			"kCBMsgArgType":    d.role,
		})
	})
	return err
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
	d.attrmu.Lock()
	if err := context.Cause(d.ctx); err != nil {
		d.attrmu.Unlock()
		return err
	}
	// Local enqueue and retirement share one boundary with subscriptions and
	// notification sends. Native removal has no acknowledgement event.
	err := d.conn.SendError(xpc.Dict{"kCBMsgId": 12, "kCBMsgArgs": xpc.Dict{}}, false)
	if err == nil {
		d.attrs = make(map[int]*attr)
		d.stopServiceNotifiersLocked(nil)
	}
	d.attrmu.Unlock()
	if err != nil {
		d.fail(err)
		return context.Cause(d.ctx)
	}
	return nil
}

// stopServiceNotifiersLocked requires attrmu. A nil set retires every notifier;
// otherwise only notifiers belonging to the supplied attribute records stop.
func (d *device) stopServiceNotifiersLocked(removed map[int]*attr) {
	d.subscribersmu.Lock()
	defer d.subscribersmu.Unlock()
	for uuid, c := range d.subscribers {
		c.notifiersmu.Lock()
		for id, n := range c.notifiers {
			if removed == nil || removed[id] == n.a {
				n.stop()
				delete(c.notifiers, id)
			}
		}
		if len(c.notifiers) == 0 {
			delete(d.subscribers, uuid)
		}
		c.notifiersmu.Unlock()
	}
}

// Native descriptor registration only supports static values; it does not
// assign descriptor IDs that could route application read or write handlers.
func validateServiceDescriptors(s *Service) error {
	for _, c := range s.Characteristics() {
		for _, descriptor := range c.Descriptors() {
			if descriptor.rhandler != nil || descriptor.whandler != nil {
				return fmt.Errorf("dynamic macOS descriptor %s: %w", descriptor.uuid, errors.ErrUnsupported)
			}
		}
	}
	return nil
}

func (d *device) AddService(s *Service) error {
	if err := context.Cause(d.ctx); err != nil {
		return err
	}
	if s.uuid.Equal(attrGAPUUID) || s.uuid.Equal(attrGATTUUID) {
		// skip GATT and GAP services
		return nil
	}
	if err := validateServiceDescriptors(s); err != nil {
		return fmt.Errorf("add service: %w", err)
	}

	var added map[int]*attr
	rsp, err := d.requests.request(d.ctx, 18, func() error {
		d.attrmu.Lock()
		if err := context.Cause(d.ctx); err != nil {
			d.attrmu.Unlock()
			return err
		}
		added = make(map[int]*attr, len(s.Characteristics()))
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
			added[d.attrN] = &attr{h: uint16(d.attrN), nativeID: d.attrN, value: c.value, pvt: c}
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

		// Publish only when this request actually enters the native send queue.
		// The registry lock is released before waiting for event 18.
		err := d.conn.SendError(xpc.Dict{"kCBMsgId": 10, "kCBMsgArgs": xs}, false)
		if err == nil {
			for id, a := range added {
				d.attrs[id] = a
			}
		}
		d.attrmu.Unlock()
		if err != nil {
			d.fail(err)
			return context.Cause(d.ctx)
		}
		return nil
	})
	if err == nil {
		result, ok := rsp["kCBMsgArgResult"].(int64)
		if !ok {
			err = fmt.Errorf("invalid native AddService result: %w", ErrInvalidLength)
		} else if result != 0 {
			err = fmt.Errorf("native AddService result %d", result)
		}
	}
	if err != nil {
		d.attrmu.Lock()
		for id, a := range added {
			if d.attrs[id] == a {
				delete(d.attrs, id)
			}
		}
		if added != nil {
			d.stopServiceNotifiersLocked(added)
		}
		d.attrmu.Unlock()
		return fmt.Errorf("add service: %w", err)
	}
	return nil
}

func (d *device) SetServices(ss []*Service) error {
	if err := context.Cause(d.ctx); err != nil {
		return err
	}
	// Reject unsupported local configuration before retiring any live service.
	// Native failures below retain the existing partial-application behavior.
	for _, s := range ss {
		if s.uuid.Equal(attrGAPUUID) || s.uuid.Equal(attrGATTUUID) {
			continue
		}
		if err := validateServiceDescriptors(s); err != nil {
			return fmt.Errorf("set services: %w", err)
		}
	}
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
	d.sendScanCommand(29, args)
}

func (d *device) StopScanning() {
	d.sendScanCommand(30, nil)
}

func (d *device) Connect(p Peripheral) {
	pp := p.(*peripheral)
	d.plistmu.Lock()
	if err := context.Cause(d.ctx); err != nil {
		d.plistmu.Unlock()
		d.reportConnectError(p, err)
		return
	}
	previous := d.plist[pp.ID()]
	if previous != nil &&
		(previous.closing || previous.cancel != nil && context.Cause(previous.context()) != nil) {
		err := context.Cause(previous.context())
		if err == nil {
			err = io.ErrClosedPipe
		}
		d.plistmu.Unlock()
		d.reportConnectError(p, err)
		return
	}
	if previous == nil {
		pending := pp
		if pp.cancel != nil || pp.closing {
			// A connected object owns its original context and caches, even after
			// removal from the registry. Reconnection must not reuse its state.
			// A canceled pending object also keeps its original closing marker.
			pending = &peripheral{id: pp.id, d: d, name: pp.name, connectSource: pp}
		}
		d.plist[pp.id.String()] = pending
	}
	// Repeated connects still send the native command, but keep the current
	// UUID owner and the original source of a pending reconnect.
	// Registration and local enqueue share the registry lock so cancellation
	// cannot overtake this connect. SendError does not wait for peer callbacks.
	err := d.conn.SendError(xpc.Dict{"kCBMsgId": 31, "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": pp.id,
		"kCBMsgArgOptions": xpc.Dict{
			"kCBConnectOptionNotifyOnDisconnection": 1,
		},
	}}, false)
	d.plistmu.Unlock()
	if err != nil {
		d.fail(err)
	}
}

// Native reception must stay available while an ATT handler calls synchronous
// device APIs. One worker preserves business request order without unbounded
// goroutines; subscriptions and native operation responses stay on the receiver.
func (d *device) queueATTRequest(id int, args xpc.Dict) {
	if context.Cause(d.ctx) != nil {
		return
	}
	d.attOnce.Do(func() {
		d.attQueue = make(chan darwinATTRequest, 16)
		d.attDone = make(chan struct{})
		go func() {
			defer close(d.attDone)
			for {
				select {
				case <-d.ctx.Done():
					return
				case request := <-d.attQueue:
					if context.Cause(d.ctx) != nil {
						return
					}
					d.respondToRequest(request.id, request.args)
				}
			}
		}()
	})
	select {
	case <-d.ctx.Done():
	case d.attQueue <- darwinATTRequest{id: id, args: args}:
	default:
		// Preserve protocol validation and write-command response suppression,
		// but do not run any business handler when the queue has no capacity.
		d.respondToRequestStatus(id, args, attEcodeInsuffResources)
	}
}

func (d *device) respondToRequest(id int, args xpc.Dict) {
	d.respondToRequestStatus(id, args, attEcodeSuccess)
}

func (d *device) respondToRequestStatus(id int, args xpc.Dict, forcedStatus attEcode) {
	if (id == 19 || id == 20) && context.Cause(d.ctx) != nil {
		return
	}
	switch id {
	case 19: // ReadRequest
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		t := args.MustGetInt("kCBMsgArgTransactionID")
		a := args.MustGetInt("kCBMsgArgAttributeID")
		o := args.MustGetInt("kCBMsgArgOffset")

		d.attrmu.RLock()
		attribute := d.attrs[a]
		d.attrmu.RUnlock()
		if attribute == nil {
			d.respondATT(a, t, nil, attEcodeInvalidHandle)
			return
		}
		v := attribute.value
		code := attEcodeSuccess
		if v == nil {
			var handler ReadHandler
			switch target := attribute.pvt.(type) {
			case *Characteristic:
				if target != nil {
					handler = target.rhandler
				}
			case *Descriptor:
				if target != nil {
					handler = target.rhandler
				}
			}
			if handler == nil {
				code = attEcodeReadNotPerm
			} else if forcedStatus != attEcodeSuccess {
				code = forcedStatus
			} else {
				if context.Cause(d.ctx) != nil {
					return
				}
				c := newCentral(d, u)
				req := &ReadRequest{
					Request: Request{Central: c},
					Cap:     int(c.mtu - 1),
					Offset:  o,
				}
				rsp := newResponseWriter(req.Cap)
				handler.ServeRead(rsp, req)
				code = attErrorForStatus(rsp.status)
				if code == attEcodeSuccess {
					v = rsp.bytes()
				}
			}
		} else if forcedStatus != attEcodeSuccess {
			code = forcedStatus
		}
		if code != attEcodeSuccess {
			v = nil
		}
		d.respondATT(a, t, v, code)

	case 20: // WriteRequest
		t := args.MustGetInt("kCBMsgArgTransactionID")
		writes, ok := args["kCBMsgArgATTWrites"].(xpc.Array)
		if !ok || len(writes) == 0 {
			d.respondATT(0, t, nil, attEcodeInvalidPDU)
			return
		}
		a, noRsp, valid := 0, true, true
		hasResponseAttribute := false
		var single xpc.Dict
		for _, item := range writes {
			w, ok := item.(xpc.Dict)
			if !ok {
				valid, noRsp = false, false
				continue
			}
			h, validHandle := w["kCBMsgArgAttributeID"].(int64)
			i, validIgnore := w["kCBMsgArgIgnoreResponse"].(int64)
			if a == 0 && validHandle {
				a = int(h)
			}
			if !validIgnore || i != 1 {
				noRsp = false
				if !hasResponseAttribute && validHandle {
					a, hasResponseAttribute = int(h), true
				}
			}
			if !validHandle || h <= 0 || !validIgnore || i < 0 || i > 1 {
				valid = false
			}
			single = w
		}
		code := attEcodeSuccess
		switch {
		case !valid:
			code = attEcodeInvalidPDU
		case len(writes) != 1:
			// ServeWrite can have arbitrary side effects and provides no rollback.
			// Reject the whole native atomic batch before invoking any handler.
			code = attEcodeReqNotSupp
		default:
			o, validOffset := single["kCBMsgArgOffset"].(int64)
			b, validData := single["kCBMsgArgData"].([]byte)
			if !validOffset || !validData {
				code = attEcodeInvalidPDU
			} else if o != 0 {
				code = attEcodeInvalidOffset
			} else {
				d.attrmu.RLock()
				attribute := d.attrs[a]
				d.attrmu.RUnlock()
				if attribute == nil {
					code = attEcodeInvalidHandle
				} else {
					var handler WriteHandler
					switch target := attribute.pvt.(type) {
					case *Characteristic:
						if target != nil {
							handler = target.whandler
						}
					case *Descriptor:
						if target != nil {
							handler = target.whandler
						}
					}
					if handler == nil {
						code = attEcodeWriteNotPerm
					} else if forcedStatus != attEcodeSuccess {
						code = forcedStatus
					} else {
						if context.Cause(d.ctx) != nil {
							return
						}
						u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
						code = attErrorForStatus(handler.ServeWrite(Request{Central: newCentral(d, u)}, b))
					}
				}
			}
		}
		if !noRsp {
			d.respondATT(a, t, nil, code)
		}

	case 21: // subscribed
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		a := args.MustGetInt("kCBMsgArgAttributeID")
		d.attrmu.RLock()
		attr := d.attrs[a]
		if attr == nil {
			d.attrmu.RUnlock()
			return
		}
		d.subscribersmu.Lock()
		if context.Cause(d.ctx) != nil {
			d.subscribersmu.Unlock()
			d.attrmu.RUnlock()
			return
		}
		key := u.String()
		c := d.subscribers[key]
		if c == nil {
			c = newCentral(d, u)
			d.subscribers[key] = c
		}
		c.startNotify(attr, c.mtu)
		d.subscribersmu.Unlock()
		d.attrmu.RUnlock()

	case 22: // unubscribed
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		a := args.MustGetInt("kCBMsgArgAttributeID")
		d.attrmu.RLock()
		attr := d.attrs[a]
		if attr == nil {
			d.attrmu.RUnlock()
			return
		}
		d.subscribersmu.Lock()
		key := u.String()
		if c := d.subscribers[key]; c != nil && c.stopNotify(attr) {
			delete(d.subscribers, key)
		}
		d.subscribersmu.Unlock()
		d.attrmu.RUnlock()

	case 23: // notificationSent
	}
}

func (d *device) respondATT(attribute, transaction int, value []byte, code attEcode) {
	d.attMu.Lock()
	if context.Cause(d.ctx) != nil {
		d.attMu.Unlock()
		return
	}
	err := d.conn.SendError(xpc.Dict{"kCBMsgId": 13, "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgAttributeID":   attribute,
		"kCBMsgArgData":          value,
		"kCBMsgArgTransactionID": transaction,
		"kCBMsgArgResult":        int(code),
	}}, false)
	d.attMu.Unlock()
	if err != nil {
		d.fail(err)
	}
}

func (d *device) CancelConnection(p Peripheral) {
	pp := p.(*peripheral)
	d.plistmu.Lock()
	if context.Cause(d.ctx) != nil {
		d.plistmu.Unlock()
		return
	}
	current := d.plist[pp.ID()]
	// A retired object may cancel its explicit pending reconnect, but never a
	// newer connected session. Keep closing entries until native disconnection.
	fromPendingReconnect := current != nil && current.cancel == nil && current.connectSource == pp
	if pp.cancel != nil && current != pp && !fromPendingReconnect || current != nil && current.closing {
		d.plistmu.Unlock()
		return
	}
	if current != nil {
		current.closing = true
	}
	// Keep the old UUID owner registered until its cancel is locally queued.
	// Native disconnection/reconnection cannot publish a newer owner in between.
	err := d.conn.SendError(xpc.Dict{"kCBMsgId": 32, "kCBMsgArgs": xpc.Dict{"kCBMsgArgDeviceUUID": pp.id}}, false)
	d.plistmu.Unlock()
	if err != nil {
		d.fail(err)
	}
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
	case 6: // StateChanged, including the Init response.
		// Queue before waking Init so a later state cannot overtake its first
		// callback. The worker waits for Init to release its response waiter.
		if !d.queueStateCallback(args) {
			d.fail(errDarwinStateQueueFull)
			return
		}
		d.requests.deliver(id, args)
	case // device event
		16, // AdvertisingStarted
		17, // AdvertisingStopped
		18: // ServiceAdded
		d.requests.deliver(id, args)

	case
		19, // ReadRequest
		20: // WriteRequest
		d.queueATTRequest(id, args)
	case
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
			if !d.queueDiscoveryCallback(&peripheral{id: xpc.UUID(u.b), d: d, name: a.LocalName}, a, rssi) {
				d.fail(errDarwinDiscoveryQueueFull)
			}
		}

	case peripheralConnected:
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		p := &peripheral{
			id:   xpc.UUID(u.b),
			d:    d,
			sub:  newSubscriber(),
			name: args.GetString("kCBMsgArgName", ""),
		}
		p.ctx, p.cancel = context.WithCancelCause(d.ctx)
		d.plistmu.Lock()
		if err := context.Cause(d.ctx); err != nil {
			d.plistmu.Unlock()
			p.stop(err)
			return
		}
		// A UUID has one connected owner. A repeated native success must not
		// replace its pending requests, discovery cache or subscriptions.
		previous := d.plist[p.ID()]
		if previous != nil &&
			(previous.closing || previous.cancel != nil) {
			d.plistmu.Unlock()
			p.stop(io.ErrClosedPipe)
			return
		}
		if p.name == "" && previous != nil {
			p.name = previous.name
		}
		var connected func()
		if f := d.peripheralConnected; f != nil {
			connected = func() { f(p, nil) }
		}
		if p.callbackQueue(d, connected) == nil {
			d.plistmu.Unlock()
			p.stop(errDarwinCallbackCapacity)
			d.fail(errDarwinCallbackCapacity)
			return
		}
		d.plist[xpc.UUID(u.b).String()] = p
		d.plistmu.Unlock()

	case peripheralDisconnected:
		u := UUID{args.MustGetUUID("kCBMsgArgDeviceUUID")}
		d.plistmu.Lock()
		p := d.plist[xpc.UUID(u.b).String()]
		delete(d.plist, xpc.UUID(u.b).String())
		d.plistmu.Unlock()
		if p == nil {
			return
		}
		cause := context.Cause(p.context())
		if !errors.Is(cause, ErrDatabaseChanged) {
			cause = nil
		}
		p.stop(io.ErrClosedPipe)
		if p.cancel == nil {
			// A pending argument can be reused for another attempt. Do not
			// attach a completed connected-session queue to that public object.
			if f := d.peripheralDisconnected; f != nil {
				callbackPeripheral := p
				if p.connectSource != nil {
					callbackPeripheral = p.connectSource
				}
				if !d.startPeripheralCallout(func() { f(callbackPeripheral, cause) }) {
					d.fail(errDarwinCallbackCapacity)
				}
			}
			return
		}
		if !d.finishPeripheralCallbacks(p, cause) {
			d.fail(errDarwinCallbackCapacity)
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
	d.attMu.Lock()
	d.cancel(err)
	d.attMu.Unlock()
	err = context.Cause(d.ctx)
	d.stopStateCallbacks()
	d.stopDiscoveryCallbacks()
	// Cancel native delivery even if a user callback is currently blocked.
	_ = d.conn.Close()

	d.plistmu.Lock()
	peripherals := d.plist
	d.plist = make(map[string]*peripheral)
	d.plistmu.Unlock()
	for _, p := range peripherals {
		p.stop(err)
		if p.cancel == nil {
			if f := d.peripheralConnected; f != nil {
				callbackPeripheral := p
				if p.connectSource != nil {
					callbackPeripheral = p.connectSource
				}
				// Done/Err already report session termination if all callback
				// slots remain occupied by application code.
				d.startPeripheralCallout(func() { f(callbackPeripheral, err) })
			}
		} else {
			d.finishPeripheralCallbacks(p, err)
		}
	}
	d.subscribersmu.Lock()
	for _, c := range d.subscribers {
		c.stopNotifiers()
	}
	d.subscribers = make(map[string]*central)
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
