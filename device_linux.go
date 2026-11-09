package gatt

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/seangong/gatt/v2/linux"
	"github.com/seangong/gatt/v2/linux/cmd"
)

type device struct {
	deviceHandler

	hci            *linux.HCI
	state          State
	closeHCI       func() error
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	initialized    bool
	stopping       bool
	stopDone       chan struct{}
	stopErr        error
	workers        sync.WaitGroup
	lifecycleSlots chan struct{}
	requestSlots   chan struct{}
	callbackSlots  chan struct{}

	// All the following fields are only used peripheralManager (server) implementation.
	servicesMu sync.Mutex
	svcs       []*Service
	attrs      *attrRange

	devID   int
	chkLE   bool
	maxConn int

	advData   *cmd.LESetAdvertisingData
	scanResp  *cmd.LESetScanResponseData
	advParam  *cmd.LESetAdvertisingParameters
	scanParam *cmd.LESetScanParameters
}

func NewDevice(opts ...Option) (Device, error) {
	d := &device{
		maxConn: 1,    // Support 1 connection at a time.
		devID:   -1,   // Find an available HCI device.
		chkLE:   true, // Check if the device supports LE.

		advParam: &cmd.LESetAdvertisingParameters{
			AdvertisingIntervalMin:  0x800,     // [0x0800]: 0.625 ms * 0x0800 = 1280.0 ms
			AdvertisingIntervalMax:  0x800,     // [0x0800]: 0.625 ms * 0x0800 = 1280.0 ms
			AdvertisingType:         0x00,      // [0x00]: ADV_IND, 0x01: DIRECT(HIGH), 0x02: SCAN, 0x03: NONCONN, 0x04: DIRECT(LOW)
			OwnAddressType:          0x00,      // [0x00]: public, 0x01: random
			DirectAddressType:       0x00,      // [0x00]: public, 0x01: random
			DirectAddress:           [6]byte{}, // Public or Random Address of the device to be connected
			AdvertisingChannelMap:   0x7,       // [0x07] 0x01: ch37, 0x2: ch38, 0x4: ch39
			AdvertisingFilterPolicy: 0x00,
		},
		scanParam: &cmd.LESetScanParameters{
			LEScanType:           0x01,   // [0x00]: passive, 0x01: active
			LEScanInterval:       0x0010, // [0x10]: 0.625ms * 16
			LEScanWindow:         0x0010, // [0x10]: 0.625ms * 16
			OwnAddressType:       0x00,   // [0x00]: public, 0x01: random
			ScanningFilterPolicy: 0x00,   // [0x00]: accept all, 0x01: ignore non-white-listed.
		},
	}

	if err := d.Option(opts...); err != nil {
		return nil, err
	}
	h, err := linux.NewHCI(d.devID, d.chkLE, d.maxConn)
	if err != nil {
		return nil, err
	}

	d.hci = h
	d.closeHCI = h.Close
	d.initLifecycleLocked()
	return d, nil
}

func (d *device) initLifecycleLocked() {
	if d.ctx != nil {
		return
	}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	d.stopDone = make(chan struct{})
	d.lifecycleSlots = make(chan struct{}, max(1, d.maxConn)*2)
	d.requestSlots = make(chan struct{}, max(1, d.maxConn)*2)
	d.callbackSlots = make(chan struct{}, max(1, d.maxConn)*16)
}

func (d *device) Init(f func(Device, State)) error {
	d.mu.Lock()
	d.initLifecycleLocked()
	if d.stopping {
		d.mu.Unlock()
		return io.ErrClosedPipe
	}
	if d.initialized {
		d.mu.Unlock()
		return errors.New("device already initialized")
	}
	d.initialized = true
	d.state = StatePoweredOn
	d.stateChanged = f
	d.mu.Unlock()
	d.hci.AcceptMasterHandler = func(pd *linux.PlatData) {
		a := pd.Address
		d.mu.Lock()
		attrs := d.attrs
		d.mu.Unlock()
		c := newCentral(attrs, net.HardwareAddr(a[:]), pd.Conn)
		c.requestSlots, c.callbackSlots = d.requestSlots, d.callbackSlots
		stopCancel := context.AfterFunc(d.ctx, c.cancel)
		d.runConnection(c.Context(), true, func() {
			if d.centralConnected != nil {
				d.centralConnected(c)
			}
		}, c.loop, func() {
			if d.centralDisconnected != nil {
				d.centralDisconnected(c)
			}
		}, func() error {
			stopCancel()
			return c.Close()
		}, nil)
	}
	d.hci.AcceptSlaveHandler = func(pd *linux.PlatData) {
		d.acceptPeripheral(pd, nil)
	}
	d.hci.AdvertisementHandler = func(pd *linux.PlatData) {
		a := &Advertisement{}
		if err := a.unmarshall(pd.Data); err != nil {
			return
		}
		a.Connectable = pd.Connectable
		p := &peripheral{pd: pd, d: d}
		if d.peripheralDiscovered != nil {
			pd.Name = a.LocalName
			d.startCallback(func() { d.peripheralDiscovered(p, a, int(pd.RSSI)) })
		}
	}
	if f != nil {
		// Preserve state callback order without including user code in the core
		// shutdown wait. Either callback may itself call Stop.
		go func() {
			f(d, StatePoweredOn)
			<-d.stopDone
			f(d, StatePoweredOff)
		}()
	}
	return nil
}

func (d *device) startCallback(f func()) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopping {
		return false
	}
	return startCallout(d.callbackSlots, f)
}

// runConnection separates cancelable protocol work from ordered application
// callbacks. A stuck callback retains its lifecycle slot after disconnection.
func (d *device) runConnection(ctx context.Context, waitConnected bool, connected, run, disconnected func(), closeConn func() error, failed func(error)) {
	d.mu.Lock()
	if d.stopping {
		d.mu.Unlock()
		return
	}
	select {
	case d.lifecycleSlots <- struct{}{}:
	default:
		d.workers.Add(1)
		d.mu.Unlock()
		if failed != nil {
			failed(errors.New("connection lifecycle callback capacity exhausted"))
		}
		go func() { defer d.workers.Done(); _ = closeConn() }()
		return
	}
	d.workers.Add(1)
	d.mu.Unlock()
	ready, coreDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer d.workers.Done()
		defer close(coreDone)
		defer func() { _ = closeConn() }()
		// Servers must finish Connected registration before handling ATT.
		// Client callbacks may themselves issue synchronous ATT requests, so
		// their protocol loop must start without waiting for Connected.
		if waitConnected {
			select {
			case <-ctx.Done():
				return
			case <-d.ctx.Done():
				return
			case <-ready:
			}
		}
		if ctx.Err() == nil && d.ctx.Err() == nil {
			run()
		}
	}()
	go func() {
		defer func() { <-d.lifecycleSlots }()
		if d.ctx.Err() != nil {
			return
		}
		if ctx.Err() != nil {
			if failed != nil {
				failed(io.ErrClosedPipe)
			}
			return
		}
		connected()
		close(ready)
		<-coreDone
		d.mu.Lock()
		stopping := d.stopping
		d.mu.Unlock()
		if stopping {
			<-d.stopDone
		}
		disconnected()
	}()
}

// Stop closes the Linux device and waits for its protocol workers. Application
// callbacks are canceled cooperatively and may finish after Stop returns.
func (d *device) Stop() error {
	return d.StopContext(context.Background())
}

// StopContext starts final device shutdown once and waits for completion or
// ctx cancellation. Canceling ctx only stops this caller's wait; the shared
// shutdown continues until its resources and protocol workers are released.
func (d *device) StopContext(ctx context.Context) error {
	d.mu.Lock()
	d.initLifecycleLocked()
	if !d.stopping {
		d.stopping = true
		d.state = StatePoweredOff
		d.cancel()
		go func() {
			err := d.closeHCI()
			d.workers.Wait()
			d.stopErr = err
			close(d.stopDone)
		}()
	}
	done := d.stopDone
	d.mu.Unlock()
	select {
	case <-done:
		return d.stopErr
	default:
	}
	select {
	case <-done:
		return d.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *device) AddService(s *Service) error {
	d.servicesMu.Lock()
	defer d.servicesMu.Unlock()
	d.mu.Lock()
	if d.stopping {
		d.mu.Unlock()
		return io.ErrClosedPipe
	}
	services := append(append([]*Service(nil), d.svcs...), s)
	d.mu.Unlock()
	attrs := generateAttributes(services, uint16(1)) // ble attrs start at 1
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopping {
		return io.ErrClosedPipe
	}
	d.svcs, d.attrs = services, attrs
	return nil
}

func (d *device) RemoveAllServices() error {
	d.servicesMu.Lock()
	defer d.servicesMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopping {
		return io.ErrClosedPipe
	}
	d.svcs = nil
	d.attrs = nil
	return nil
}

func (d *device) SetServices(s []*Service) error {
	d.servicesMu.Lock()
	defer d.servicesMu.Unlock()
	d.mu.Lock()
	if d.stopping {
		d.mu.Unlock()
		return io.ErrClosedPipe
	}
	d.mu.Unlock()
	services := append([]*Service(nil), s...)
	attrs := generateAttributes(services, uint16(1)) // ble attrs start at 1
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopping {
		return io.ErrClosedPipe
	}
	d.svcs, d.attrs = services, attrs
	return nil
}

func (d *device) Advertise(a *AdvPacket) error {
	if err := a.Err(); err != nil {
		return err
	}
	d.advData = &cmd.LESetAdvertisingData{
		AdvertisingDataLength: uint8(a.Len()),
		AdvertisingData:       a.Bytes(),
	}

	if err := d.update(); err != nil {
		return err
	}

	return d.hci.SetAdvertiseEnable(true)
}

func (d *device) AdvertiseNameAndServices(name string, uu []UUID) error {
	a, scan := nameAndServicesPackets(name, uu)
	d.scanResp = scan
	return d.Advertise(a)
}

// Keep packet construction independent of controller IO so a replacement can
// explicitly clear the previous scan response when the name fits in advertising.
func nameAndServicesPackets(name string, uu []UUID) (*AdvPacket, *cmd.LESetScanResponseData) {
	a := &AdvPacket{}
	a.AppendFlags(flagGeneralDiscoverable | flagLEOnly)
	a.AppendUUIDFit(uu)

	if len(a.b)+len(name)+2 < MaxEIRPacketLength {
		a.AppendName(name)
		return a, &cmd.LESetScanResponseData{}
	}
	scan := (&AdvPacket{}).AppendName(name)
	return a, &cmd.LESetScanResponseData{
		ScanResponseDataLength: uint8(scan.Len()),
		ScanResponseData:       scan.Bytes(),
	}
}

func (d *device) AdvertiseIBeaconData(b []byte) error {
	a := &AdvPacket{}
	a.AppendFlags(flagGeneralDiscoverable | flagLEOnly)
	a.AppendManufacturerData(0x004C, b)
	d.advData = &cmd.LESetAdvertisingData{
		AdvertisingDataLength: uint8(a.Len()),
		AdvertisingData:       a.Bytes(),
	}

	return d.Advertise(a)
}

func (d *device) AdvertiseIBeacon(u UUID, major, minor uint16, pwr int8) error {
	b := make([]byte, 23)
	b[0] = 0x02                               // Data type: iBeacon
	b[1] = 0x15                               // Data length: 21 bytes
	copy(b[2:], reverse(u.b))                 // Big endian
	binary.BigEndian.PutUint16(b[18:], major) // Big endian
	binary.BigEndian.PutUint16(b[20:], minor) // Big endian
	b[22] = uint8(pwr)                        // Measured Tx Power
	return d.AdvertiseIBeaconData(b)
}

func (d *device) StopAdvertising() error {
	return d.hci.SetAdvertiseEnable(false)
}

func (d *device) Scan(ss []UUID, dup bool) {
	// TODO: filter
	d.hci.SetScanEnable(true, dup)
}

func (d *device) StopScanning() {
	d.hci.SetScanEnable(false, true)
}

func (d *device) Connect(p Peripheral) {
	d.connectionResult(p, d.hci.ConnectWithResult(p.(*peripheral).pd))
}

func (d *device) connectionResult(source Peripheral, result <-chan linux.ConnectionResult) {
	consume := func(outcome linux.ConnectionResult) {
		if outcome.Err != nil {
			d.connectionFailure(source, outcome.Err)
			return
		}
		d.acceptPeripheral(outcome.Peripheral, source)
	}
	select {
	case outcome := <-result:
		consume(outcome)
	default:
		go func() {
			select {
			case <-d.ctx.Done():
			case outcome := <-result:
				consume(outcome)
			}
		}()
	}
}

func (d *device) connectionFailure(source Peripheral, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	d.mu.Lock()
	if d.stopping || d.peripheralConnected == nil {
		d.mu.Unlock()
		return
	}
	f := d.peripheralConnected
	accepted := startCallout(d.callbackSlots, func() { f(source, err) })
	d.mu.Unlock()
	if !accepted {
		log.Printf("gatt: connection failure callback capacity exhausted for %s: %v", source.ID(), err)
	}
}

func (d *device) acceptPeripheral(pd *linux.PlatData, source Peripheral) {
	if pd == nil || pd.Conn == nil {
		if source != nil {
			d.connectionFailure(source, io.ErrClosedPipe)
		}
		return
	}
	p := &peripheral{
		d: d, pd: pd, l2c: pd.Conn, mtu: attDefaultMTU,
		reqc: make(chan message), quitc: make(chan struct{}),
		sub: newSubscriber(), callout: d.startCallback,
	}
	if source == nil {
		source = p
	}
	ctx := d.ctx
	if conn, ok := pd.Conn.(interface{ Context() context.Context }); ok {
		ctx = conn.Context()
	}
	failed := func(err error) {
		if conn, ok := pd.Conn.(interface{ ConnectionCanceled() bool }); ok && conn.ConnectionCanceled() {
			return
		}
		if errors.Is(err, io.ErrClosedPipe) {
			if cause := p.closedError(); cause != nil {
				err = cause
			}
		}
		d.connectionFailure(source, err)
	}
	d.runConnection(ctx, false, func() {
		if d.peripheralConnected != nil {
			d.peripheralConnected(p, nil)
		}
	}, p.loop, func() {
		if d.peripheralDisconnected != nil {
			d.peripheralDisconnected(p, nil)
		}
	}, pd.Conn.Close, failed)
}

func (d *device) CancelConnection(p Peripheral) {
	d.hci.CancelConnection(p.(*peripheral).pd)
}

func (d *device) SendHCIRawCommand(c cmd.CmdParam) ([]byte, error) {
	if d == nil || d.hci == nil {
		return nil, errors.New("device is not initialized")
	}
	return d.hci.SendRawCommand(c)
}

// Flush pending advertising settings to the device.
func (d *device) update() error {
	if d.advParam != nil {
		if err := d.hci.SendCmdWithAdvOff(d.advParam); err != nil {
			return err
		}
		d.advParam = nil
	}
	if d.scanResp != nil {
		if err := d.hci.SendCmdWithAdvOff(d.scanResp); err != nil {
			return err
		}
		d.scanResp = nil
	}
	if d.advData != nil {
		if err := d.hci.SendCmdWithAdvOff(d.advData); err != nil {
			return err
		}
		d.advData = nil
	}
	return nil
}
