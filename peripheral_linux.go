package gatt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/seangong/gatt/v2/linux"
)

const attTransactionTimeout = 30 * time.Second

type peripheral struct {
	// NameChanged is called whenever the peripheral GAP device name has changed.
	NameChanged func(*peripheral)

	// ServicedModified is called when one or more service of a peripheral have changed.
	// A list of invalid service is provided in the parameter.
	ServicesModified func(*peripheral, []*Service)

	d      *device
	svcs   []*Service
	svcsMu sync.RWMutex

	servicesDiscovered        bool
	characteristicsDiscovered map[*Service]bool
	descriptorsDiscovered     map[*Characteristic]bool

	sub *subscriber

	mtu uint16 // Owned by requestLoop after the connection starts.
	l2c io.ReadWriteCloser

	reqc         chan message
	quitc        chan struct{}
	stopOnce     sync.Once
	abortOnce    sync.Once
	stopErr      error // Published by closing quitc.
	callout      func(func()) bool
	metadataOnce sync.Once
	metadataGate chan struct{}
	responseMu   sync.Mutex
	// responsePending is registered before Write and cleared before its reader
	// acknowledgement. Only the request owner changes it, under responseMu.
	responsePending bool

	pd *linux.PlatData // platform specific data
}

func (p *peripheral) Device() Device { return p.d }
func (p *peripheral) ID() string     { return strings.ToUpper(net.HardwareAddr(p.pd.Address[:]).String()) }
func (p *peripheral) Name() string   { return p.pd.Name }
func (p *peripheral) Services() []*Service {
	p.svcsMu.RLock()
	defer p.svcsMu.RUnlock()
	return p.svcs
}

func finish(op byte, h uint16, b []byte) bool {
	if len(b) != 5 {
		return false
	}
	done := b[0] == attOpError && b[1] == op && b[2] == byte(h) && b[3] == byte(h>>8)
	e := attEcode(b[4])
	if e != attEcodeAttrNotFound {
		// log.Printf("unexpected protocol error: %s", e)
		// FIXME: terminate the connection
	}
	return done && e == attEcodeAttrNotFound
}

// lockMetadata serializes complete discovery and CCCD transactions. It is not
// held by the response pump or notification callbacks, so transport I/O and
// callbacks can finish while a transaction is waiting for its response.
func (p *peripheral) lockMetadata() error {
	if p.l2c == nil || p.reqc == nil || p.quitc == nil {
		return io.ErrClosedPipe
	}
	if err := p.closedError(); err != nil {
		return err
	}
	p.metadataOnce.Do(func() { p.metadataGate = make(chan struct{}, 1) })
	select {
	case <-p.quitc:
		return p.stopErr
	case <-p.connectionDone():
		return p.closedError()
	case p.metadataGate <- struct{}{}:
	}
	if err := p.closedError(); err != nil {
		p.unlockMetadata()
		return err
	}
	return nil
}

func (p *peripheral) unlockMetadata() { <-p.metadataGate }

func (p *peripheral) databaseChanged(kind string) error {
	err := fmt.Errorf("discovered %s changed; reconnect before using cached handles: %w", kind, ErrDatabaseChanged)
	p.abort(err)
	return err
}

func (p *peripheral) DiscoverServices(s []UUID) ([]*Service, error) {
	// TODO: implement the UUID filters
	// p.pd.Conn.Write([]byte{0x02, 0x87, 0x00}) // MTU
	if err := p.lockMetadata(); err != nil {
		return nil, err
	}
	defer p.unlockMetadata()
	var services []*Service
	for start := 1; start <= 0xffff; {
		op := byte(attOpReadByGroupReq)
		b := make([]byte, 7)
		b[0] = op
		binary.LittleEndian.PutUint16(b[1:3], uint16(start))
		binary.LittleEndian.PutUint16(b[3:5], 0xffff)
		binary.LittleEndian.PutUint16(b[5:7], 0x2800)

		b, err := p.sendReq(op, b)
		if err != nil {
			if finish(op, uint16(start), b) {
				break
			}
			return nil, err
		}
		if len(b) < 2 {
			return nil, ErrInvalidLength
		}
		l, b := int(b[1]), b[2:]
		if (l != 6 && l != 20) || len(b) == 0 || len(b)%l != 0 {
			return nil, ErrInvalidLength
		}
		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			endh := binary.LittleEndian.Uint16(b[2:4])
			if int(h) < start || endh < h {
				return nil, fmt.Errorf("invalid service range 0x%04x-0x%04x at 0x%04x", h, endh, start)
			}
			services = append(services, &Service{uuid: UUID{b[4:l]}, h: h, endh: endh})
			start = int(endh) + 1
			b = b[l:]
		}
	}
	if err := p.closedError(); err != nil {
		return nil, err
	}
	if p.servicesDiscovered {
		if len(p.svcs) != len(services) {
			return nil, p.databaseChanged("services")
		}
		for i, s := range services {
			old := p.svcs[i]
			if old.h != s.h || old.endh != s.endh || !old.uuid.Equal(s.uuid) {
				return nil, p.databaseChanged("services")
			}
		}
		return p.svcs, nil
	}
	p.svcsMu.Lock()
	p.svcs = services
	p.svcsMu.Unlock()
	p.servicesDiscovered = true
	return services, nil
}

func (p *peripheral) DiscoverIncludedServices(ss []UUID, s *Service) ([]*Service, error) {
	// TODO
	return nil, nil
}

func (p *peripheral) DiscoverCharacteristics(cs []UUID, s *Service) ([]*Characteristic, error) {
	// TODO: implement the UUID filters
	if err := p.lockMetadata(); err != nil {
		return nil, err
	}
	defer p.unlockMetadata()
	if s == nil || s.h == 0 || s.endh < s.h {
		return nil, errors.New("invalid service range")
	}
	var chars []*Characteristic
	for start := int(s.h); start <= int(s.endh); {
		op := byte(attOpReadByTypeReq)
		b := make([]byte, 7)
		b[0] = op
		binary.LittleEndian.PutUint16(b[1:3], uint16(start))
		binary.LittleEndian.PutUint16(b[3:5], s.endh)
		binary.LittleEndian.PutUint16(b[5:7], 0x2803)

		b, err := p.sendReq(op, b)
		if err != nil {
			if finish(op, uint16(start), b) {
				break
			}
			return nil, err
		}
		if len(b) < 2 {
			return nil, ErrInvalidLength
		}
		l, b := int(b[1]), b[2:]
		if (l != 7 && l != 21) || len(b) == 0 || len(b)%l != 0 {
			return nil, ErrInvalidLength
		}
		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			vh := binary.LittleEndian.Uint16(b[3:5])
			if int(h) < start || h <= s.h || vh <= h || vh > s.endh ||
				(len(chars) != 0 && h <= chars[len(chars)-1].vh) {
				return nil, fmt.Errorf("invalid characteristic handles 0x%04x-0x%04x in service 0x%04x-0x%04x", h, vh, s.h, s.endh)
			}
			chars = append(chars, &Characteristic{uuid: UUID{b[5:l]}, svc: s, props: Property(b[2]), h: h, vh: vh})
			start = int(h) + 1
			b = b[l:]
		}
	}
	for i, c := range chars {
		c.endh = s.endh
		if i+1 < len(chars) {
			c.endh = chars[i+1].h - 1
		}
	}
	if err := p.closedError(); err != nil {
		return nil, err
	}
	if p.characteristicsDiscovered[s] {
		oldChars := s.Characteristics()
		if len(oldChars) != len(chars) {
			return nil, p.databaseChanged("characteristics")
		}
		for i, c := range chars {
			old := oldChars[i]
			if old.h != c.h || old.vh != c.vh || old.endh != c.endh || old.props != c.props || !old.uuid.Equal(c.uuid) {
				return nil, p.databaseChanged("characteristics")
			}
		}
		return oldChars, nil
	}
	s.SetCharacteristics(chars)
	if p.characteristicsDiscovered == nil {
		p.characteristicsDiscovered = make(map[*Service]bool)
	}
	p.characteristicsDiscovered[s] = true
	return chars, nil
}

func (p *peripheral) DiscoverDescriptors(ds []UUID, c *Characteristic) ([]*Descriptor, error) {
	// TODO: implement the UUID filters
	if err := p.lockMetadata(); err != nil {
		return nil, err
	}
	defer p.unlockMetadata()
	if c == nil || c.vh == 0 {
		return nil, errors.New("invalid characteristic value handle")
	}
	endh := c.endh
	if endh == 0 && c.svc != nil {
		endh = c.svc.endh
	}
	if c.vh > endh {
		return nil, fmt.Errorf("invalid descriptor range after 0x%04x ending at 0x%04x", c.vh, endh)
	}
	var descs []*Descriptor
	var cccd *Descriptor
	for start := int(c.vh) + 1; start <= int(endh); {
		op := byte(attOpFindInfoReq)
		b := make([]byte, 5)
		b[0] = op
		binary.LittleEndian.PutUint16(b[1:3], uint16(start))
		binary.LittleEndian.PutUint16(b[3:5], endh)

		b, err := p.sendReq(op, b)
		if err != nil {
			if finish(op, uint16(start), b) {
				break
			}
			return nil, err
		}
		if len(b) < 2 {
			return nil, ErrInvalidLength
		}
		var l int
		switch b[1] {
		case 1:
			l = 4
		case 2:
			l = 18
		default:
			return nil, ErrInvalidLength
		}
		b = b[2:]
		if len(b) == 0 || len(b)%l != 0 {
			return nil, ErrInvalidLength
		}
		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			if int(h) < start || h > endh {
				return nil, fmt.Errorf("invalid descriptor handle 0x%04x in range 0x%04x-0x%04x", h, start, endh)
			}
			d := &Descriptor{uuid: UUID{b[2:l]}, h: h, char: c}
			descs = append(descs, d)
			if d.uuid.Equal(attrClientCharacteristicConfigUUID) {
				cccd = d
			}
			start = int(h) + 1
			b = b[l:]
		}
	}
	if err := p.closedError(); err != nil {
		return nil, err
	}
	if p.descriptorsDiscovered[c] {
		oldDescs := c.Descriptors()
		if len(oldDescs) != len(descs) {
			return nil, p.databaseChanged("descriptors")
		}
		for i, d := range descs {
			old := oldDescs[i]
			if old.h != d.h || !old.uuid.Equal(d.uuid) {
				return nil, p.databaseChanged("descriptors")
			}
		}
		return oldDescs, nil
	}
	c.descsMu.Lock()
	c.descs, c.cccd = descs, cccd
	c.descsMu.Unlock()
	if p.descriptorsDiscovered == nil {
		p.descriptorsDiscovered = make(map[*Characteristic]bool)
	}
	p.descriptorsDiscovered[c] = true
	return descs, nil
}

func (p *peripheral) ReadCharacteristic(c *Characteristic) ([]byte, error) {
	result := p.readCharacteristic(c)
	if result.err != nil {
		return nil, result.err
	}
	return result.b[1:], nil
}

func (p *peripheral) readCharacteristic(c *Characteristic) peripheralResult {
	b := make([]byte, 3)
	op := byte(attOpReadReq)
	b[0] = op
	binary.LittleEndian.PutUint16(b[1:3], c.vh)
	return p.send(op, b, false)
}

func (p *peripheral) ReadLongCharacteristic(c *Characteristic) ([]byte, error) {
	// The spec says that a read blob request should fail if the characteristic
	// is smaller than mtu - 1.  To simplify the API, the first read is done
	// with a regular read request.  If the buffer received is equal to mtu -1,
	// then we read the rest of the data using read blob.
	first := p.readCharacteristic(c)
	if first.err != nil {
		return nil, first.err
	}
	firstRead := first.b[1:]
	if len(firstRead) < int(first.mtu)-1 {
		return firstRead, nil
	}

	var buf bytes.Buffer
	buf.Write(firstRead)
	off := len(firstRead)
	firstBlob := true
	for {
		if off > 0xffff {
			return nil, fmt.Errorf("read blob offset %d exceeds uint16: %w", off, ErrInvalidLength)
		}
		b := make([]byte, 5)
		op := byte(attOpReadBlobReq)
		b[0] = op
		binary.LittleEndian.PutUint16(b[1:3], c.vh)
		binary.LittleEndian.PutUint16(b[3:5], uint16(off))

		result := p.send(op, b, false)
		if result.err != nil {
			// A fixed value that exactly filled the first response may reject
			// the first blob. An intervening MTU change cannot prove completeness.
			if firstBlob && result.mtu == first.mtu && len(firstRead) == int(first.mtu)-1 &&
				errors.Is(result.err, attEcodeAttrNotLong) && len(result.b) == 5 &&
				result.b[1] == attOpReadBlobReq && binary.LittleEndian.Uint16(result.b[2:4]) == c.vh {
				return firstRead, nil
			}
			return nil, result.err
		}
		firstBlob = false
		b = result.b[1:]
		if len(b) == 0 {
			break
		}
		buf.Write(b)
		off += len(b)
		if len(b) < int(result.mtu)-1 {
			break
		}
	}
	return buf.Bytes(), nil
}

func (p *peripheral) WriteCharacteristic(c *Characteristic, value []byte, noRsp bool) error {
	b := make([]byte, 3+len(value))
	op := byte(attOpWriteReq)
	b[0] = op
	if noRsp {
		b[0] = attOpWriteCmd
	}
	binary.LittleEndian.PutUint16(b[1:3], c.vh)
	copy(b[3:], value)

	if noRsp {
		return p.sendCmd(attOpWriteCmd, b)
	}
	_, err := p.sendReq(op, b)
	return err
}

func (p *peripheral) ReadDescriptor(d *Descriptor) ([]byte, error) {
	b := make([]byte, 3)
	op := byte(attOpReadReq)
	b[0] = op
	binary.LittleEndian.PutUint16(b[1:3], d.h)

	b, err := p.sendReq(op, b)
	if err != nil {
		return nil, err
	}
	b = b[1:]
	return b, nil
}

func (p *peripheral) WriteDescriptor(d *Descriptor, value []byte) error {
	b := make([]byte, 3+len(value))
	op := byte(attOpWriteReq)
	b[0] = op
	binary.LittleEndian.PutUint16(b[1:3], d.h)
	copy(b[3:], value)

	_, err := p.sendReq(op, b)
	return err
}

func (p *peripheral) setNotifyValue(c *Characteristic, flag uint16,
	f func(*Characteristic, []byte, error)) error {
	if err := p.lockMetadata(); err != nil {
		return err
	}
	defer p.unlockMetadata()
	cccd := c.Descriptor()
	if cccd == nil {
		return errors.New("no cccd") // FIXME
	}
	ccc := uint16(0)
	previous := p.sub.fn(c.vh)
	if f != nil {
		ccc = flag
		p.sub.subscribe(c.vh, func(b []byte, err error) { f(c, b, err) })
	}
	b := make([]byte, 5)
	op := byte(attOpWriteReq)
	b[0] = op
	binary.LittleEndian.PutUint16(b[1:3], cccd.h)
	binary.LittleEndian.PutUint16(b[3:5], ccc)

	if _, err := p.sendReq(op, b); err != nil {
		if f != nil {
			if previous == nil {
				p.sub.unsubscribe(c.vh)
			} else {
				p.sub.subscribe(c.vh, previous)
			}
		}
		return err
	}
	if f == nil {
		p.sub.unsubscribe(c.vh)
	}
	return nil
}

func (p *peripheral) SetNotifyValue(c *Characteristic,
	f func(*Characteristic, []byte, error)) error {
	return p.setNotifyValue(c, gattCCCNotifyFlag, f)
}

func (p *peripheral) SetIndicateValue(c *Characteristic,
	f func(*Characteristic, []byte, error)) error {
	return p.setNotifyValue(c, gattCCCIndicateFlag, f)
}

func (p *peripheral) ReadRSSI() int {
	// TODO: implement
	return -1
}

// TODO: unifiy the message with OS X pots and refactor
type message struct {
	op    byte
	b     []byte
	noRsp bool
	rspc  chan peripheralResult
}

type peripheralResult struct {
	b   []byte
	err error
	mtu uint16 // MTU in effect when this request was sent.
}

func (p *peripheral) connectionDone() <-chan struct{} {
	if c, ok := p.l2c.(interface{ Context() context.Context }); ok {
		return c.Context().Done()
	}
	return nil
}

func (p *peripheral) stop(err error) {
	if err == nil {
		err = io.ErrClosedPipe
	}
	p.stopOnce.Do(func() {
		p.stopErr = err
		close(p.quitc)
	})
}

func (p *peripheral) abort(err error) {
	p.stop(err)
	p.abortOnce.Do(func() {
		if c, ok := p.l2c.(interface{ Abort(error) }); ok {
			c.Abort(err)
			return
		}
		if closeErr := p.l2c.Close(); closeErr != nil {
			log.Printf("peripheral: close connection after %v: %v", err, closeErr)
		}
	})
}

func (p *peripheral) closedError() error {
	select {
	case <-p.quitc:
		return p.stopErr
	default:
	}
	select {
	case <-p.connectionDone():
		return io.ErrClosedPipe
	default:
		return nil
	}
}

func (p *peripheral) sendCmd(op byte, b []byte) error {
	return p.send(op, b, true).err
}

func (p *peripheral) sendReq(op byte, b []byte) ([]byte, error) {
	result := p.send(op, b, false)
	return result.b, result.err
}

func (p *peripheral) send(op byte, b []byte, noRsp bool) peripheralResult {
	if p.l2c == nil || p.reqc == nil || p.quitc == nil {
		return peripheralResult{err: io.ErrClosedPipe}
	}
	if err := p.closedError(); err != nil {
		return peripheralResult{err: err}
	}
	m := message{op: op, b: b, noRsp: noRsp, rspc: make(chan peripheralResult, 1)}
	done := p.connectionDone()
	select {
	case <-p.quitc:
		return peripheralResult{err: p.stopErr}
	case <-done:
		return peripheralResult{err: p.closedError()}
	case p.reqc <- m:
	}
	select {
	case <-p.quitc:
		return peripheralResult{err: p.stopErr}
	case <-done:
		return peripheralResult{err: p.closedError()}
	case result := <-m.rspc:
		return result
	}
}

func (p *peripheral) requestLoop(rspc <-chan []byte, processed chan<- uint16) {
	defer func() {
		p.responseMu.Lock()
		p.responsePending = false
		p.responseMu.Unlock()
	}()
	done := p.connectionDone()
	for {
		var req message
		select {
		case <-p.quitc:
			return
		case <-done:
			p.stop(io.ErrClosedPipe)
			return
		case req = <-p.reqc:
		}
		if err := p.closedError(); err != nil {
			return
		}
		result := peripheralResult{mtu: p.mtu}
		if err := validPeripheralRequest(req.op, req.b, p.mtu); err != nil {
			result.err = err
			select {
			case <-p.quitc:
				return
			case <-done:
				p.stop(io.ErrClosedPipe)
				return
			case req.rspc <- result:
				continue
			}
		}
		if !req.noRsp {
			// A peer can respond before the transport's Write returns. Register
			// this transaction first, without holding the mutex across IO.
			p.responseMu.Lock()
			p.responsePending = true
			p.responseMu.Unlock()
		}
		n, err := p.l2c.Write(req.b)
		if err == nil && n != len(req.b) {
			err = io.ErrShortWrite
		}
		if err != nil {
			p.abort(fmt.Errorf("write ATT operation 0x%02x: %w", req.op, err))
			return
		}
		if !req.noRsp {
			// Queueing and local transport writes are not response-wait time.
			// A timed-out bearer is retired before another request can be sent.
			timer := time.NewTimer(attTransactionTimeout)
			select {
			case <-p.quitc:
				timer.Stop()
				return
			case <-done:
				timer.Stop()
				p.stop(io.ErrClosedPipe)
				return
			case <-timer.C:
				p.abort(fmt.Errorf("ATT operation 0x%02x response timeout: %w", req.op, context.DeadlineExceeded))
				return
			case result.b = <-rspc:
				timer.Stop()
			}
			p.responseMu.Lock()
			p.responsePending = false
			p.responseMu.Unlock()
			if err := validPeripheralResponse(req.op, result.b, result.mtu); err != nil {
				p.abort(err)
				return
			}
			if result.b[0] == attOpError {
				handle := binary.LittleEndian.Uint16(result.b[2:4])
				result.err = fmt.Errorf("ATT operation 0x%02x at handle 0x%04x: %w", req.op, handle, attEcode(result.b[4]))
			} else if req.op == attOpMtuReq {
				clientMTU := binary.LittleEndian.Uint16(req.b[1:3])
				serverMTU := binary.LittleEndian.Uint16(result.b[1:3])
				p.mtu = attDefaultMTU
				if clientMTU >= attDefaultMTU && serverMTU >= attDefaultMTU {
					p.mtu = min(clientMTU, serverMTU)
				}
			}
			// The reader must apply this MTU before reading the next packet.
			// A channel handoff alone does not mean the response was processed.
			select {
			case <-p.quitc:
				return
			case <-done:
				p.stop(io.ErrClosedPipe)
				return
			case processed <- p.mtu:
			}
		}
		select {
		case <-p.quitc:
			return
		case <-done:
			p.stop(io.ErrClosedPipe)
			return
		case req.rspc <- result:
		}
	}
}

func validPeripheralRequest(op byte, b []byte, mtu uint16) error {
	switch op {
	case attOpWriteReq, attOpWriteCmd:
		if len(b) < 3 || len(b) > int(mtu) || len(b)-3 > attMaxValueLen {
			return fmt.Errorf("ATT write PDU length %d exceeds MTU %d or value limit %d: %w", len(b), mtu, attMaxValueLen, ErrInvalidLength)
		}
	case attOpMtuReq:
		if len(b) != 3 {
			return ErrInvalidLength
		}
		if requested := binary.LittleEndian.Uint16(b[1:3]); requested < attDefaultMTU || requested > attMaxMTU {
			return fmt.Errorf("unsupported ATT MTU %d (supported %d..%d): %w", requested, attDefaultMTU, attMaxMTU, ErrInvalidLength)
		}
	}
	return nil
}

func validPeripheralResponse(op byte, b []byte, mtu uint16) error {
	if len(b) == 0 {
		return ErrInvalidLength
	}
	if len(b) > int(mtu) {
		return fmt.Errorf("ATT response length %d exceeds transaction MTU %d: %w", len(b), mtu, ErrInvalidLength)
	}
	if b[0] == attOpError {
		if len(b) != 5 {
			return ErrInvalidLength
		}
		if b[1] == op {
			return nil
		}
	} else if b[0] == attRspFor[op] {
		switch op {
		case attOpMtuReq:
			if len(b) != 3 {
				return ErrInvalidLength
			}
		case attOpFindInfoReq, attOpReadByTypeReq, attOpReadByGroupReq:
			if len(b) < 2 {
				return ErrInvalidLength
			}
		case attOpWriteReq:
			if len(b) != 1 {
				return ErrInvalidLength
			}
		}
		return nil
	}
	return fmt.Errorf("ATT operation 0x%02x received mismatched response 0x%02x", op, b[0])
}

func (p *peripheral) loop() {
	// After startup only requestLoop reads or changes p.mtu. The reader keeps a
	// local size, synchronized after each response before receiving more PDUs.
	receiveMTU := p.mtu
	rspc := make(chan []byte)
	processed := make(chan uint16)
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		p.requestLoop(rspc, processed)
	}()
	defer func() {
		p.stop(io.EOF)
		<-pumpDone
	}()
	p.readLoop(rspc, processed, receiveMTU)
}

func (p *peripheral) readLoop(rspc chan<- []byte, processed <-chan uint16, receiveMTU uint16) {
	// L2CAP implementations shall support a minimum MTU size of 48 bytes.
	// The default value is 672 bytes
	buf := make([]byte, 672)

	// Handling response or notification/indication
	for {
		if err := p.closedError(); err != nil {
			return
		}
		n, err := p.l2c.Read(buf)
		if n == 0 || err != nil {
			if err == nil {
				err = io.EOF
			}
			if p.closedError() == nil {
				p.abort(err)
			}
			return
		}

		b := make([]byte, n)
		copy(b, buf)

		if (b[0] != attOpHandleNotify) && (b[0] != attOpHandleInd) {
			p.responseMu.Lock()
			pending := p.responsePending
			p.responseMu.Unlock()
			if !pending {
				// An idle response cannot belong to a future transaction or
				// keep notifications queued behind a response handoff.
				continue
			}
			select {
			case <-p.quitc:
				return
			case <-p.connectionDone():
				return
			case rspc <- b:
			}
			select {
			case <-p.quitc:
				return
			case <-p.connectionDone():
				return
			case receiveMTU = <-processed:
			}
			continue
		}

		if len(b) < 3 {
			p.abort(ErrInvalidLength)
			return
		}
		h := binary.LittleEndian.Uint16(b[1:3])
		validValue := h != 0 && len(b) <= int(receiveMTU) && len(b)-3 <= attMaxValueLen
		if validValue {
			f := p.sub.fn(h)
			if f == nil {
				log.Printf("notified by unsubscribed handle")
			} else {
				callback := func() { f(b[3:], nil) }
				if p.callout != nil {
					if !p.callout(callback) {
						p.abort(errors.New("peripheral notification callback capacity exceeded"))
						return
					}
				} else {
					go callback()
				}
			}
		}

		if b[0] == attOpHandleInd {
			// write aknowledgement for indication
			n, err := p.l2c.Write([]byte{attOpHandleCnf})
			if err == nil && n != 1 {
				err = io.ErrShortWrite
			}
			if err != nil {
				p.abort(fmt.Errorf("write ATT indication confirmation: %w", err))
				return
			}
		}

	}
}

func (p *peripheral) SetMTU(mtu uint16) error {
	if mtu < attDefaultMTU || mtu > attMaxMTU {
		return fmt.Errorf("unsupported ATT MTU %d (supported %d..%d): %w", mtu, attDefaultMTU, attMaxMTU, ErrInvalidLength)
	}
	b := make([]byte, 3)
	op := byte(attOpMtuReq)
	b[0] = op
	binary.LittleEndian.PutUint16(b[1:3], uint16(mtu))

	_, err := p.sendReq(op, b)
	return err
}
