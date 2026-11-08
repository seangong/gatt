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

	"github.com/seangong/gatt/v2/linux"
)

type peripheral struct {
	// NameChanged is called whenever the peripheral GAP device name has changed.
	NameChanged func(*peripheral)

	// ServicedModified is called when one or more service of a peripheral have changed.
	// A list of invalid service is provided in the parameter.
	ServicesModified func(*peripheral, []*Service)

	d    *device
	svcs []*Service

	sub *subscriber

	mtu uint16 // Owned by requestLoop after the connection starts.
	l2c io.ReadWriteCloser

	reqc       chan message
	quitc      chan struct{}
	stopOnce   sync.Once
	abortOnce  sync.Once
	stopErr    error // Published by closing quitc.
	callout    func(func()) bool
	notifyOnce sync.Once
	notifyGate chan struct{}

	pd *linux.PlatData // platform specific data
}

func (p *peripheral) Device() Device       { return p.d }
func (p *peripheral) ID() string           { return strings.ToUpper(net.HardwareAddr(p.pd.Address[:]).String()) }
func (p *peripheral) Name() string         { return p.pd.Name }
func (p *peripheral) Services() []*Service { return p.svcs }

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

func (p *peripheral) DiscoverServices(s []UUID) ([]*Service, error) {
	// TODO: implement the UUID filters
	// p.pd.Conn.Write([]byte{0x02, 0x87, 0x00}) // MTU
	done := false
	start := uint16(0x0001)
	for !done {
		op := byte(attOpReadByGroupReq)
		b := make([]byte, 7)
		b[0] = op
		binary.LittleEndian.PutUint16(b[1:3], start)
		binary.LittleEndian.PutUint16(b[3:5], 0xFFFF)
		binary.LittleEndian.PutUint16(b[5:7], 0x2800)

		var err error
		b, err = p.sendReq(op, b)
		if err != nil {
			if finish(op, start, b) {
				break
			}
			return nil, err
		}
		b = b[1:]
		l, b := int(b[0]), b[1:]
		switch {
		case l == 6 && (len(b)%6 == 0):
		case l == 20 && (len(b)%20 == 0):
		default:
			return nil, ErrInvalidLength
		}

		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			endh := binary.LittleEndian.Uint16(b[2:4])
			s := &Service{
				uuid: UUID{b[4:l]},
				h:    h,
				endh: endh,
			}
			p.svcs = append(p.svcs, s)
			b = b[l:]
			done = endh == 0xFFFF
			start = endh + 1
		}
	}
	return p.svcs, nil
}

func (p *peripheral) DiscoverIncludedServices(ss []UUID, s *Service) ([]*Service, error) {
	// TODO
	return nil, nil
}

func (p *peripheral) DiscoverCharacteristics(cs []UUID, s *Service) ([]*Characteristic, error) {
	// TODO: implement the UUID filters
	done := false
	start := s.h
	var prev *Characteristic
	for !done {
		op := byte(attOpReadByTypeReq)
		b := make([]byte, 7)
		b[0] = op
		binary.LittleEndian.PutUint16(b[1:3], start)
		binary.LittleEndian.PutUint16(b[3:5], s.endh)
		binary.LittleEndian.PutUint16(b[5:7], 0x2803)

		var err error
		b, err = p.sendReq(op, b)
		if err != nil {
			if finish(op, start, b) {
				break
			}
			return nil, err
		}
		b = b[1:]

		l, b := int(b[0]), b[1:]
		switch {
		case l == 7 && (len(b)%7 == 0):
		case l == 21 && (len(b)%21 == 0):
		default:
			return nil, ErrInvalidLength
		}

		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			props := Property(b[2])
			vh := binary.LittleEndian.Uint16(b[3:5])
			u := UUID{b[5:l]}
			s := searchService(p.svcs, h, vh)
			if s == nil {
				log.Printf("Can't find service range that contains 0x%04X - 0x%04X", h, vh)
				return nil, fmt.Errorf("Can't find service range that contains 0x%04X - 0x%04X", h, vh)
			}
			c := &Characteristic{
				uuid:  u,
				svc:   s,
				props: props,
				h:     h,
				vh:    vh,
			}
			s.chars = append(s.chars, c)
			b = b[l:]
			done = vh == s.endh
			start = vh + 1
			if prev != nil {
				prev.endh = c.h - 1
			}
			prev = c
		}
	}
	if len(s.chars) > 1 {
		s.chars[len(s.chars)-1].endh = s.endh
	}
	return s.chars, nil
}

func (p *peripheral) DiscoverDescriptors(ds []UUID, c *Characteristic) ([]*Descriptor, error) {
	// TODO: implement the UUID filters
	done := false
	start := c.vh + 1
	for !done {
		if c.endh == 0 {
			c.endh = c.svc.endh
		}
		op := byte(attOpFindInfoReq)
		b := make([]byte, 5)
		b[0] = op
		binary.LittleEndian.PutUint16(b[1:3], start)
		binary.LittleEndian.PutUint16(b[3:5], c.endh)

		var err error
		b, err = p.sendReq(op, b)
		if err != nil {
			if finish(attOpFindInfoReq, start, b) {
				break
			}
			return nil, err
		}
		b = b[1:]

		var l int
		f, b := int(b[0]), b[1:]
		switch {
		case f == 1 && (len(b)%4 == 0):
			l = 4
		case f == 2 && (len(b)%18 == 0):
			l = 18
		default:
			return nil, ErrInvalidLength
		}

		for len(b) != 0 {
			h := binary.LittleEndian.Uint16(b[:2])
			u := UUID{b[2:l]}
			d := &Descriptor{uuid: u, h: h, char: c}
			c.descs = append(c.descs, d)
			if u.Equal(attrClientCharacteristicConfigUUID) {
				c.cccd = d
			}
			b = b[l:]
			done = h == c.endh
			start = h + 1
		}
	}
	return c.descs, nil
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
	if c.cccd == nil {
		return errors.New("no cccd") // FIXME
	}
	p.notifyOnce.Do(func() { p.notifyGate = make(chan struct{}, 1) })
	if err := p.closedError(); err != nil {
		return err
	}
	select {
	case <-p.quitc:
		return p.stopErr
	case <-p.connectionDone():
		return p.closedError()
	case p.notifyGate <- struct{}{}:
	}
	defer func() { <-p.notifyGate }()
	if err := p.closedError(); err != nil {
		return err
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
	binary.LittleEndian.PutUint16(b[1:3], c.cccd.h)
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

func searchService(ss []*Service, start, end uint16) *Service {
	for _, s := range ss {
		if s.h < start && s.endh >= end {
			return s
		}
	}
	return nil
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

func (p *peripheral) requestLoop(rspc <-chan []byte) {
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
		n, err := p.l2c.Write(req.b)
		if err == nil && n != len(req.b) {
			err = io.ErrShortWrite
		}
		if err != nil {
			p.abort(fmt.Errorf("write ATT operation 0x%02x: %w", req.op, err))
			return
		}
		if !req.noRsp {
			select {
			case <-p.quitc:
				return
			case <-done:
				p.stop(io.ErrClosedPipe)
				return
			case result.b = <-rspc:
			}
			if err := validPeripheralResponse(req.op, result.b); err != nil {
				p.abort(err)
				return
			}
			if result.b[0] == attOpError {
				handle := binary.LittleEndian.Uint16(result.b[2:4])
				result.err = fmt.Errorf("ATT operation 0x%02x at handle 0x%04x: %w", req.op, handle, attEcode(result.b[4]))
			} else if req.op == attOpMtuReq {
				clientMTU := binary.LittleEndian.Uint16(req.b[1:3])
				serverMTU := binary.LittleEndian.Uint16(result.b[1:3])
				p.mtu = min(clientMTU, serverMTU)
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

func validPeripheralResponse(op byte, b []byte) error {
	if len(b) == 0 {
		return ErrInvalidLength
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
	rspc := make(chan []byte)
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		p.requestLoop(rspc)
	}()
	defer func() {
		p.stop(io.EOF)
		<-pumpDone
	}()

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
			select {
			case <-p.quitc:
				return
			case <-p.connectionDone():
				return
			case rspc <- b:
			}
			continue
		}

		if len(b) < 3 {
			p.abort(ErrInvalidLength)
			return
		}
		h := binary.LittleEndian.Uint16(b[1:3])
		f := p.sub.fn(h)
		if f == nil {
			log.Printf("notified by unsubscribed handle")
			// FIXME: terminate the connection?
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
	b := make([]byte, 3)
	op := byte(attOpMtuReq)
	b[0] = op
	binary.LittleEndian.PutUint16(b[1:3], uint16(mtu))

	_, err := p.sendReq(op, b)
	return err
}
