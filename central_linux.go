package gatt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type security int

const (
	securityLow = iota
	securityMed
	securityHigh
)

type central struct {
	attrs         *attrRange
	mtu           uint16
	mtuMu         sync.RWMutex
	addr          net.HardwareAddr
	security      security
	l2conn        io.ReadWriteCloser
	ctx           context.Context
	cancel        context.CancelFunc
	handlerBusy   chan struct{}
	requestSlots  chan struct{}
	callbackSlots chan struct{}
	sendGate      chan struct{}
	notifiers     map[uint16]*notifier
	notifiersmu   *sync.Mutex
	indication    *pendingIndication // Protected by notifiersmu.
}

type pendingIndication struct {
	timer *time.Timer
}

const indicationTimeout = 30 * time.Second

func newCentral(a *attrRange, addr net.HardwareAddr, l2conn io.ReadWriteCloser) *central {
	if a == nil {
		a = &attrRange{base: 1}
	}
	ctx := context.Background()
	if conn, ok := l2conn.(interface{ Context() context.Context }); ok {
		ctx = conn.Context()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &central{
		attrs:         a,
		mtu:           23,
		addr:          addr,
		security:      securityLow,
		l2conn:        l2conn,
		ctx:           ctx,
		cancel:        cancel,
		handlerBusy:   make(chan struct{}, 1),
		requestSlots:  make(chan struct{}, 1),
		callbackSlots: make(chan struct{}, 16),
		sendGate:      make(chan struct{}, 1),
		notifiers:     make(map[uint16]*notifier),
		notifiersmu:   &sync.Mutex{},
	}
}

func (c *central) ID() string {
	return c.addr.String()
}

func (c *central) Close() error {
	c.cancel()
	c.notifiersmu.Lock()
	for _, n := range c.notifiers {
		n.stop()
	}
	if c.indication != nil {
		c.indication.timer.Stop()
		c.indication = nil
	}
	c.notifiersmu.Unlock()
	return c.l2conn.Close()
}

func (c *central) Context() context.Context { return c.ctx }

// A canceled handler retains its slot until it actually returns. This bounds
// application code that ignores cancellation across disconnected centrals.
func (c *central) callHandler(f func()) bool {
	if c.ctx.Err() != nil {
		return false
	}
	select {
	case c.handlerBusy <- struct{}{}:
	default:
		return false
	}
	select {
	case c.requestSlots <- struct{}{}:
	default:
		<-c.handlerBusy
		return false
	}
	done := make(chan struct{})
	go func() {
		f()
		<-c.requestSlots
		<-c.handlerBusy
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-c.ctx.Done():
		return false
	}
}

func startCallout(slots chan struct{}, f func()) bool {
	select {
	case slots <- struct{}{}:
	default:
		return false
	}
	go func() {
		defer func() { <-slots }()
		f()
	}()
	return true
}

func (c *central) MTU() int {
	c.mtuMu.RLock()
	defer c.mtuMu.RUnlock()
	return int(c.mtu)
}

func (n *notifier) Cap() int {
	return min(n.central.MTU()-3, attMaxValueLen)
}

func (c *central) loop() {
	// Keep confirmations flowing even while an application handler is blocked.
	// The bounded queue also prevents commands from growing without limit.
	requests := make(chan []byte, 16)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer c.Close()
		for {
			select {
			case <-c.ctx.Done():
				return
			case request := <-requests:
				if c.ctx.Err() != nil {
					return
				}
				if !c.handleAndRespond(request) {
					return
				}
			}
		}
	}()
	defer func() {
		c.Close()
		<-done
	}()
	for {
		// L2CAP implementations shall support a minimum MTU size of 48 bytes.
		// The default value is 672 bytes
		b := make([]byte, 672)
		n, err := c.l2conn.Read(b)
		if n == 0 || err != nil || c.ctx.Err() != nil {
			return
		}
		if b[0] == attOpHandleCnf {
			c.handleReq(b[:n:n])
			continue
		}
		select {
		case <-c.ctx.Done():
			return
		case requests <- b[:n:n]:
		default:
			return
		}
	}
}

func (c *central) lockSend() bool {
	select {
	case <-c.ctx.Done():
		return false
	case c.sendGate <- struct{}{}:
	}
	if c.ctx.Err() != nil {
		c.unlockSend()
		return false
	}
	return true
}

func (c *central) unlockSend() { <-c.sendGate }

// An MTU response and its effective size are published in wire order with
// notifications. Ordinary handlers run outside the send gate: they may write
// a notification themselves or wait for a confirmation.
func (c *central) handleAndRespond(request []byte) bool {
	exchange := len(request) != 0 && request[0] == attOpMtuReq
	if exchange {
		if !c.lockSend() {
			return false
		}
		defer c.unlockSend()
	}
	rsp := c.handleReq(request)
	if c.ctx.Err() != nil {
		return false
	}
	if rsp == nil {
		return true
	}
	if !exchange {
		if !c.lockSend() {
			return false
		}
		defer c.unlockSend()
	}
	if n, err := c.l2conn.Write(rsp); err != nil || n != len(rsp) {
		return false
	}
	if exchange && rsp[0] == attOpMtuRsp {
		c.mtuMu.Lock()
		c.mtu = binary.LittleEndian.Uint16(rsp[1:3])
		c.mtuMu.Unlock()
	}
	return true
}

// handleReq dispatches a raw request from the central shim
// to an appropriate handler, based on its type.
func (c *central) handleReq(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	reqType, req := b[0], b[1:]
	if reqType&0x40 != 0 && reqType != attOpWriteCmd {
		return nil // Unsupported commands never generate ATT responses.
	}
	if reqType == attOpHandleCnf {
		if len(req) == 0 {
			c.confirmIndication()
		}
		return nil
	}
	if len(b) > c.MTU() {
		if reqType&0x40 != 0 { // The command flag forbids an ATT response.
			return nil
		}
		return attErrorRsp(reqType, 0, attEcodeInvalidPDU)
	}
	if !validATTParamLength(reqType, len(req)) {
		if reqType == attOpWriteCmd {
			return nil
		}
		return attErrorRsp(reqType, 0, attEcodeInvalidPDU)
	}
	switch reqType {
	case attOpFindInfoReq, attOpFindByTypeValueReq, attOpReadByTypeReq, attOpReadByGroupReq:
		start, end := readHandleRange(req)
		if start == 0 || start > end {
			return attErrorRsp(reqType, start, attEcodeInvalidHandle)
		}
	}

	var resp []byte
	switch reqType {
	case attOpMtuReq:
		resp = c.handleMTU(req)
	case attOpFindInfoReq:
		resp = c.handleFindInfo(req)
	case attOpFindByTypeValueReq:
		resp = c.handleFindByTypeValue(req)
	case attOpReadByTypeReq:
		resp = c.handleReadByType(req)
	case attOpReadReq:
		resp = c.handleRead(req)
	case attOpReadBlobReq:
		resp = c.handleReadBlob(req)
	case attOpReadByGroupReq:
		resp = c.handleReadByGroup(req)
	case attOpWriteReq:
		resp = c.handleWrite(reqType, req)
	case attOpWriteCmd:
		c.handleWrite(reqType, req)
		return nil
	case attOpReadMultiReq, attOpPrepWriteReq, attOpExecWriteReq, attOpSignedWriteCmd:
		fallthrough
	default:
		resp = attErrorRsp(reqType, 0x0000, attEcodeReqNotSupp)
	}
	return resp
}

func validATTParamLength(op byte, n int) bool {
	switch op {
	case attOpMtuReq, attOpReadReq:
		return n == 2
	case attOpFindInfoReq, attOpReadBlobReq:
		return n == 4
	case attOpReadByTypeReq, attOpReadByGroupReq:
		return n == 6 || n == 20
	case attOpFindByTypeValueReq:
		return n >= 6
	case attOpWriteReq, attOpWriteCmd:
		return n >= 2
	default:
		return true // Unsupported opcodes are handled by handleReq.
	}
}

func (c *central) handleMTU(b []byte) []byte {
	mtu := binary.LittleEndian.Uint16(b[:2])
	if mtu < 23 {
		mtu = 23
	}
	if mtu > attMaxMTU {
		mtu = attMaxMTU
	}
	return []byte{attOpMtuRsp, uint8(mtu), uint8(mtu >> 8)}
}

// REQ: FindInfoReq(0x04), StartHandle, EndHandle
// RSP: FindInfoRsp(0x05), UUIDFormat, Handle, UUID, Handle, UUID, ...
func (c *central) handleFindInfo(b []byte) []byte {
	start, end := readHandleRange(b[:4])

	w := newL2capWriter(c.mtu)
	w.WriteByteFit(attOpFindInfoRsp)

	uuidLen := -1
	for _, a := range c.attrs.Subrange(start, end) {
		if uuidLen == -1 {
			uuidLen = a.typ.Len()
			if uuidLen == 2 {
				w.WriteByteFit(0x01) // TODO: constants for 16bit vs 128bit uuid magic numbers here
			} else {
				w.WriteByteFit(0x02)
			}
		}
		if a.typ.Len() != uuidLen {
			break
		}
		w.Chunk()
		w.WriteUint16Fit(a.h)
		w.WriteUUIDFit(a.typ)
		if ok := w.Commit(); !ok {
			break
		}
	}

	if uuidLen == -1 {
		return attErrorRsp(attOpFindInfoReq, start, attEcodeAttrNotFound)
	}
	return w.Bytes()
}

// REQ: FindByTypeValueReq(0x06), StartHandle, EndHandle, Type(UUID), Value
// RSP: FindByTypeValueRsp(0x07), AttrHandle, GroupEndHandle, AttrHandle, GroupEndHandle, ...
func (c *central) handleFindByTypeValue(b []byte) []byte {
	start, end := readHandleRange(b[:4])
	t := UUID{b[4:6]}
	u := UUID{b[6:]}

	// Only support the ATT ReadByGroupReq for GATT Primary Service Discovery.
	// More sepcifically, the "Discover Primary Services By Service UUID" sub-procedure
	if !t.Equal(attrPrimaryServiceUUID) {
		return attErrorRsp(attOpFindByTypeValueReq, start, attEcodeAttrNotFound)
	}

	w := newL2capWriter(c.mtu)
	w.WriteByteFit(attOpFindByTypeValueRsp)

	var wrote bool
	for _, a := range c.attrs.Subrange(start, end) {
		if !a.typ.Equal(attrPrimaryServiceUUID) {
			continue
		}
		if !(UUID{a.value}.Equal(u)) {
			continue
		}
		w.Chunk()
		w.WriteUint16Fit(a.h)
		w.WriteUint16Fit(a.groupEnd)
		if ok := w.Commit(); !ok {
			break
		}
		wrote = true
	}
	if !wrote {
		return attErrorRsp(attOpFindByTypeValueReq, start, attEcodeAttrNotFound)
	}

	return w.Bytes()
}

func (c *central) readValue(a attr, capacity, offset int) ([]byte, attEcode) {
	if a.props&CharRead == 0 {
		return nil, attEcodeReadNotPerm
	}
	v := a.value
	if a.typ.Equal(attrClientCharacteristicConfigUUID) {
		c.notifiersmu.Lock()
		var ccc uint16
		if n := c.notifiers[a.h]; n != nil {
			ccc = n.ccc
		}
		c.notifiersmu.Unlock()
		v = []byte{byte(ccc), byte(ccc >> 8)}
	}
	if v != nil {
		if offset > len(v) {
			return nil, attEcodeInvalidOffset
		}
		return v[offset:], attEcodeSuccess
	}
	handler := a.rhandler
	if handler == nil {
		return nil, attEcodeUnlikely
	}
	rsp := newResponseWriter(capacity)
	req := &ReadRequest{
		Request: Request{Central: c},
		Cap:     capacity,
		Offset:  offset,
	}
	if !c.callHandler(func() {
		handler.ServeRead(rsp, req)
	}) {
		return nil, attEcodeUnlikely
	}
	if code := attErrorForStatus(rsp.status); code != attEcodeSuccess {
		return nil, code
	}
	return rsp.bytes(), attEcodeSuccess
}

// attTypeEqual compares attribute types as full Bluetooth UUIDs. Attribute
// values and the public UUID.Equal retain their byte-for-byte comparison.
func attTypeEqual(u, v UUID) bool {
	if u.Len() == v.Len() {
		return u.Equal(v)
	}
	if u.Len() == 16 {
		u, v = v, u
	}
	if u.Len() != 2 || v.Len() != 16 {
		return false
	}
	full := [16]byte{0xfb, 0x34, 0x9b, 0x5f, 0x80, 0x00, 0x00, 0x80, 0x00, 0x10, 0x00, 0x00, u.b[0], u.b[1], 0x00, 0x00}
	return bytes.Equal(full[:], v.b)
}

// REQ: ReadByType(0x08), StartHandle, EndHandle, Type(UUID)
// RSP: ReadByType(0x09), LenOfEachDataField, DataField, DataField, ...
func (c *central) handleReadByType(b []byte) []byte {
	start, end := readHandleRange(b[:4])
	t := UUID{b[4:]}

	// Each handle-value pair has a one-byte length field and a two-byte handle.
	valueCap := min(int(c.mtu)-4, 253)
	w := newL2capWriter(c.mtu)
	w.WriteByteFit(attOpReadByTypeRsp)
	uuidLen := -1
	for _, a := range c.attrs.Subrange(start, end) {
		if !attTypeEqual(a.typ, t) {
			continue
		}
		if (a.secure&CharRead) != 0 && c.security > securityLow {
			if uuidLen != -1 {
				break
			}
			return attErrorRsp(attOpReadByTypeReq, a.h, attEcodeAuthentication)
		}
		v, code := c.readValue(a, valueCap, 0)
		if code != attEcodeSuccess {
			if uuidLen != -1 {
				break
			}
			return attErrorRsp(attOpReadByTypeReq, a.h, code)
		}
		if uuidLen == -1 {
			uuidLen = len(v)
			w.WriteByteFit(byte(min(uuidLen, valueCap) + 2))
		}
		if len(v) != uuidLen {
			break
		}
		w.Chunk()
		w.WriteUint16Fit(a.h)
		w.WriteFit(v[:min(len(v), valueCap)])
		if ok := w.Commit(); !ok {
			break
		}
	}
	if uuidLen == -1 {
		return attErrorRsp(attOpReadByTypeReq, start, attEcodeAttrNotFound)
	}
	return w.Bytes()
}

// REQ: ReadReq(0x0A), Handle
// RSP: ReadRsp(0x0B), Value
func (c *central) handleRead(b []byte) []byte {
	h := binary.LittleEndian.Uint16(b)
	a, ok := c.attrs.At(h)
	if !ok {
		return attErrorRsp(attOpReadReq, h, attEcodeInvalidHandle)
	}
	if a.props&CharRead == 0 {
		return attErrorRsp(attOpReadReq, h, attEcodeReadNotPerm)
	}
	if a.secure&CharRead != 0 && c.security > securityLow {
		return attErrorRsp(attOpReadReq, h, attEcodeAuthentication)
	}
	valueCap := min(int(c.mtu)-1, attMaxValueLen)
	v, code := c.readValue(a, valueCap, 0)
	if code != attEcodeSuccess {
		return attErrorRsp(attOpReadReq, h, code)
	}

	w := newL2capWriter(uint16(valueCap + 1))
	w.WriteByteFit(attOpReadRsp)
	w.Chunk()
	w.WriteFit(v)
	w.CommitFit()
	return w.Bytes()
}

// FIXME: check this, untested, might be broken
func (c *central) handleReadBlob(b []byte) []byte {
	h := binary.LittleEndian.Uint16(b)
	offset := binary.LittleEndian.Uint16(b[2:])
	a, ok := c.attrs.At(h)
	if !ok {
		return attErrorRsp(attOpReadBlobReq, h, attEcodeInvalidHandle)
	}
	if a.props&CharRead == 0 {
		return attErrorRsp(attOpReadBlobReq, h, attEcodeReadNotPerm)
	}
	if a.secure&CharRead != 0 && c.security > securityLow {
		return attErrorRsp(attOpReadBlobReq, h, attEcodeAuthentication)
	}
	valueCap := min(int(c.mtu)-1, attMaxValueLen)
	v, code := c.readValue(a, valueCap, int(offset))
	if code != attEcodeSuccess {
		return attErrorRsp(attOpReadBlobReq, h, code)
	}
	w := newL2capWriter(uint16(valueCap + 1))
	w.WriteByteFit(attOpReadBlobRsp)
	w.Chunk()
	w.WriteFit(v)
	w.CommitFit()
	return w.Bytes()
}

func (c *central) handleReadByGroup(b []byte) []byte {
	start, end := readHandleRange(b)
	t := UUID{b[4:]}

	// Only support the ATT ReadByGroupReq for GATT Primary Service Discovery.
	// More specifically, the "Discover All Primary Services" sub-procedure.
	if !attTypeEqual(t, attrPrimaryServiceUUID) {
		return attErrorRsp(attOpReadByGroupReq, start, attEcodeUnsuppGrpType)
	}

	w := newL2capWriter(c.mtu)
	w.WriteByteFit(attOpReadByGroupRsp)
	uuidLen := -1
	for _, a := range c.attrs.Subrange(start, end) {
		if !a.typ.Equal(attrPrimaryServiceUUID) {
			continue
		}
		if uuidLen == -1 {
			uuidLen = len(a.value)
			w.WriteByteFit(byte(uuidLen + 4))
		}
		if uuidLen != len(a.value) {
			break
		}
		w.Chunk()
		w.WriteUint16Fit(a.h)
		w.WriteUint16Fit(a.groupEnd)
		w.WriteFit(a.value)
		if ok := w.Commit(); !ok {
			break
		}
	}
	if uuidLen == -1 {
		return attErrorRsp(attOpReadByGroupReq, start, attEcodeAttrNotFound)
	}
	return w.Bytes()
}

func (c *central) handleWrite(reqType byte, b []byte) []byte {
	h := binary.LittleEndian.Uint16(b[:2])
	value := b[2:]

	a, ok := c.attrs.At(h)
	if !ok {
		return attErrorRsp(reqType, h, attEcodeInvalidHandle)
	}

	noRsp := reqType == attOpWriteCmd
	charFlag := CharWrite
	if noRsp {
		charFlag = CharWriteNR
	}
	if a.props&charFlag == 0 {
		return attErrorRsp(reqType, h, attEcodeWriteNotPerm)
	}
	if a.secure&charFlag == 0 && c.security > securityLow {
		return attErrorRsp(reqType, h, attEcodeAuthentication)
	}

	if len(value) > attMaxValueLen {
		return attErrorRsp(reqType, h, attEcodeInvalAttrValueLen)
	}

	// Props of Service and Characteristic declration are read only.
	// So we only need deal with writable descriptors here.
	// (Characteristic's value is implemented with descriptor)
	if !a.typ.Equal(attrClientCharacteristicConfigUUID) {
		// Regular write, not CCC
		handler := a.whandler
		if handler == nil {
			return attErrorRsp(reqType, h, attEcodeUnlikely)
		}
		r := Request{Central: c}
		value = append([]byte(nil), value...)
		status := byte(StatusUnexpectedError)
		if !c.callHandler(func() {
			status = handler.ServeWrite(r, value)
		}) {
			return attErrorRsp(reqType, h, attEcodeUnlikely)
		}
		if noRsp {
			return nil
		}
		if code := attErrorForStatus(status); code != attEcodeSuccess {
			return attErrorRsp(reqType, h, code)
		}
		return []byte{attOpWriteRsp}
	}

	// CCC/descriptor write
	if len(value) != 2 {
		return attErrorRsp(reqType, h, attEcodeInvalAttrValueLen)
	}
	ccc := binary.LittleEndian.Uint16(value)
	if ccc & ^uint16(gattCCCNotifyFlag|gattCCCIndicateFlag) != 0 ||
		ccc&gattCCCNotifyFlag != 0 && a.notifyProps&CharNotify == 0 ||
		ccc&gattCCCIndicateFlag != 0 && a.notifyProps&CharIndicate == 0 {
		return attErrorRsp(reqType, h, attEcodeValueNotAllowed)
	}
	if ccc&(gattCCCNotifyFlag|gattCCCIndicateFlag) != 0 {
		if !c.startNotify(&a, int(c.mtu-3), ccc) {
			return attErrorRsp(reqType, h, attEcodeInsuffResources)
		}
	} else {
		c.stopNotify(&a)
	}
	if noRsp {
		return nil
	}
	return []byte{attOpWriteRsp}
}

func (c *central) sendNotification(n *notifier, data []byte) (int, error) {
	if !c.lockSend() {
		return 0, io.ErrClosedPipe
	}
	defer c.unlockSend()
	mtu := uint16(c.MTU())
	if len(data) > min(int(mtu)-3, attMaxValueLen) {
		return 0, io.ErrShortWrite
	}
	c.notifiersmu.Lock()
	if c.ctx.Err() != nil || n.Done() {
		c.notifiersmu.Unlock()
		return 0, io.ErrClosedPipe
	}
	op := byte(attOpHandleNotify)
	if n.ccc == gattCCCIndicateFlag {
		if c.indication != nil {
			c.notifiersmu.Unlock()
			return 0, ErrIndicationPending
		}
		op = attOpHandleInd
		pending := &pendingIndication{}
		c.indication = pending
		pending.timer = time.AfterFunc(indicationTimeout, func() {
			c.notifiersmu.Lock()
			if c.indication != pending {
				c.notifiersmu.Unlock()
				return
			}
			// Mark closure before releasing the lock, so no new write can
			// reuse the bearer between the timeout and transport shutdown.
			c.cancel()
			c.notifiersmu.Unlock()
			c.Close()
		})
	}
	c.notifiersmu.Unlock()
	w := newL2capWriter(mtu)
	w.WriteByteFit(op)
	w.WriteUint16Fit(n.a.valueHandle)
	w.WriteFit(data)
	packet := w.Bytes()
	written, err := c.l2conn.Write(packet)
	if err == nil && written != len(packet) {
		err = io.ErrShortWrite
	}
	if op == attOpHandleInd && err != nil {
		// A partial indication may still be acknowledged. Retire the bearer
		// rather than allowing that acknowledgement to match a later value.
		c.notifiersmu.Lock()
		c.cancel()
		c.notifiersmu.Unlock()
		err = errors.Join(err, c.Close())
	}
	// Report value bytes to the caller, excluding the ATT opcode and handle.
	written -= 3
	if written < 0 {
		written = 0
	}
	return written, err
}

func (c *central) confirmIndication() {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	if c.indication != nil {
		c.indication.timer.Stop()
		c.indication = nil
	}
}

func readHandleRange(b []byte) (start, end uint16) {
	return binary.LittleEndian.Uint16(b), binary.LittleEndian.Uint16(b[2:])
}

func (c *central) startNotify(a *attr, maxlen int, ccc uint16) bool {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	if c.ctx.Err() != nil {
		return false
	}
	if n, found := c.notifiers[a.h]; found {
		n.ccc = ccc
		return true
	}
	if a.nhandler == nil {
		return false
	}
	n := newNotifier(c, a, maxlen)
	n.ccc = ccc
	c.notifiers[a.h] = n
	if !startCallout(c.callbackSlots, func() { a.nhandler.ServeNotify(Request{Central: c}, n) }) {
		delete(c.notifiers, a.h)
		n.stop()
		return false
	}
	return true
}

func (c *central) stopNotify(a *attr) {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	// char := a.pvt.(*Characteristic)
	if n, found := c.notifiers[a.h]; found {
		n.stop()
		delete(c.notifiers, a.h)
	}
}
