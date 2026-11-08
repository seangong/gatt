package linux

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

const hciCloseTimeout = 5 * time.Second

// Leave time to interrupt IO and join the local workers after graceful shutdown.
const hciGraceTimeout = hciCloseTimeout - time.Second

type HCI struct {
	AcceptMasterHandler  func(pd *PlatData)
	AcceptSlaveHandler   func(pd *PlatData)
	AdvertisementHandler func(pd *PlatData)

	d io.ReadWriteCloser
	c *cmd.Cmd
	e *evt.Evt

	plist   map[bdaddr]*PlatData
	plistmu *sync.Mutex

	bufCnt  chan struct{}
	bufSize int

	maxConn int
	connsmu *sync.Mutex
	conns   map[uint16]*conn
	closed  bool // Protected by connsmu.

	closeOnce       sync.Once
	closeDone       chan struct{}
	abort           chan struct{}
	changed         chan struct{}
	readerDone      chan struct{}
	readerStarted   bool
	transportClosed bool
	transportOnce   sync.Once
	transportErr    error
	closeCause      error
	closeErr        error
	workers         sync.WaitGroup
	callbacks       chan struct{}
	scanning        bool
	pending         *connectionAttempt // Protected by connsmu.

	adv   bool
	advmu *sync.Mutex
}

type bdaddr [6]byte

type PlatData struct {
	Name        string
	AddressType uint8
	Address     [6]byte
	Data        []byte
	Connectable bool
	RSSI        int8

	Conn io.ReadWriteCloser
}

// connectionAttempt keeps one outgoing attempt and its cancellation together.
// All fields and channel publications are protected by HCI.connsmu.
type connectionAttempt struct {
	source        *PlatData
	address       [6]byte
	addressType   uint8
	createDone    chan struct{}
	done          chan struct{}
	conn          *conn
	terminalErr   error
	cancelStarted bool
	cancelDone    chan struct{}
	cancelErr     error
}

func newConnectionAttempt(pd *PlatData) *connectionAttempt {
	return &connectionAttempt{
		source: pd, address: pd.Address, addressType: pd.AddressType,
		createDone: make(chan struct{}), done: make(chan struct{}), cancelDone: make(chan struct{}),
	}
}

func (a *connectionAttempt) created() {
	select {
	case <-a.createDone:
	default:
		close(a.createDone)
	}
}

func (a *connectionAttempt) complete(c *conn, err error) {
	select {
	case <-a.done:
	default:
		a.conn = c
		a.terminalErr = err
		close(a.done)
	}
}

func NewHCI(devID int, chk bool, maxConn int) (*HCI, error) {
	d, err := newDevice(devID, chk)
	if err != nil {
		return nil, err
	}
	c := cmd.NewCmd(d)
	e := evt.NewEvt()

	h := &HCI{
		d: d,
		c: c,
		e: e,

		plist:   make(map[bdaddr]*PlatData),
		plistmu: &sync.Mutex{},

		bufCnt:  make(chan struct{}, 15-1),
		bufSize: 27,

		maxConn: maxConn,
		connsmu: &sync.Mutex{},
		conns:   map[uint16]*conn{},

		advmu: &sync.Mutex{},
	}

	e.HandleEvent(evt.LEMeta, evt.HandlerFunc(h.handleLEMeta))
	e.HandleEvent(evt.DisconnectionComplete, evt.HandlerFunc(h.handleDisconnectionComplete))
	e.HandleEvent(evt.NumberOfCompletedPkts, evt.HandlerFunc(h.handleNumberOfCompletedPkts))
	e.HandleEvent(evt.CommandComplete, evt.HandlerFunc(c.HandleComplete))
	e.HandleEvent(evt.CommandStatus, evt.HandlerFunc(c.HandleStatus))

	ctx, cancel := context.WithTimeout(context.Background(), hciCloseTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := d.SetDeadline(deadline); err != nil {
		h.requestClose(err)
		return nil, h.Close()
	}
	go h.mainLoop()
	if err := h.resetDevice(ctx); err != nil {
		h.requestClose(err)
		return nil, h.Close()
	}
	if err := ctx.Err(); err != nil {
		h.requestClose(err)
		return nil, h.Close()
	}
	if err := d.SetDeadline(time.Time{}); err != nil {
		h.requestClose(err)
		return nil, h.Close()
	}
	h.connsmu.Lock()
	closed := h.closed
	h.connsmu.Unlock()
	if closed {
		return nil, h.Close()
	}
	return h, nil
}

// initLifecycle is called with connsmu held. Tests can construct an HCI directly.
func (h *HCI) initLifecycle() {
	if h.advmu == nil {
		h.advmu = &sync.Mutex{}
	}
	if h.closeDone == nil {
		h.closeDone = make(chan struct{})
		h.abort = make(chan struct{})
		h.changed = make(chan struct{}, 1)
		h.readerDone = make(chan struct{})
		h.callbacks = make(chan struct{}, max(1, h.maxConn))
	}
}

// requestClose never waits: a worker can report failure without joining itself.
func (h *HCI) requestClose(cause error) <-chan struct{} {
	h.connsmu.Lock()
	h.initLifecycle()
	if cause != nil && !h.transportClosed && h.closeCause == nil {
		h.closeCause = cause
		close(h.abort)
	}
	h.closed = true
	h.c.BeginShutdown()
	for _, c := range h.conns {
		c.stop()
	}
	done := h.closeDone
	h.closeOnce.Do(func() { go h.shutdown() })
	h.connsmu.Unlock()
	return done
}

func (h *HCI) Close() error {
	<-h.requestClose(nil)
	return h.closeErr
}

func (h *HCI) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), hciGraceTimeout)
	defer cancel()
	select {
	case <-h.abort:
		cancel()
	default:
	}
	// Cancel response waits when the receiver fails, including during shutdown.
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-h.abort:
			cancel()
		case <-ctx.Done():
		}
		h.closeTransport()
	}()
	if d, ok := h.d.(interface{ SetDeadline(time.Time) error }); ok {
		deadline, _ := ctx.Deadline()
		if err := d.SetDeadline(deadline); err != nil {
			h.requestClose(fmt.Errorf("hci: set shutdown deadline: %w", err))
			cancel()
		}
	}
	err := h.disconnectAll(ctx)
	if ctx.Err() != nil && err != nil {
		err = ctx.Err()
	}
	cancel()
	<-watchDone
	h.closeTransport()
	h.connsmu.Lock()
	started, readerDone := h.readerStarted, h.readerDone
	h.connsmu.Unlock()
	h.c.Wait()
	h.workers.Wait()
	if started {
		<-readerDone
	}
	h.connsmu.Lock()
	cause := h.closeCause
	h.connsmu.Unlock()
	if cause != nil && errors.Is(err, context.Canceled) {
		err = nil
	}
	h.closeErr = errors.Join(cause, err, h.transportErr)
	// The poller's IO deadline can win the race with the context timer.
	if errors.Is(h.closeErr, os.ErrDeadlineExceeded) && !errors.Is(h.closeErr, context.DeadlineExceeded) {
		h.closeErr = errors.Join(context.DeadlineExceeded, h.closeErr)
	}
	close(h.closeDone)
}

// closeTransport is independent of the graceful sender: closing the pollable
// socket must be able to interrupt that sender's own Write as well.
func (h *HCI) closeTransport() {
	h.transportOnce.Do(func() {
		h.c.Close()
		h.connsmu.Lock()
		h.transportClosed = true
		if a := h.pending; a != nil {
			a.created()
			a.complete(nil, io.ErrClosedPipe)
			h.pending = nil
		}
		for handle, c := range h.conns {
			c.terminate(io.ErrClosedPipe)
			delete(h.conns, handle)
		}
		h.connsmu.Unlock()
		h.transportErr = h.d.Close()
	})
}

func (h *HCI) disconnectAll(ctx context.Context) error {
	h.connsmu.Lock()
	scanning, pending := h.scanning, h.pending
	h.connsmu.Unlock()
	h.advmu.Lock()
	advertising := h.adv
	h.adv = false
	h.advmu.Unlock()
	var commands []cmd.CmdParam
	if scanning {
		commands = append(commands, cmd.LESetScanEnable{})
	}
	if advertising {
		commands = append(commands, cmd.LESetAdvertiseEnable{})
	}
	for _, cp := range commands {
		response, err := h.c.SendShutdownContext(ctx, cp)
		if err != nil {
			return err
		}
		if len(response) == 0 || response[0] != 0 {
			return fmt.Errorf("hci: shutdown command 0x%04X returned status [% X]", cp.Opcode(), response)
		}
	}
	if pending != nil {
		if err := h.cancelPendingConnection(ctx, pending); err != nil {
			return err
		}
	}
	for {
		h.connsmu.Lock()
		connections := make([]*conn, 0, len(h.conns))
		for _, c := range h.conns {
			connections = append(connections, c)
		}
		pendingConnection := h.pending != nil
		h.connsmu.Unlock()
		if len(connections) == 0 && !pendingConnection {
			return nil
		}
		for _, c := range connections {
			if err := c.disconnect(ctx); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.changed:
		}
	}
}

func (h *HCI) startWorker(f func()) bool {
	h.connsmu.Lock()
	if h.closed {
		h.connsmu.Unlock()
		return false
	}
	h.workers.Add(1)
	h.connsmu.Unlock()
	go func() { defer h.workers.Done(); f() }()
	return true
}

// Callouts run outside the core workers so a callback may call Close. Slots are
// held until the user function actually returns, including after cancellation.
func (h *HCI) callout(f func()) bool {
	h.connsmu.Lock()
	h.initLifecycle()
	if h.closed {
		h.connsmu.Unlock()
		return false
	}
	select {
	case h.callbacks <- struct{}{}:
		h.connsmu.Unlock()
		go func() { defer func() { <-h.callbacks }(); f() }()
		return true
	default:
		h.connsmu.Unlock()
		return false
	}
}

func (h *HCI) SetAdvertiseEnable(en bool) error {
	h.advmu.Lock()
	h.adv = en
	h.advmu.Unlock()
	return h.setAdvertiseEnable(en)
}

func (h *HCI) setAdvertiseEnable(en bool) error {
	h.connsmu.Lock()
	closed, connectionCount := h.closed, len(h.conns)
	h.connsmu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	h.advmu.Lock()
	full := en && h.adv && connectionCount == h.maxConn
	h.advmu.Unlock()
	if full {
		return nil
	}
	if err := h.c.SendAndCheckResp(cmd.LESetAdvertiseEnable{AdvertisingEnable: btoi(en)}, []byte{0}); err != nil {
		return fmt.Errorf("hci: set advertising enabled=%t: %w", en, err)
	}
	return nil
}

func (h *HCI) SendCmdWithAdvOff(cp cmd.CmdParam) error {
	h.advmu.Lock()
	advertising := h.adv
	h.advmu.Unlock()
	// Reset leaves advertising disabled. Only pause an active advertising
	// configuration before updating it.
	if advertising {
		if err := h.setAdvertiseEnable(false); err != nil {
			return fmt.Errorf("disable advertising before command 0x%04x: %w", cp.Opcode(), err)
		}
	}
	err := h.c.SendAndCheckResp(cp, []byte{0})
	if err != nil {
		err = fmt.Errorf("configure advertising with command 0x%04x: %w", cp.Opcode(), err)
	}
	h.advmu.Lock()
	advertising = h.adv
	h.advmu.Unlock()
	if advertising {
		if restoreErr := h.setAdvertiseEnable(true); restoreErr != nil {
			err = errors.Join(err, fmt.Errorf("restore advertising after command 0x%04x: %w", cp.Opcode(), restoreErr))
		}
	}
	return err
}

func (h *HCI) SetScanEnable(en bool, dup bool) error {
	h.connsmu.Lock()
	if h.closed {
		h.connsmu.Unlock()
		return io.ErrClosedPipe
	}
	h.scanning = en
	h.connsmu.Unlock()
	return h.c.SendAndCheckResp(cmd.LESetScanEnable{
		LEScanEnable: btoi(en), FilterDuplicates: btoi(!dup),
	}, []byte{0})
}

func (h *HCI) Connect(pd *PlatData) error {
	h.connsmu.Lock()
	if h.closed {
		h.connsmu.Unlock()
		return io.ErrClosedPipe
	}
	if h.pending != nil {
		h.connsmu.Unlock()
		return errors.New("hci: connection attempt already pending")
	}
	a := newConnectionAttempt(pd)
	h.pending = a
	h.connsmu.Unlock()
	// Keep the caller's parsed name without sharing mutable advertisement data.
	report := *pd
	report.Data = append([]byte(nil), pd.Data...)
	h.plistmu.Lock()
	h.plist[pd.Address] = &report
	h.plistmu.Unlock()
	_, err := h.c.Send(
		cmd.LECreateConn{
			LEScanInterval:        0x0004,         // N x 0.625ms
			LEScanWindow:          0x0004,         // N x 0.625ms
			InitiatorFilterPolicy: 0x00,           // white list not used
			PeerAddressType:       pd.AddressType, // public or random
			PeerAddress:           pd.Address,     //
			OwnAddressType:        0x00,           // public
			ConnIntervalMin:       0x0006,         // N x 0.125ms
			ConnIntervalMax:       0x0006,         // N x 0.125ms
			ConnLatency:           0x0000,         //
			SupervisionTimeout:    0x000A,         // N x 10ms
			MinimumCELength:       0x0000,         // N x 0.625ms
			MaximumCELength:       0x0000,         // N x 0.625ms
		})
	h.connsmu.Lock()
	a.created()
	if err != nil {
		a.complete(nil, nil)
		if h.pending == a && !a.cancelStarted {
			h.pending = nil
		}
	}
	h.initLifecycle()
	select {
	case h.changed <- struct{}{}:
	default:
	}
	h.connsmu.Unlock()
	return err
}

func (h *HCI) CancelConnection(pd *PlatData) error {
	h.connsmu.Lock()
	if a := h.pending; a != nil && a.source == pd {
		h.connsmu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), hciCloseTimeout)
		defer cancel()
		return h.cancelPendingConnection(ctx, a)
	}
	target := pd.Conn
	for _, c := range h.conns {
		if c.origin == pd {
			target = c
			break
		}
	}
	h.connsmu.Unlock()
	if target == nil {
		return nil
	}
	return target.Close()
}

// cancelPendingConnection is also used by shutdown. The attempt stays reserved
// until both its terminal event and the cancellation transaction have finished:
// a queued cancellation must never reach a later Create Connection.
func (h *HCI) cancelPendingConnection(ctx context.Context, a *connectionAttempt) (err error) {
	h.connsmu.Lock()
	if a.cancelStarted {
		h.connsmu.Unlock()
		select {
		case <-a.cancelDone:
			return a.cancelErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	a.cancelStarted = true
	h.connsmu.Unlock()

	// Context cancellation alone cannot interrupt the command's device Write.
	// Fail closed so the transport's Close releases that write as well.
	watchDone := make(chan struct{})
	stopWatch := context.AfterFunc(ctx, func() {
		h.requestClose(fmt.Errorf("hci: cancel pending connection: %w", ctx.Err()))
		close(watchDone)
	})
	defer func() {
		if !stopWatch() {
			<-watchDone
		}
		if ctx.Err() != nil {
			err = errors.Join(ctx.Err(), err)
		}
		if err != nil {
			h.requestClose(fmt.Errorf("hci: cancel pending connection: %w", err))
		}
		h.connsmu.Lock()
		select {
		case <-a.cancelDone:
		default:
			a.cancelErr = err
			close(a.cancelDone)
		}
		select {
		case <-a.done:
			if h.pending == a {
				h.pending = nil
			}
		default:
		}
		h.initLifecycle()
		select {
		case h.changed <- struct{}{}:
		default:
		}
		h.connsmu.Unlock()
	}()

	select {
	case <-a.createDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-a.done:
		return h.disconnectAttempt(ctx, a)
	default:
	}

	response, err := h.c.SendShutdownContext(ctx, cmd.LECreateConnCancel{})
	if err != nil {
		return fmt.Errorf("send LE Create Connection Cancel: %w", err)
	}
	if len(response) == 0 {
		return errors.New("LE Create Connection Cancel returned no status")
	}
	var statusErr error
	if response[0] != 0 {
		statusErr = fmt.Errorf("LE Create Connection Cancel returned status 0x%02x", response[0])
		// A successful connection can win before its event reaches the reader.
		// Command Disallowed is harmless only once that attempt is terminal.
		if response[0] != 0x0c {
			return statusErr
		}
	}
	select {
	case <-a.done:
		return h.disconnectAttempt(ctx, a)
	case <-ctx.Done():
		return errors.Join(statusErr, ctx.Err())
	}
}

func (h *HCI) disconnectAttempt(ctx context.Context, a *connectionAttempt) error {
	h.connsmu.Lock()
	c, err := a.conn, a.terminalErr
	h.connsmu.Unlock()
	if c == nil {
		return err
	}
	if err := c.disconnect(ctx); err != nil {
		return err
	}
	select {
	case <-c.linkDone:
		return c.linkErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *HCI) SendRawCommand(c cmd.CmdParam) ([]byte, error) {
	return h.c.Send(c)
}

func btoi(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func (h *HCI) mainLoop() {
	h.connsmu.Lock()
	h.initLifecycle()
	if h.closed {
		h.connsmu.Unlock()
		return
	}
	h.readerStarted = true
	h.connsmu.Unlock()
	defer close(h.readerDone)
	b := make([]byte, 4096)
	for {
		n, err := h.d.Read(b)
		if err != nil || n == 0 {
			h.connsmu.Lock()
			closed := h.transportClosed
			h.connsmu.Unlock()
			if !closed {
				if err == nil {
					err = io.EOF
				}
				h.requestClose(fmt.Errorf("hci: read: %w", err))
			}
			return
		}
		p := append([]byte(nil), b[:n]...)
		h.handlePacket(p)
	}
}

func (h *HCI) handlePacket(b []byte) {
	if len(b) == 0 {
		return
	}
	t, b := packetType(b[0]), b[1:]
	var err error
	switch t {
	case typCommandPkt:
		if len(b) < 2 {
			h.requestClose(errors.New("hci: malformed command packet"))
			return
		}
		op := uint16(b[0]) | uint16(b[1])<<8
		log.Printf("unmanaged cmd: opcode (%04x) [ % X ]\n", op, b)
	case typACLDataPkt:
		err = h.handleL2CAP(b)
	case typSCODataPkt:
		err = fmt.Errorf("SCO packet not supported")
	case typEventPkt:
		err = h.e.Dispatch(b)
	case typVendorPkt:
		err = fmt.Errorf("Vendor packet not supported")
	default:
		h.requestClose(fmt.Errorf("Unknown event: 0x%02X [ % X ]", t, b))
	}
	if err != nil {
		log.Printf("hci: %s, [ % X]", err, b)
	}
}

func (h *HCI) resetDevice(ctx context.Context) error {
	seq := []cmd.CmdParam{
		cmd.Reset{},
		cmd.SetEventMask{EventMask: 0x3dbff807fffbffff},
		cmd.LESetEventMask{LEEventMask: 0x000000000000001F},
		cmd.WriteSimplePairingMode{SimplePairingMode: 1},
		cmd.WriteLEHostSupported{LESupportedHost: 1, SimultaneousLEHost: 0},
		cmd.WriteInquiryMode{InquiryMode: 2},
		cmd.WritePageScanType{PageScanType: 1},
		cmd.WriteInquiryScanType{ScanType: 1},
		cmd.WriteClassOfDevice{ClassOfDevice: [3]byte{0x40, 0x02, 0x04}},
		cmd.WritePageTimeout{PageTimeout: 0x2000},
		cmd.WriteDefaultLinkPolicy{DefaultLinkPolicySettings: 0x5},
		cmd.HostBufferSize{
			HostACLDataPacketLength:            0x1000,
			HostSynchronousDataPacketLength:    0xff,
			HostTotalNumACLDataPackets:         0x0014,
			HostTotalNumSynchronousDataPackets: 0x000a},
		cmd.LESetScanParameters{
			LEScanType:           0x01,   // [0x00]: passive, 0x01: active
			LEScanInterval:       0x0010, // [0x10]: 0.625ms * 16
			LEScanWindow:         0x0010, // [0x10]: 0.625ms * 16
			OwnAddressType:       0x00,   // [0x00]: public, 0x01: random
			ScanningFilterPolicy: 0x00,   // [0x00]: accept all, 0x01: ignore non-white-listed.
		},
	}
	for _, s := range seq {
		if err := h.c.SendAndCheckRespContext(ctx, s, []byte{0x00}); err != nil {
			return err
		}
	}
	return nil
}

// Keep only complete AD structures from one payload before merging it with
// other reports. Padding or a malformed tail must not hide a later scan response.
func advertisingDataPrefix(data []byte) []byte {
	end := 0
	for end < len(data) {
		length := int(data[end])
		if length == 0 || length > len(data)-end-1 {
			break
		}
		end += length + 1
	}
	return data[:end]
}

func (h *HCI) handleAdvertisement(b []byte) {
	// If no one is interested, don't bother.
	if h.AdvertisementHandler == nil {
		return
	}
	ep := &evt.LEAdvertisingReportEP{}
	if err := ep.Unmarshal(b); err != nil {
		return
	}
	for i := 0; i < int(ep.NumReports); i++ {
		addr := bdaddr(ep.Address[i])
		et := ep.EventType[i]
		connectable := et == advInd || et == advDirectInd
		scannable := et == advInd || et == advScanInd
		data := advertisingDataPrefix(ep.Data[i])

		if et == scanRsp {
			h.plistmu.Lock()
			pd, ok := h.plist[addr]
			var report PlatData
			if ok {
				pd.Data = append(pd.Data, data...)
				report = *pd
				report.Data = append([]byte(nil), pd.Data...)
			}
			h.plistmu.Unlock()
			if ok {
				h.callout(func() { h.AdvertisementHandler(&report) })
			}
			continue
		}

		pd := &PlatData{
			AddressType: ep.AddressType[i],
			Address:     ep.Address[i],
			Data:        data,
			Connectable: connectable,
			RSSI:        ep.RSSI[i],
		}
		h.plistmu.Lock()
		h.plist[addr] = pd
		h.plistmu.Unlock()
		if scannable {
			continue
		}
		report := *pd
		report.Data = append([]byte(nil), pd.Data...)
		h.callout(func() { h.AdvertisementHandler(&report) })
	}
}

func (h *HCI) handleNumberOfCompletedPkts(b []byte) error {
	ep := &evt.NumberOfCompletedPktsEP{}
	if err := ep.Unmarshal(b); err != nil {
		return err
	}
	for _, r := range ep.Packets {
		for i := 0; i < int(r.NumOfCompletedPkts); i++ {
			select {
			case <-h.bufCnt:
			default:
				return fmt.Errorf("hci: completion exceeds outstanding ACL packets")
			}
		}
	}
	return nil
}

func (h *HCI) handleConnection(b []byte) {
	ep := &evt.LEConnectionCompleteEP{}
	if err := ep.Unmarshal(b); err != nil {
		return
	}
	h.connsmu.Lock()
	h.initLifecycle()
	a := h.pending
	matches := a != nil && (ep.Status != 0 || ep.Role == 0x00 &&
		a.address == ep.PeerAddress && a.addressType == ep.PeerAddressType)
	if ep.Status != 0 || h.transportClosed || h.closeCause != nil {
		if matches {
			a.complete(nil, nil)
			if !a.cancelStarted {
				h.pending = nil
			}
		}
		select {
		case h.changed <- struct{}{}:
		default:
		}
		h.connsmu.Unlock()
		return
	}
	c := newConn(h, ep.ConnectionHandle)
	canceling := false
	if matches {
		c.origin = a.source
		a.complete(c, nil)
		canceling = a.cancelStarted
		if !canceling {
			h.pending = nil
		}
	}
	if previous := h.conns[c.attr]; previous != nil {
		previous.terminate(io.ErrClosedPipe)
	}
	h.conns[c.attr] = c
	closing := h.closed || canceling
	if closing {
		c.stop()
	}
	select {
	case h.changed <- struct{}{}:
	default:
	}
	h.connsmu.Unlock()
	if closing {
		return
	}
	h.startWorker(func() {
		if err := h.setAdvertiseEnable(true); err != nil {
			c.Abort(err)
			return
		}
		// FIXME: sloppiness. This call should be called by the package user once we
		// flesh out the support of l2cap signaling packets (CID:0x0001,0x0005)
		if ep.ConnLatency != 0 || ep.ConnInterval > 0x18 {
			if _, err := c.updateConnection(); err != nil {
				c.Abort(err)
				return
			}
		}
		pd := &PlatData{Address: ep.PeerAddress, Conn: c}
		// master connection
		if ep.Role == 0x01 {
			if h.AcceptMasterHandler != nil && !h.callout(func() { h.AcceptMasterHandler(pd) }) {
				c.Abort(errors.New("connection callback capacity exhausted"))
			}
			return
		}
		h.plistmu.Lock()
		if advertised := h.plist[ep.PeerAddress]; advertised != nil {
			*pd = *advertised
			pd.Conn = c
		}
		h.plistmu.Unlock()
		if h.AcceptSlaveHandler != nil && !h.callout(func() { h.AcceptSlaveHandler(pd) }) {
			c.Abort(errors.New("connection callback capacity exhausted"))
		}
	})
}

func (h *HCI) handleDisconnectionComplete(b []byte) error {
	ep := &evt.DisconnectionCompleteEP{}
	if err := ep.Unmarshal(b); err != nil {
		return err
	}
	if ep.Status != 0 {
		err := fmt.Errorf("hci: disconnect handle 0x%04x: status 0x%02x", ep.ConnectionHandle, ep.Status)
		h.requestClose(err)
		return err
	}
	h.connsmu.Lock()
	if c := h.conns[ep.ConnectionHandle]; c != nil {
		delete(h.conns, ep.ConnectionHandle)
		c.terminate(nil)
	}
	h.initLifecycle()
	select {
	case h.changed <- struct{}{}:
	default:
	}
	h.connsmu.Unlock()
	h.startWorker(func() {
		if err := h.setAdvertiseEnable(true); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			log.Printf("hci: re-enable advertising: %v", err)
		}
	})
	return nil
}

func (h *HCI) handleLEMeta(b []byte) error {
	if len(b) == 0 {
		return errors.New("hci: empty LE event")
	}
	code := evt.LEEventCode(b[0])
	switch code {
	case evt.LEConnectionComplete:
		h.handleConnection(b)
	case evt.LEConnectionUpdateComplete:
		// anything to do here?
	case evt.LEAdvertisingReport:
		h.handleAdvertisement(b)
	// case evt.LEReadRemoteUsedFeaturesComplete:
	// case evt.LELTKRequest:
	// case evt.LERemoteConnectionParameterRequest:
	default:
		return fmt.Errorf("Unhandled LE event: %s, [ % X ]", code, b)
	}
	return nil
}

func (h *HCI) handleL2CAP(b []byte) error {
	a := &aclData{}
	if err := a.unmarshal(b); err != nil {
		return err
	}
	h.connsmu.Lock()
	defer h.connsmu.Unlock()
	c, found := h.conns[a.attr]
	if !found {
		// should not happen, just be cautious for now.
		log.Printf("l2conn: got data for disconnected handle: 0x%04x", a.attr)
		return nil
	}
	select {
	case <-c.done:
		return nil
	default:
	}
	pb := a.flags & aclPBMask
	pdu := a.b
	if pb == aclPBContinuation {
		if c.rxPDU == nil {
			return nil
		}
		pdu = append(c.rxPDU, a.b...)
	}
	c.rxPDU = nil

	// The length field can itself span ACL fragments.
	pduLen := l2capHeaderLen
	if len(pdu) >= 2 {
		pduLen += int(binary.LittleEndian.Uint16(pdu))
	}
	if len(pdu) < pduLen && pb != aclPBComplete {
		c.rxPDU = pdu
		return nil
	}
	if len(pdu) != pduLen {
		log.Printf("l2conn: l2cap packet is too short/corrupt, length is %d, flags is 0x%02x", len(pdu), a.flags)
		return nil
	}
	cid := binary.LittleEndian.Uint16(pdu[2:])
	if cid == 5 {
		return c.handleSignal(pdu)
	}
	// Never block the HCI reader: transmit completion events share this loop.
	select {
	case c.aclc <- pdu[l2capHeaderLen:]:
		return nil
	default:
		c.stop()
		// connsmu is held here; register the worker before releasing it.
		if !h.closed {
			h.workers.Add(1)
			go func() {
				defer h.workers.Done()
				if err := c.Close(); err != nil {
					log.Printf("l2conn: receive queue overflow cleanup: %v", err)
				}
			}()
		}
		return fmt.Errorf("l2conn: receive queue full for handle 0x%04X, disconnecting", a.attr)
	}
}

func (h *HCI) trace(fmt string, v ...interface{}) {
	log.Printf(fmt, v)
}
