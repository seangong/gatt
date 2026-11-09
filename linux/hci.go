package linux

import (
	"container/list"
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

	plist    map[peerAddress]*advertisementEntry
	plistmu  *sync.Mutex
	plistLRU list.List // Protected by plistmu.

	bufCnt  chan struct{}
	bufSize int
	// aclSubmit serializes one non-blocking packet submission with credit and
	// connection lifecycle events. It is never held across a poll wait.
	aclSubmit sync.Mutex

	maxConn int
	connsmu *sync.Mutex
	conns   map[uint16]*conn
	closed  bool // Protected by connsmu.

	closeOnce       sync.Once
	closeDone       chan struct{}
	stopping        chan struct{}
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

	adv     bool
	advmu   *sync.Mutex
	advGate chan struct{} // Serialize advertising intent and complete configuration operations.
}

type peerAddress struct {
	address     [6]byte
	addressType uint8
}

// The advertising payload is kept separately from the latest combined report.
// A Connect argument can seed metadata without representing an observed ADV.
type advertisementEntry struct {
	data             PlatData
	advertisingData  []byte
	hasAdvertisement bool
	element          *list.Element
}

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
	report        PlatData // Owned Connect metadata, retained even if the scan cache evicts this peer.
	knownName     string   // Immutable name accepted with the original Connect argument.
	address       [6]byte
	addressType   uint8
	createDone    chan struct{}
	done          chan struct{}
	conn          *conn
	terminalErr   error
	cancelStarted bool
	cancelDone    chan struct{}
	cancelErr     error
	result        chan ConnectionResult
	resultDone    bool
	withResult    bool
}

func newConnectionAttempt(pd *PlatData) *connectionAttempt {
	report := *pd
	report.Data = append([]byte(nil), pd.Data...)
	return &connectionAttempt{
		source: pd, report: report, knownName: pd.Name, address: pd.Address, addressType: pd.AddressType,
		createDone: make(chan struct{}), done: make(chan struct{}), cancelDone: make(chan struct{}),
		result: make(chan ConnectionResult, 1),
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

		plist:   make(map[peerAddress]*advertisementEntry),
		plistmu: &sync.Mutex{},

		bufCnt:  make(chan struct{}, 15-1),
		bufSize: 27,

		maxConn: maxConn,
		connsmu: &sync.Mutex{},
		conns:   map[uint16]*conn{},

		advmu: &sync.Mutex{},
	}

	h.registerEventHandlers()

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

func (h *HCI) registerEventHandlers() {
	h.e.HandleEvent(evt.LEMeta, evt.HandlerFunc(h.handleLEMeta))
	h.e.HandleEvent(evt.DisconnectionComplete, evt.HandlerFunc(h.handleDisconnectionComplete))
	h.e.HandleEvent(evt.NumberOfCompletedPkts, evt.HandlerFunc(h.handleNumberOfCompletedPkts))
	h.e.HandleEvent(evt.CommandComplete, evt.HandlerFunc(h.c.HandleComplete))
	h.e.HandleEvent(evt.CommandStatus, evt.HandlerFunc(h.c.HandleStatus))
	h.e.HandleEvent(evt.HardwareError, evt.HandlerFunc(h.handleHardwareError))
}

// Controller failure makes queued work and connection state unreliable. Use
// asynchronous shutdown so the HCI receiver never waits for its own completion.
func (h *HCI) handleHardwareError(b []byte) error {
	var err error
	if len(b) != 1 {
		err = fmt.Errorf("hci: invalid hardware-error event length %d, want 1", len(b))
	} else {
		err = fmt.Errorf("hci: controller hardware error 0x%02x", b[0])
	}
	h.requestClose(err)
	return err
}

// initLifecycle is called with connsmu held. Tests can construct an HCI directly.
func (h *HCI) initLifecycle() {
	if h.advmu == nil {
		h.advmu = &sync.Mutex{}
	}
	if h.closeDone == nil {
		h.closeDone = make(chan struct{})
		h.stopping = make(chan struct{})
		h.advGate = make(chan struct{}, 1)
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
	resultErr := h.closeCause
	if resultErr == nil {
		resultErr = context.Canceled
	}
	if h.pending != nil {
		h.pending.finishResult(nil, resultErr)
	}
	for _, c := range h.conns {
		if h.closeCause == nil {
			c.intentionalClose = true
		}
		c.finishConnect(resultErr)
		c.stop()
	}
	done := h.closeDone
	h.closeOnce.Do(func() {
		close(h.stopping)
		go h.shutdown()
	})
	h.connsmu.Unlock()
	return done
}

func (h *HCI) Close() error {
	<-h.requestClose(nil)
	return h.closeErr
}

// Done is closed when HCI shutdown starts, including after a controller or
// transport failure. It does not signal that resources have been released;
// Close waits for shutdown to finish and returns its result.
func (h *HCI) Done() <-chan struct{} {
	h.connsmu.Lock()
	defer h.connsmu.Unlock()
	h.initLifecycle()
	return h.stopping
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
	if h.plistmu != nil {
		h.plistmu.Lock()
		h.plist = make(map[peerAddress]*advertisementEntry)
		h.plistLRU.Init()
		h.plistmu.Unlock()
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
		var creditErr error
		h.transportClosed = true
		if a := h.pending; a != nil {
			a.created()
			a.complete(nil, io.ErrClosedPipe)
			resultErr := h.closeCause
			if resultErr == nil {
				resultErr = context.Canceled
			}
			a.finishResult(nil, resultErr)
			h.pending = nil
		}
		for handle, c := range h.conns {
			c.terminate(io.ErrClosedPipe)
			creditErr = errors.Join(creditErr, c.releaseACL())
			delete(h.conns, handle)
		}
		h.connsmu.Unlock()
		h.transportErr = errors.Join(creditErr, h.d.Close())
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
	if err := h.lockAdvertising(); err != nil {
		return err
	}
	defer func() { <-h.advGate }()
	h.advmu.Lock()
	h.adv = en
	h.advmu.Unlock()
	return h.setAdvertiseEnableLocked(en)
}

// lockAdvertising never holds a state mutex while waiting. Shutdown bypasses
// this gate and wakes its waiters before starting controller cleanup.
func (h *HCI) lockAdvertising() error {
	h.connsmu.Lock()
	h.initLifecycle()
	closed, stopping, gate := h.closed, h.stopping, h.advGate
	h.connsmu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	select {
	case <-stopping:
		return io.ErrClosedPipe
	case gate <- struct{}{}:
	}
	h.connsmu.Lock()
	closed = h.closed
	h.connsmu.Unlock()
	if closed {
		<-gate
		return io.ErrClosedPipe
	}
	return nil
}

// Automatic restoration uses the current intent, not the state when the
// connection event queued this operation.
func (h *HCI) setAdvertiseEnable(en bool) error {
	if err := h.lockAdvertising(); err != nil {
		return err
	}
	defer func() { <-h.advGate }()
	return h.setAdvertiseEnableLocked(en)
}

// setAdvertiseEnableLocked requires advGate, including when pausing advertising
// for a configuration command. It does not change the requested advertising state.
func (h *HCI) setAdvertiseEnableLocked(en bool) error {
	h.connsmu.Lock()
	closed, connectionCount := h.closed, len(h.conns)
	h.connsmu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	h.advmu.Lock()
	requested := h.adv
	h.advmu.Unlock()
	if en && (!requested || connectionCount == h.maxConn) {
		return nil
	}
	if err := h.c.SendAndCheckResp(cmd.LESetAdvertiseEnable{AdvertisingEnable: btoi(en)}, []byte{0}); err != nil {
		return fmt.Errorf("hci: set advertising enabled=%t: %w", en, err)
	}
	return nil
}

func (h *HCI) SendCmdWithAdvOff(cp cmd.CmdParam) error {
	if err := h.lockAdvertising(); err != nil {
		return err
	}
	defer func() { <-h.advGate }()
	h.advmu.Lock()
	advertising := h.adv
	h.advmu.Unlock()
	// Reset leaves advertising disabled. Only pause an active advertising
	// configuration before updating it.
	if advertising {
		if err := h.setAdvertiseEnableLocked(false); err != nil {
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
		if restoreErr := h.setAdvertiseEnableLocked(true); restoreErr != nil {
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
	_, err := h.connect(pd, false)
	return err
}

func (h *HCI) connect(pd *PlatData, withResult bool) (*connectionAttempt, error) {
	h.connsmu.Lock()
	if h.closed {
		h.connsmu.Unlock()
		return nil, io.ErrClosedPipe
	}
	if h.pending != nil {
		h.connsmu.Unlock()
		return nil, errors.New("hci: connection attempt already pending")
	}
	a := newConnectionAttempt(pd)
	a.withResult = withResult
	h.pending = a
	h.initLifecycle()
	h.connsmu.Unlock()
	// Keep the caller's parsed name without sharing mutable advertisement data.
	report := a.report
	report.Data = append([]byte(nil), a.report.Data...)
	h.plistmu.Lock()
	select {
	case <-h.stopping:
		// A close may finish before this admitted sender reaches the cache.
	default:
		key := peerAddress{a.address, a.addressType}
		h.advertisementLocked(key, true).data = report
	}
	h.plistmu.Unlock()
	_, err := h.c.Send(
		cmd.LECreateConn{
			LEScanInterval:        0x0004,        // N x 0.625ms
			LEScanWindow:          0x0004,        // N x 0.625ms
			InitiatorFilterPolicy: 0x00,          // white list not used
			PeerAddressType:       a.addressType, // public or random
			PeerAddress:           a.address,     //
			OwnAddressType:        0x00,          // public
			ConnIntervalMin:       0x0006,        // N x 0.125ms
			ConnIntervalMax:       0x0006,        // N x 0.125ms
			ConnLatency:           0x0000,        //
			SupervisionTimeout:    0x000A,        // N x 10ms
			MinimumCELength:       0x0000,        // N x 0.625ms
			MaximumCELength:       0x0000,        // N x 0.625ms
		})
	var abortConn *conn
	h.connsmu.Lock()
	a.created()
	if err != nil {
		a.complete(nil, nil)
		if a.finishResult(nil, err) && a.conn != nil {
			// An early physical completion can race a rejected command status.
			// A failed result must not subsequently publish that connection.
			abortConn = a.conn
		}
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
	if abortConn != nil {
		abortConn.Abort(err)
	}
	return a, err
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
	if c, ok := target.(*conn); ok && h.conns[c.attr] == c {
		select {
		case <-c.done:
		default:
			c.intentionalClose = true
			c.finishConnect(context.Canceled)
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
	a.finishResult(nil, context.Canceled)
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
	// The producer can still be publishing the terminal outcome after closing
	// createDone. Read that outcome under the same lock before choosing Cancel.
	h.connsmu.Lock()
	select {
	case <-a.done:
		h.connsmu.Unlock()
		return h.disconnectAttempt(ctx, a)
	default:
		h.connsmu.Unlock()
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
		// A malformed lifecycle/flow-control event can leave credits or
		// connection ownership unknowable. Unsupported events and advertising
		// errors retain their previous non-fatal handling.
		if err != nil && len(b) != 0 {
			if b[0] == evt.HardwareError {
				h.requestClose(fmt.Errorf("hci: invalid hardware-error event: %w", err))
			} else if b[0] == evt.CommandComplete || b[0] == evt.CommandStatus {
				// Once a command response cannot be parsed, its transaction and
				// command allowance cannot safely be recovered. A closed Cmd can
				// also reject a valid response during normal transport shutdown.
				if !errors.Is(err, io.ErrClosedPipe) {
					h.requestClose(fmt.Errorf("hci: invalid command event 0x%02x: %w", b[0], err))
				}
			} else if b[0] == evt.NumberOfCompletedPkts || b[0] == evt.DisconnectionComplete ||
				b[0] == evt.LEMeta && len(b) > 2 && b[2] == byte(evt.LEConnectionComplete) {
				h.requestClose(err)
			}
		}
	case typVendorPkt:
		err = fmt.Errorf("Vendor packet not supported")
	default:
		h.requestClose(fmt.Errorf("Unknown event: 0x%02X [ % X ]", t, b))
	}
	if err != nil {
		log.Printf("hci: %s, [ % X]", err, b)
	}
}

type readLocalSupportedFeatures struct{}

func (readLocalSupportedFeatures) Opcode() int    { return 0x1003 }
func (readLocalSupportedFeatures) Len() int       { return 0 }
func (readLocalSupportedFeatures) Marshal([]byte) {}

func (h *HCI) resetDevice(ctx context.Context) error {
	if err := h.c.SendAndCheckRespContext(ctx, cmd.Reset{}, []byte{0x00}); err != nil {
		return err
	}
	features, err := h.c.SendContext(ctx, readLocalSupportedFeatures{})
	if err != nil {
		return fmt.Errorf("read controller features (0x1003): %w", err)
	}
	if len(features) == 0 || features[0] != 0 {
		return fmt.Errorf("read controller features (0x1003): invalid response [% X]", features)
	}
	if len(features) != 9 {
		return fmt.Errorf("read controller features (0x1003): response length %d, want 9", len(features))
	}
	// Feature bit 37 marks controllers without BR/EDR. Do not require the
	// LE Supported bit here: LnxDeviceID explicitly allows bypassing that check
	// for drivers which report it incorrectly.
	brEdr := features[5]&0x20 == 0
	seq := []cmd.CmdParam{
		cmd.SetEventMask{EventMask: 0x3dbff807fffbffff},
		cmd.LESetEventMask{LEEventMask: 0x000000000000001F},
	}
	if brEdr {
		// Preserve the existing dual-mode setup. LE-only controllers exclude
		// these BR/EDR settings. HostBufferSize is not needed for LE-only setup:
		// controller-to-host flow control is never enabled by this package.
		seq = append(seq,
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
		)
	}
	seq = append(seq,
		cmd.LESetScanParameters{
			LEScanType:           0x01,   // [0x00]: passive, 0x01: active
			LEScanInterval:       0x0010, // [0x10]: 0.625ms * 16
			LEScanWindow:         0x0010, // [0x10]: 0.625ms * 16
			OwnAddressType:       0x00,   // [0x00]: public, 0x01: random
			ScanningFilterPolicy: 0x00,   // [0x00]: accept all, 0x01: ignore non-white-listed.
		},
	)
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
		addr := peerAddress{ep.Address[i], ep.AddressType[i]}
		et := ep.EventType[i]
		connectable := et == advInd || et == advDirectInd
		scannable := et == advInd || et == advScanInd
		data := advertisingDataPrefix(ep.Data[i])

		if et == scanRsp {
			h.plistmu.Lock()
			entry := h.advertisementLocked(addr, false)
			var report PlatData
			found := entry != nil && entry.hasAdvertisement
			if found {
				combined := make([]byte, 0, len(entry.advertisingData)+len(data))
				combined = append(combined, entry.advertisingData...)
				entry.data.Data = append(combined, data...)
				report = entry.data
				report.Data = append([]byte(nil), report.Data...)
			}
			h.plistmu.Unlock()
			if found {
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
		entry := h.advertisementLocked(addr, true)
		entry.data, entry.advertisingData, entry.hasAdvertisement = *pd, data, true
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
		h.requestClose(fmt.Errorf("hci: invalid completed-packets event: %w", err))
		return err
	}
	h.aclSubmit.Lock()
	h.connsmu.Lock()
	// Validate the entire event before returning any credits. Duplicate handle
	// records, if present, are included in the same connection's total.
	completed := make(map[*conn]int, len(ep.Packets))
	var err error
	total := 0
	for _, r := range ep.Packets {
		if r.NumOfCompletedPkts == 0 {
			continue
		}
		c := h.conns[r.ConnectionHandle]
		if c == nil || completed[c]+int(r.NumOfCompletedPkts) > c.aclOutstanding {
			err = fmt.Errorf("hci: completion exceeds outstanding ACL packets for handle 0x%04x", r.ConnectionHandle)
			break
		}
		completed[c] += int(r.NumOfCompletedPkts)
		total += int(r.NumOfCompletedPkts)
	}
	if err == nil && total > len(h.bufCnt) {
		err = errors.New("hci: inconsistent ACL credit accounting")
	}
	if err == nil {
		for c, count := range completed {
			c.aclOutstanding -= count
			for i := 0; i < count; i++ {
				<-h.bufCnt
			}
		}
	}
	h.connsmu.Unlock()
	h.aclSubmit.Unlock()
	if err != nil {
		h.requestClose(err)
	}
	return err
}

func (h *HCI) handleConnection(b []byte) {
	ep := &evt.LEConnectionCompleteEP{}
	if err := ep.Unmarshal(b); err != nil {
		h.requestClose(fmt.Errorf("hci: invalid connection-complete event: %w", err))
		return
	}
	h.aclSubmit.Lock()
	h.connsmu.Lock()
	h.initLifecycle()
	a := h.pending
	matches := a != nil && (ep.Status != 0 || ep.Role == 0x00 &&
		a.address == ep.PeerAddress && a.addressType == ep.PeerAddressType)
	if ep.Status != 0 || h.transportClosed || h.closeCause != nil {
		if matches {
			a.complete(nil, nil)
			resultErr := h.closeCause
			if resultErr == nil {
				if ep.Status != 0 {
					resultErr = fmt.Errorf("LE Create Connection failed: HCI status 0x%02x", ep.Status)
				} else {
					resultErr = io.ErrClosedPipe
				}
			}
			a.finishResult(nil, resultErr)
			if !a.cancelStarted {
				h.pending = nil
			}
		}
		select {
		case h.changed <- struct{}{}:
		default:
		}
		h.connsmu.Unlock()
		h.aclSubmit.Unlock()
		return
	}
	if previous := h.conns[ep.ConnectionHandle]; previous != nil {
		h.connsmu.Unlock()
		h.aclSubmit.Unlock()
		h.requestClose(fmt.Errorf("hci: active connection handle 0x%04x reused before disconnection", ep.ConnectionHandle))
		return
	}
	c := newConn(h, ep.ConnectionHandle)
	canceling := false
	if matches {
		c.origin = a.source
		c.attempt = a
		a.complete(c, nil)
		canceling = a.cancelStarted
		if !canceling {
			h.pending = nil
		}
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
	h.aclSubmit.Unlock()
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
		pd := &PlatData{Address: ep.PeerAddress, AddressType: ep.PeerAddressType, Conn: c}
		// master connection
		if ep.Role == 0x01 {
			if h.AcceptMasterHandler != nil && !h.callout(func() { h.AcceptMasterHandler(pd) }) {
				c.Abort(errors.New("connection callback capacity exhausted"))
			}
			return
		}
		if matches {
			*pd = a.report
			pd.Data = append([]byte(nil), a.report.Data...)
			pd.Conn = c
		}
		h.plistmu.Lock()
		if advertised := h.advertisementLocked(peerAddress{ep.PeerAddress, ep.PeerAddressType}, false); advertised != nil {
			*pd = advertised.data
			pd.Data = append([]byte(nil), advertised.data.Data...)
			pd.Conn = c
		}
		h.plistmu.Unlock()
		if matches && pd.Name == "" {
			pd.Name = a.knownName
		}
		if matches && a.withResult {
			h.connsmu.Lock()
			if c.attempt == a && !a.resultDone {
				a.finishResult(pd, nil)
				c.attempt = nil
			}
			h.connsmu.Unlock()
			return
		}
		if h.AcceptSlaveHandler != nil && !h.callout(func() { h.AcceptSlaveHandler(pd) }) {
			c.Abort(errors.New("connection callback capacity exhausted"))
			return
		}
		h.connsmu.Lock()
		if c.attempt == a && a != nil {
			a.finishResult(pd, nil)
			c.attempt = nil
		}
		h.connsmu.Unlock()
	})
}

func (h *HCI) handleDisconnectionComplete(b []byte) error {
	ep := &evt.DisconnectionCompleteEP{}
	if err := ep.Unmarshal(b); err != nil {
		h.requestClose(fmt.Errorf("hci: invalid disconnection-complete event: %w", err))
		return err
	}
	if ep.Status != 0 {
		err := fmt.Errorf("hci: disconnect handle 0x%04x: status 0x%02x", ep.ConnectionHandle, ep.Status)
		h.requestClose(err)
		return err
	}
	h.aclSubmit.Lock()
	h.connsmu.Lock()
	var creditErr error
	if c := h.conns[ep.ConnectionHandle]; c != nil {
		delete(h.conns, ep.ConnectionHandle)
		c.terminate(nil)
		creditErr = c.releaseACL()
	}
	h.initLifecycle()
	select {
	case h.changed <- struct{}{}:
	default:
	}
	h.connsmu.Unlock()
	h.aclSubmit.Unlock()
	if creditErr != nil {
		h.requestClose(creditErr)
		return creditErr
	}
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
		return fmt.Errorf("Unhandled LE event: 0x%02X, [ % X ]", code, b)
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
	if cid != 4 {
		return nil
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
	log.Printf(fmt, v...)
}
