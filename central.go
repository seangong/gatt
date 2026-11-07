package gatt

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
)

// Central is the interface that represent a remote central device.
type Central interface {
	ID() string   // ID returns platform specific ID of the remote central device.
	Close() error // Close disconnects the connection.
	MTU() int     // MTU returns the current connection mtu.
}

type ResponseWriter interface {
	// Write writes data to return as the characteristic value.
	Write([]byte) (int, error)

	// SetStatus reports the result of the read operation. See the Status* constants.
	SetStatus(byte)
}

// responseWriter is the default implementation of ResponseWriter.
type responseWriter struct {
	capacity int
	buf      *bytes.Buffer
	status   byte
}

func newResponseWriter(c int) *responseWriter {
	return &responseWriter{
		capacity: c,
		buf:      new(bytes.Buffer),
		status:   StatusSuccess,
	}
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if avail := w.capacity - w.buf.Len(); avail < len(b) {
		return 0, fmt.Errorf("requested write %d bytes, %d available", len(b), avail)
	}
	return w.buf.Write(b)
}

func (w *responseWriter) SetStatus(status byte) { w.status = status }
func (w *responseWriter) bytes() []byte         { return w.buf.Bytes() }

// A ReadHandler handles GATT read requests.
type ReadHandler interface {
	ServeRead(resp ResponseWriter, req *ReadRequest)
}

// ReadHandlerFunc is an adapter to allow the use of
// ordinary functions as ReadHandlers. If f is a function
// with the appropriate signature, ReadHandlerFunc(f) is a
// ReadHandler that calls f.
type ReadHandlerFunc func(resp ResponseWriter, req *ReadRequest)

// ServeRead returns f(r, maxlen, offset).
func (f ReadHandlerFunc) ServeRead(resp ResponseWriter, req *ReadRequest) {
	f(resp, req)
}

// A WriteHandler handles GATT write requests.
// Write and WriteNR requests are presented identically;
// the server will ensure that a response is sent if appropriate.
type WriteHandler interface {
	ServeWrite(r Request, data []byte) (status byte)
}

// WriteHandlerFunc is an adapter to allow the use of
// ordinary functions as WriteHandlers. If f is a function
// with the appropriate signature, WriteHandlerFunc(f) is a
// WriteHandler that calls f.
type WriteHandlerFunc func(r Request, data []byte) byte

// ServeWrite returns f(r, data).
func (f WriteHandlerFunc) ServeWrite(r Request, data []byte) byte {
	return f(r, data)
}

// A NotifyHandler handles GATT notification requests.
// Notifications can be sent using the provided notifier. Handlers should
// observe Request.Context or Notifier.Done so they can release their resources
// when the connection ends.
type NotifyHandler interface {
	ServeNotify(r Request, n Notifier)
}

// NotifyHandlerFunc is an adapter to allow the use of
// ordinary functions as NotifyHandlers. If f is a function
// with the appropriate signature, NotifyHandlerFunc(f) is a
// NotifyHandler that calls f.
type NotifyHandlerFunc func(r Request, n Notifier)

// ServeNotify calls f(r, n).
func (f NotifyHandlerFunc) ServeNotify(r Request, n Notifier) {
	f(r, n)
}

// A Notifier provides a means for a GATT server to send
// notifications about value changes to a connected device.
// Notifiers are provided by NotifyHandlers.
type Notifier interface {
	// Write sends one value and returns the number of value bytes sent.
	// On Linux, data exceeding Cap returns io.ErrShortWrite without sending
	// a value. CCCD=2 selects indications; Write returns after transmission,
	// without waiting for confirmation. Until confirmed, another indication
	// returns ErrIndicationPending. CCCD=1 or 3 selects notifications.
	Write(data []byte) (int, error)

	// Done reports whether the central has requested not to
	// receive any more notifications with this notifier. A transport write
	// already in progress may finish after Done becomes true.
	Done() bool

	// Cap returns the maximum number of bytes that may be sent
	// in a single notification.
	Cap() int
}

// ErrIndicationPending means the previous indication on this connection has
// not been confirmed. The caller may retry after the client confirms it.
var ErrIndicationPending = errors.New("central has an unconfirmed indication")

type notifier struct {
	central *central
	a       *attr
	maxlen  int
	done    atomic.Bool
	ccc     uint16 // Protected by central.notifiersmu on Linux.
}

func newNotifier(c *central, a *attr, maxlen int) *notifier {
	return &notifier{central: c, a: a, maxlen: maxlen, ccc: gattCCCNotifyFlag}
}

func (n *notifier) Write(b []byte) (int, error) {
	if n.done.Load() {
		return 0, errors.New("central stopped notifications")
	}
	return n.central.sendNotification(n, b)
}

func (n *notifier) Done() bool {
	return n.done.Load()
}

func (n *notifier) stop() {
	n.done.Store(true)
}
