package linux

import (
	"errors"
	"fmt"
	"io"
)

// aclSubmitFunc surrounds a single non-blocking transport attempt. The
// transport releases it before waiting for writability and calls it again on
// every retry. Ordinary io.Writer.Write cannot provide this guarantee.
type aclSubmitFunc func(attempt func() (int, error)) (int, error)

type aclPacketWriter interface {
	writeACL([]byte, aclSubmitFunc) (int, error)
}

type aclReservation struct {
	held bool // An acquired credit not yet accepted by the controller.
}

// writeACL owns the credit already acquired by conn.write. Only submission
// transfers it from a local reservation to this connection's outstanding count.
func (c *conn) writeACL(packet []byte) error {
	h := c.hci
	r := &aclReservation{held: true}
	h.connsmu.Lock()
	c.aclReserved = r
	h.connsmu.Unlock()
	defer func() {
		h.connsmu.Lock()
		missing := false
		if r.held {
			select {
			case <-h.bufCnt:
			default:
				missing = true
			}
			r.held = false
		}
		if c.aclReserved == r {
			c.aclReserved = nil
		}
		h.connsmu.Unlock()
		if missing {
			h.requestClose(errors.New("hci: missing reserved ACL credit"))
		}
	}()
	writer, ok := h.d.(aclPacketWriter)
	if !ok {
		err := errors.New("hci: transport does not support controlled ACL submission")
		h.requestClose(err)
		return err
	}
	retired := false
	n, err := writer.writeACL(packet, func(attempt func() (int, error)) (int, error) {
		h.aclSubmit.Lock()
		defer h.aclSubmit.Unlock()
		h.connsmu.Lock()
		select {
		case <-c.done:
			retired = true
		default:
		}
		if h.closed || h.transportClosed || h.conns[c.attr] != c || !r.held {
			retired = true
		}
		h.connsmu.Unlock()
		if retired {
			return 0, io.ErrClosedPipe
		}
		// The transport contract permits exactly one non-blocking syscall here.
		// Lifecycle handlers cannot retire/reuse the handle before publication.
		n, err := attempt()
		if err == nil && n == len(packet) {
			h.connsmu.Lock()
			if r.held {
				r.held = false
				c.aclReserved = nil
				c.aclOutstanding++
			}
			h.connsmu.Unlock()
		}
		return n, err
	})
	if err == nil && n != len(packet) {
		err = io.ErrShortWrite
	}
	if err != nil && !retired {
		// A partial/failed native packet has uncertain submission state. End the
		// transport instead of allowing a later completion to match new traffic.
		h.requestClose(fmt.Errorf("hci: write ACL packet: %w", err))
	}
	return err
}

// releaseACL runs while connsmu is held. A local reservation is separate from
// accepted packets, so disconnect and a later poll retry cannot refund twice.
func (c *conn) releaseACL() error {
	h := c.hci
	n := c.aclOutstanding
	if c.aclReserved != nil && c.aclReserved.held {
		n++
	}
	if n > len(h.bufCnt) {
		return errors.New("hci: inconsistent ACL credit accounting")
	}
	for i := 0; i < n; i++ {
		<-h.bufCnt
	}
	c.aclOutstanding = 0
	if c.aclReserved != nil {
		c.aclReserved.held = false
		c.aclReserved = nil
	}
	return nil
}
