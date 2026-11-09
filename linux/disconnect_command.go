package linux

import (
	"errors"
	"io"

	"github.com/seangong/gatt/v2/linux/cmd"
)

var errRetiredDisconnect = errors.New("l2conn: connection retired before Disconnect submission")

// boundDisconnect keeps the connection identity until its command reaches the
// controller. A handle alone is insufficient after a queued or polled wait.
type boundDisconnect struct {
	cmd.Disconnect
	owner *conn
}

func (p *boundDisconnect) UnsentCancellation() (<-chan struct{}, error) {
	return p.owner.linkDone, errRetiredDisconnect
}

func (p *boundDisconnect) WriteCommand(transport io.Writer, packet []byte) (int, error) {
	writer, ok := transport.(aclPacketWriter)
	if !ok {
		return 0, errors.New("hci: transport does not support controlled Disconnect submission")
	}
	h := p.owner.hci
	// The controlled packet writer uses a single non-blocking attempt per
	// callback, releasing this fence before any wait for socket writability.
	return writer.writeACL(packet, func(attempt func() (int, error)) (int, error) {
		h.aclSubmit.Lock()
		defer h.aclSubmit.Unlock()
		h.connsmu.Lock()
		retired := h.transportClosed || p.owner.linkClosed || h.conns[p.owner.attr] != p.owner
		h.connsmu.Unlock()
		if retired {
			return 0, errRetiredDisconnect
		}
		return attempt()
	})
}
