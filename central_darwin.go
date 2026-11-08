package gatt

import (
	"context"
	"sync"

	"github.com/seangong/gatt/v2/xpc"
)

type central struct {
	dev         *device
	uuid        UUID
	mtu         int
	notifiers   map[uint16]*notifier
	notifiersmu *sync.Mutex
}

func newCentral(d *device, u UUID) *central {
	return &central{
		dev:         d,
		mtu:         23,
		uuid:        u,
		notifiers:   make(map[uint16]*notifier),
		notifiersmu: &sync.Mutex{},
	}
}

func (c *central) ID() string   { return c.uuid.String() }
func (c *central) Close() error { return nil }
func (c *central) MTU() int     { return c.mtu }

// Context allows handlers to cooperate with device shutdown.
func (c *central) Context() context.Context { return c.dev.ctx }

func (n *notifier) Cap() int { return n.maxlen }

func (c *central) sendNotification(n *notifier, b []byte) (int, error) {
	a := n.a
	data := make([]byte, len(b))
	copy(data, b) // have to make a copy, why?
	if err := c.dev.sendCmd(15, xpc.Dict{
		// "kCBMsgArgUUIDs": [][]byte{reverse(c.uuid.b)}, // connection interrupted
		// "kCBMsgArgUUIDs": [][]byte{c.uuid.b}, // connection interrupted
		// "kCBMsgArgUUIDs": []xpc.UUID{xpc.UUID(reverse(c.uuid.b))},
		// "kCBMsgArgUUIDs": []xpc.UUID{xpc.UUID(c.uuid.b)},
		// "kCBMsgArgUUIDs": reverse(c.uuid.b),
		//
		// FIXME: Sigh... tried to targeting the central, but couldn't get work.
		// So, broadcast to all subscribed centrals. Either of the following works.
		// "kCBMsgArgUUIDs": []xpc.UUID{},
		"kCBMsgArgUUIDs":       [][]byte{},
		"kCBMsgArgAttributeID": a.h,
		"kCBMsgArgData":        data,
	}); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *central) startNotify(a *attr, maxlen int) {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	if context.Cause(c.dev.ctx) != nil {
		return
	}
	if _, found := c.notifiers[a.h]; found {
		return
	}
	n := newNotifier(c, a, maxlen)
	c.notifiers[a.h] = n
	char := a.pvt.(*Characteristic)
	go char.nhandler.ServeNotify(Request{Central: c}, n)
}

func (c *central) stopNotify(a *attr) {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	if n, found := c.notifiers[a.h]; found {
		n.stop()
		delete(c.notifiers, a.h)
	}
}

func (c *central) stopNotifiers() {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	for h, n := range c.notifiers {
		n.stop()
		delete(c.notifiers, h)
	}
}
