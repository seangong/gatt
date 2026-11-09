package gatt

import (
	"context"
	"io"
	"sync"

	"github.com/seangong/gatt/v2/xpc"
)

type central struct {
	dev         *device
	uuid        UUID
	mtu         int
	notifiers   map[int]*notifier
	notifiersmu *sync.Mutex
}

func newCentral(d *device, u UUID) *central {
	return &central{
		dev:         d,
		mtu:         23,
		uuid:        u,
		notifiers:   make(map[int]*notifier),
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
	c.dev.attrmu.RLock()
	if err := context.Cause(c.dev.ctx); err != nil {
		c.dev.attrmu.RUnlock()
		return 0, err
	}
	if n.Done() {
		c.dev.attrmu.RUnlock()
		return 0, io.ErrClosedPipe
	}
	err := c.dev.conn.SendError(xpc.Dict{"kCBMsgId": 15, "kCBMsgArgs": xpc.Dict{
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
		"kCBMsgArgAttributeID": a.nativeID,
		"kCBMsgArgData":        data,
	}}, false)
	c.dev.attrmu.RUnlock()
	if err != nil {
		c.dev.fail(err)
		return 0, context.Cause(c.dev.ctx)
	}
	return len(b), nil
}

func (c *central) startNotify(a *attr, maxlen int) {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	if context.Cause(c.dev.ctx) != nil {
		return
	}
	if _, found := c.notifiers[a.nativeID]; found {
		return
	}
	n := newNotifier(c, a, maxlen)
	c.notifiers[a.nativeID] = n
	char := a.pvt.(*Characteristic)
	go char.nhandler.ServeNotify(Request{Central: c}, n)
}

// stopNotify reports whether the central has no remaining subscriptions.
func (c *central) stopNotify(a *attr) bool {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	if n, found := c.notifiers[a.nativeID]; found {
		n.stop()
		delete(c.notifiers, a.nativeID)
	}
	return len(c.notifiers) == 0
}

func (c *central) stopNotifiers() {
	c.notifiersmu.Lock()
	defer c.notifiersmu.Unlock()
	for h, n := range c.notifiers {
		n.stop()
		delete(c.notifiers, h)
	}
}
