//go:build gattxpc_test

package gatt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/seangong/gatt/v2/xpc"
)

type darwinSubscription struct {
	central  Central
	notifier Notifier
}

func newDarwinSubscriptionTest(t *testing.T) (*device, [2]int, <-chan darwinSubscription) {
	t.Helper()
	added := make(chan [2]int, 1)
	d, _ := newDarwinClientTest(t, func(id int, args xpc.Dict) (int, xpc.Dict) {
		if id == 10 {
			chars := args.MustGetArray("kCBMsgArgCharacteristics")
			added <- [2]int{
				chars[0].(xpc.Dict).MustGetInt("kCBMsgArgAttributeID"),
				chars[1].(xpc.Dict).MustGetInt("kCBMsgArgAttributeID"),
			}
			return 18, xpc.Dict{"kCBMsgArgResult": int64(0)}
		}
		return 0, nil
	})
	notifiers := make(chan darwinSubscription, 4)
	s := NewService(UUID16(0xfff0))
	for _, uuid := range []uint16{0xfff1, 0xfff2} {
		s.AddCharacteristic(UUID16(uuid)).HandleNotifyFunc(func(r Request, n Notifier) {
			notifiers <- darwinSubscription{central: r.Central, notifier: n}
		})
	}
	if err := d.AddService(s); err != nil {
		t.Fatal(err)
	}
	return d, receiveDarwin(t, added), notifiers
}

func darwinSubscriptionEventForTest(d *device, event int, uuid xpc.UUID, attribute int) {
	d.HandleXpcEvent(xpc.Dict{"kCBMsgId": int64(event), "kCBMsgArgs": xpc.Dict{
		"kCBMsgArgDeviceUUID": uuid, "kCBMsgArgAttributeID": int64(attribute),
	}}, nil)
}

func TestDarwinUnsubscribeReleasesEmptyCentrals(t *testing.T) {
	d, attrs, notifiers := newDarwinSubscriptionTest(t)
	for i := 1; i <= 100; i++ {
		uuid := xpc.MakeUUID(fmt.Sprintf("%032x", i))
		darwinSubscriptionEventForTest(d, 21, uuid, attrs[0])
		created := receiveDarwin(t, notifiers)
		darwinSubscriptionEventForTest(d, 22, uuid, attrs[0])
		if !created.notifier.Done() {
			t.Fatal("notifier remained active after unsubscribe")
		}
	}
	d.subscribersmu.Lock()
	remaining := len(d.subscribers)
	d.subscribersmu.Unlock()
	if remaining != 0 {
		t.Fatalf("unsubscribed centrals retained = %d, want 0", remaining)
	}
}

func TestDarwinUnsubscribePreservesOtherCharacteristicsAndResubscription(t *testing.T) {
	d, attrs, notifiers := newDarwinSubscriptionTest(t)
	uuid := xpc.MakeUUID("00112233445566778899aabbccddeeff")
	darwinSubscriptionEventForTest(d, 21, uuid, attrs[0])
	first := receiveDarwin(t, notifiers)
	darwinSubscriptionEventForTest(d, 21, uuid, attrs[1])
	second := receiveDarwin(t, notifiers)
	if first.central != second.central {
		t.Fatal("characteristics on the same central have different owners")
	}
	darwinSubscriptionEventForTest(d, 22, uuid, attrs[0])
	darwinSubscriptionEventForTest(d, 22, uuid, attrs[0])
	if !first.notifier.Done() || second.notifier.Done() {
		t.Fatal("unsubscribe stopped the wrong characteristic")
	}
	if _, err := first.notifier.Write([]byte("stale")); err == nil {
		t.Fatal("unsubscribed notifier accepted another value")
	}
	if _, err := second.notifier.Write([]byte("live")); err != nil {
		t.Fatal(err)
	}
	darwinSubscriptionEventForTest(d, 21, uuid, attrs[0])
	replacement := receiveDarwin(t, notifiers)
	if replacement.central != second.central || replacement.notifier == first.notifier {
		t.Fatal("resubscription did not preserve the active owner and replace its stopped notifier")
	}
	darwinSubscriptionEventForTest(d, 22, uuid, attrs[1])
	if !second.notifier.Done() || replacement.notifier.Done() {
		t.Fatal("canceling the second characteristic stopped its replacement sibling")
	}
	darwinSubscriptionEventForTest(d, 22, uuid, attrs[0])
	darwinSubscriptionEventForTest(d, 21, uuid, attrs[0])
	reopened := receiveDarwin(t, notifiers)
	if reopened.central == first.central || reopened.central.ID() != first.central.ID() || reopened.notifier.Done() {
		t.Fatal("resubscription after the last unsubscribe reused the retired owner")
	}
}

func TestDarwinConcurrentSubscribeAndUnsubscribeKeepActiveOwner(t *testing.T) {
	d, attrs, notifiers := newDarwinSubscriptionTest(t)
	uuid := xpc.MakeUUID("00112233445566778899aabbccddeeff")
	for i := 0; i < 100; i++ {
		darwinSubscriptionEventForTest(d, 21, uuid, attrs[0])
		first := receiveDarwin(t, notifiers)
		start := make(chan struct{})
		var operations sync.WaitGroup
		operations.Go(func() { <-start; darwinSubscriptionEventForTest(d, 22, uuid, attrs[0]) })
		operations.Go(func() { <-start; darwinSubscriptionEventForTest(d, 21, uuid, attrs[1]) })
		close(start)
		operations.Wait()
		second := receiveDarwin(t, notifiers)
		d.subscribersmu.Lock()
		owner := d.subscribers[second.central.ID()]
		d.subscribersmu.Unlock()
		if owner != second.central || !first.notifier.Done() || second.notifier.Done() {
			t.Fatalf("round %d left an active notifier without its registered owner", i)
		}
		darwinSubscriptionEventForTest(d, 22, uuid, attrs[1])
		if !second.notifier.Done() {
			t.Fatal("active notifier could not be canceled after concurrent registration")
		}
	}
}

func TestDarwinUnsubscribeCleanupKeepsServiceAndCloseCancellation(t *testing.T) {
	for _, remove := range []bool{true, false} {
		t.Run(fmt.Sprintf("remove_%t", remove), func(t *testing.T) {
			d, attrs, notifiers := newDarwinSubscriptionTest(t)
			uuid := xpc.MakeUUID("00112233445566778899aabbccddeeff")
			darwinSubscriptionEventForTest(d, 21, uuid, attrs[0])
			first := receiveDarwin(t, notifiers)
			darwinSubscriptionEventForTest(d, 22, uuid, attrs[0])
			darwinSubscriptionEventForTest(d, 21, uuid, attrs[1])
			second := receiveDarwin(t, notifiers)
			if remove {
				if err := d.RemoveAllServices(); err != nil {
					t.Fatal(err)
				}
			} else if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if !first.notifier.Done() || !second.notifier.Done() {
				t.Fatal("service removal or close left a notifier live")
			}
			darwinSubscriptionEventForTest(d, 21, uuid, attrs[0])
			d.subscribersmu.Lock()
			remaining := len(d.subscribers)
			active := 0
			for _, c := range d.subscribers {
				c.notifiersmu.Lock()
				active += len(c.notifiers)
				c.notifiersmu.Unlock()
			}
			d.subscribersmu.Unlock()
			if active != 0 || remaining != 0 {
				t.Fatalf("retired subscription registry contains %d owners and %d notifiers", remaining, active)
			}
		})
	}
}
