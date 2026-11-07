package linux

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func TestAdvertisementCalloutOwnsItsReport(t *testing.T) {
	received := make(chan *PlatData, 1)
	release, returned := make(chan struct{}), make(chan struct{})
	h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[bdaddr]*PlatData)}
	h.AdvertisementHandler = func(pd *PlatData) { received <- pd; <-release; close(returned) }
	defer func() {
		close(release)
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Error("advertisement callback did not exit")
		}
	}()
	report := func(kind byte, value byte) []byte {
		return []byte{2, 1, kind, 0, 1, 2, 3, 4, 5, 6, 1, value, 0}
	}
	h.handleAdvertisement(report(advInd, 'a'))
	h.handleAdvertisement(report(scanRsp, 'b'))
	var pd *PlatData
	select {
	case pd = <-received:
	case <-time.After(time.Second):
		t.Fatal("advertisement callback did not start")
	}
	// The second report updates the cache while the first callout is still
	// running. Neither side can mutate the other side's snapshot.
	h.handleAdvertisement(report(scanRsp, 'c'))
	if !bytes.Equal(pd.Data, []byte("ab")) {
		t.Fatalf("callback report changed: %q", pd.Data)
	}
	pd.Name = "parsed name"
	pd.Data[0] = 'x'
	cached := h.plist[pd.Address]
	if !bytes.Equal(cached.Data, []byte("abc")) || cached.Name != "" {
		t.Fatalf("callback changed cached report: %+v", cached)
	}
}
