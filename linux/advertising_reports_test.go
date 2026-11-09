package linux

import (
	"bytes"
	"sync"
	"testing"
	"testing/synctest"
)

func TestMultipleAdvertisingReportsReachCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &HCI{
			connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{},
			plist: make(map[peerAddress]*advertisementEntry), maxConn: 2,
		}
		reports := make(chan *PlatData, 2)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		packet := []byte{2, 2,
			advNonconnInd, 0, 1, 2, 3, 4, 5, 6, 3, 2, 9, 'a', 0xc4,
			advNonconnInd, 1, 11, 12, 13, 14, 15, 16, 3, 2, 9, 'b', 0xc5}
		for length := 0; length < len(packet); length++ {
			h.handleAdvertisement(packet[:length])
		}
		synctest.Wait()
		if len(reports) != 0 || len(h.plist) != 0 {
			t.Fatal("truncated event published part of its reports")
		}
		h.handleAdvertisement(packet)
		for i := range packet {
			packet[i] = 0
		}
		synctest.Wait()
		if len(reports) != 2 {
			t.Fatalf("discovery callbacks = %d, want 2", len(reports))
		}
		found := make(map[[6]byte]*PlatData, 2)
		for range 2 {
			pd := <-reports
			found[pd.Address] = pd
		}
		for _, want := range []struct {
			address [6]byte
			typ     uint8
			value   byte
			rssi    int8
		}{
			{[6]byte{6, 5, 4, 3, 2, 1}, 0, 'a', -60},
			{[6]byte{16, 15, 14, 13, 12, 11}, 1, 'b', -59},
		} {
			got := found[want.address]
			if got == nil || got.AddressType != want.typ || got.RSSI != want.rssi || got.Connectable || !bytes.Equal(got.Data, []byte{2, 9, want.value}) {
				t.Fatalf("report for %x = %+v", want.address, got)
			}
		}
	})
}
