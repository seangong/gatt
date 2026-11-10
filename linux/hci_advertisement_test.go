package linux

import (
	"bytes"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestAdvertisementCalloutOwnsItsReport(t *testing.T) {
	received := make(chan *PlatData, 1)
	release, returned := make(chan struct{}), make(chan struct{})
	var returnedOnce sync.Once
	h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[peerAddress]*advertisementEntry)}
	h.AdvertisementHandler = func(pd *PlatData) { received <- pd; <-release; returnedOnce.Do(func() { close(returned) }) }
	defer func() {
		close(release)
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Error("advertisement callback did not exit")
		}
	}()
	report := func(kind byte, value byte) []byte {
		return []byte{2, 1, kind, 0, 1, 2, 3, 4, 5, 6, 3, 2, 9, value, 0}
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
	if !bytes.Equal(pd.Data, []byte{2, 9, 'a', 2, 9, 'b'}) {
		t.Fatalf("callback report changed: %q", pd.Data)
	}
	pd.Name = "parsed name"
	pd.Data[0] = 'x'
	cached := h.plist[peerAddress{pd.Address, pd.AddressType}].data
	if !bytes.Equal(cached.Data, []byte{2, 9, 'a', 2, 9, 'c'}) || cached.Name != "" {
		t.Fatalf("callback changed cached report: %+v", cached)
	}
}

func TestAdvertisementPayloadBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name string
		adv  []byte
		rsp  []byte
		want []byte
	}{
		{"advertising_padding", []byte{2, 9, 'a', 0, 0}, []byte{2, 9, 'b'}, []byte{2, 9, 'a', 2, 9, 'b'}},
		{"advertising_bad_tail", []byte{2, 9, 'a', 5, 9}, []byte{2, 9, 'b'}, []byte{2, 9, 'a', 2, 9, 'b'}},
		{"response_bad_tail", []byte{2, 9, 'a'}, []byte{2, 9, 'b', 0xff}, []byte{2, 9, 'a', 2, 9, 'b'}},
		{"empty_advertisement", []byte{0xff}, []byte{2, 9, 'b'}, []byte{2, 9, 'b'}},
		{"empty_response", []byte{2, 9, 'a'}, []byte{0xff}, []byte{2, 9, 'a'}},
		{"unknown_type", []byte{2, 0xee, 1}, []byte{2, 9, 'b'}, []byte{2, 0xee, 1, 2, 9, 'b'}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[peerAddress]*advertisementEntry)}
				reports := make(chan *PlatData, 2)
				h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
				report := func(kind byte, data []byte) []byte {
					packet := append([]byte{2, 1, kind, 0, 1, 2, 3, 4, 5, 6, byte(len(data))}, data...)
					return append(packet, 0xc4)
				}
				h.handleAdvertisement(report(advInd, tt.adv))
				synctest.Wait()
				select {
				case <-reports:
					t.Fatal("scannable advertisement was reported before its scan response")
				default:
				}
				h.handleAdvertisement(report(scanRsp, tt.rsp))
				synctest.Wait()
				select {
				case pd := <-reports:
					if !bytes.Equal(pd.Data, tt.want) || !pd.Connectable || pd.RSSI != -60 || pd.Address != [6]byte{6, 5, 4, 3, 2, 1} {
						t.Fatalf("merged report = %+v, want data %x and original metadata", pd, tt.want)
					}
				default:
					t.Fatal("scan response did not produce a report")
				}
			})
		})
	}
}

func TestAdvertisementCallbackTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[peerAddress]*advertisementEntry)}
		reports := make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		report := func(kind byte) []byte {
			return []byte{2, 1, kind, 0, 1, 2, 3, 4, 5, 6, 3, 2, 9, 'n', 0xc4}
		}
		h.handleAdvertisement(report(scanRsp))
		synctest.Wait()
		select {
		case <-reports:
			t.Fatal("orphan scan response produced a callback")
		default:
		}
		h.handleAdvertisement(report(advNonconnInd))
		synctest.Wait()
		select {
		case pd := <-reports:
			if pd.Connectable || !bytes.Equal(pd.Data, []byte{2, 9, 'n'}) {
				t.Fatalf("non-connectable advertisement = %+v", pd)
			}
		default:
			t.Fatal("non-scannable advertisement did not produce a callback")
		}
	})
}

func TestAdvertisementRejectsTruncatedReports(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[peerAddress]*advertisementEntry)}
		reports := make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		packet := []byte{2, 1, advNonconnInd, 0, 1, 2, 3, 4, 5, 6, 3, 2, 9, 'n', 0xc4}
		for length := 0; length < len(packet); length++ {
			h.handleAdvertisement(packet[:length])
		}
		synctest.Wait()
		select {
		case <-reports:
			t.Fatal("truncated report produced a callback")
		default:
		}
		if len(h.plist) != 0 {
			t.Fatalf("truncated report changed cache: %+v", h.plist)
		}
	})
}
