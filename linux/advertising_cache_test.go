package linux

import (
	"bytes"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func cacheTestReport(kind, addressType byte, address [6]byte, data []byte) []byte {
	packet := []byte{byte(evt.LEAdvertisingReport), 1, kind, addressType}
	for i := range address {
		packet = append(packet, address[5-i])
	}
	packet = append(packet, byte(len(data)))
	packet = append(packet, data...)
	return append(packet, 0xc4)
}

func cacheTestReceive(t *testing.T, reports <-chan *PlatData) *PlatData {
	t.Helper()
	synctest.Wait()
	select {
	case pd := <-reports:
		return pd
	default:
		t.Fatal("advertisement was not published")
		return nil
	}
}

func TestScanResponseReplacesPreviousData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[peerAddress]*advertisementEntry)}
		reports := make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		address := [6]byte{0xc0, 5, 4, 3, 2, 1}
		adv := []byte{2, 9, 'a'}
		h.handleAdvertisement(cacheTestReport(advInd, 0, address, adv))
		var first *PlatData
		for i := 0; i < 100; i++ {
			response := []byte{4, 0x16, 0xaa, 0xfe, byte(i)}
			h.handleAdvertisement(cacheTestReport(scanRsp, 0, address, response))
			pd := cacheTestReceive(t, reports)
			want := append(append([]byte(nil), adv...), response...)
			if !bytes.Equal(pd.Data, want) {
				t.Errorf("scan response %d = %x, want only latest ADV and SCAN_RSP %x", i, pd.Data, want)
				break
			}
			if first == nil {
				first = pd
			}
		}
		if first != nil && !bytes.Equal(first.Data, []byte{2, 9, 'a', 4, 0x16, 0xaa, 0xfe, 0}) {
			t.Error("later responses changed a previously published snapshot")
		}
		h.handleAdvertisement(cacheTestReport(scanRsp, 0, address, nil))
		if pd := cacheTestReceive(t, reports); !bytes.Equal(pd.Data, adv) {
			t.Errorf("empty response retained previous response: %x", pd.Data)
		}
		newAdv := []byte{2, 9, 'z'}
		h.handleAdvertisement(cacheTestReport(advInd, 0, address, newAdv))
		h.handleAdvertisement(cacheTestReport(scanRsp, 0, address, []byte{2, 0xff, 0x42}))
		if pd := cacheTestReceive(t, reports); !bytes.Equal(pd.Data, []byte{2, 9, 'z', 2, 0xff, 0x42}) {
			t.Errorf("new ADV retained previous advertising data: %x", pd.Data)
		}
	})
}

func TestAdvertisementCacheSeparatesAddressTypes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[peerAddress]*advertisementEntry)}
		reports := make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		address := [6]byte{0xc0, 5, 4, 3, 2, 1}
		h.handleAdvertisement(cacheTestReport(advInd, 0, address, []byte{2, 9, 'p'}))
		h.handleAdvertisement(cacheTestReport(advInd, 1, address, []byte{2, 9, 'r'}))
		for _, addressType := range []byte{0, 1, 0, 1} {
			h.handleAdvertisement(cacheTestReport(scanRsp, addressType, address, []byte{2, 0xff, addressType}))
			pd := cacheTestReceive(t, reports)
			wantName := byte('p')
			if addressType == 1 {
				wantName = 'r'
			}
			if pd.Address != address || pd.AddressType != addressType || !bytes.Equal(pd.Data, []byte{2, 9, wantName, 2, 0xff, addressType}) {
				t.Errorf("type %d response borrowed another identity: %+v", addressType, pd)
			}
		}
	})
}

func TestConnectionCachePreservesAdvertisingBoundaryAndIdentity(t *testing.T) {
	for _, withResult := range []bool{false, true} {
		name := "legacy"
		if withResult {
			name = "result"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				reports, accepted := make(chan *PlatData, 1), make(chan *PlatData, 1)
				h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
				h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
				address := [6]byte{0xc0, 5, 4, 3, 2, 1}
				adv := []byte{2, 9, 'p'}
				h.handleAdvertisement(cacheTestReport(advInd, 0, address, adv))
				h.handleAdvertisement(cacheTestReport(scanRsp, 0, address, []byte{2, 0xff, 'a'}))
				source := cacheTestReceive(t, reports)
				source.Name = "Public"
				var result <-chan ConnectionResult
				if withResult {
					result = resultTestStart(t, h, d, source, 0)
				} else {
					connectionIdentityConnect(t, h, d, source, 0)
				}
				// Connect receives an already merged Data snapshot. A later
				// scan response must replace its response portion, not append.
				h.handleAdvertisement(cacheTestReport(scanRsp, 0, address, []byte{2, 0xff, 'b'}))
				latest := cacheTestReceive(t, reports)
				want := []byte{2, 9, 'p', 2, 0xff, 'b'}
				if !bytes.Equal(latest.Data, want) {
					t.Errorf("response after Connect = %x, want %x", latest.Data, want)
				}
				// The other address namespace must not replace the target's
				// connection metadata while its create command is pending.
				h.handleAdvertisement(cacheTestReport(advInd, 1, address, []byte{2, 9, 'r'}))
				h.handleAdvertisement(cacheTestReport(scanRsp, 1, address, []byte{2, 0xff, 'x'}))
				cacheTestReceive(t, reports)
				connectionNameComplete(d, source, 1, 0)
				var connected *PlatData
				if withResult {
					outcome := resultTestRead(t, result)
					if outcome.Err != nil {
						t.Fatal(outcome.Err)
					}
					connected = outcome.Peripheral
				} else {
					select {
					case connected = <-accepted:
					default:
						t.Fatal("legacy connection callback missing")
					}
				}
				if connected.AddressType != 0 || connected.Name != "Public" || !bytes.Equal(connected.Data, want) {
					t.Errorf("connected metadata borrowed another identity: %+v", connected)
				}
				connected.Data[0] = 0xff
				h.plistmu.Lock()
				cachedData := append([]byte(nil), h.plist[peerAddress{address, 0}].data.Data...)
				h.plistmu.Unlock()
				if !bytes.Equal(cachedData, want) {
					t.Errorf("connection callback changed the cached snapshot: %x", cachedData)
				}
				h.handleAdvertisement(cacheTestReport(scanRsp, 0, address, []byte{2, 0xff, 'c'}))
				if pd := cacheTestReceive(t, reports); !bytes.Equal(pd.Data, []byte{2, 9, 'p', 2, 0xff, 'c'}) {
					t.Errorf("connection callback modified cached data: %x", pd.Data)
				}
				if !bytes.Equal(source.Data, []byte{2, 9, 'p', 2, 0xff, 'a'}) {
					t.Error("cache updates changed the original Connect argument")
				}
				canceled := connectionIdentityCancel(h, source)
				opcode := uint16(cmd.Disconnect{}.Opcode())
				shutdownTestCommand(t, d, opcode)
				shutdownTestStatus(d, opcode)
				shutdownTestDisconnected(d, 1)
				if err := pendingTestResult(t, canceled); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestConnectSeedDoesNotStandInForAdvertisingPacket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		reports := make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		source := &PlatData{Name: "Known", AddressType: 1, Address: [6]byte{0xc0, 5, 4, 3, 2, 1}, Data: []byte{2, 9, 'a', 2, 0xff, 'b'}}
		result := resultTestStart(t, h, d, source, 0)
		h.handleAdvertisement(cacheTestReport(scanRsp, 1, source.Address, []byte{2, 0xff, 'c'}))
		synctest.Wait()
		select {
		case pd := <-reports:
			t.Errorf("Connect metadata acted as a received ADV: %+v", pd)
		default:
		}
		// An actual empty ADV is distinct from not having observed an ADV.
		h.handleAdvertisement(cacheTestReport(advInd, 1, source.Address, nil))
		h.handleAdvertisement(cacheTestReport(scanRsp, 1, source.Address, []byte{2, 0xff, 'd'}))
		if pd := cacheTestReceive(t, reports); !bytes.Equal(pd.Data, []byte{2, 0xff, 'd'}) {
			t.Errorf("actual empty ADV did not replace seeded metadata: %x", pd.Data)
		}
		connectionNameComplete(d, source, 1, 0)
		got := resultTestRead(t, result)
		if got.Err != nil || got.Peripheral.Name != "Known" || got.Peripheral.AddressType != 1 || !bytes.Equal(got.Peripheral.Data, []byte{2, 0xff, 'd'}) {
			t.Errorf("connected metadata = %+v", got)
		}
		if !bytes.Equal(source.Data, []byte{2, 9, 'a', 2, 0xff, 'b'}) {
			t.Error("updates changed the source metadata")
		}
		canceled := connectionIdentityCancel(h, source)
		opcode := uint16(cmd.Disconnect{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestStatus(d, opcode)
		shutdownTestDisconnected(d, 1)
		if err := pendingTestResult(t, canceled); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConnectionWithoutCacheKeepsEventAddressType(t *testing.T) {
	for _, role := range []byte{0, 1} {
		name := "outbound"
		if role == 1 {
			name = "inbound"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				accepted := make(chan *PlatData, 1)
				h.AcceptMasterHandler = func(pd *PlatData) { accepted <- pd }
				h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
				peer := &PlatData{Address: [6]byte{0xc0, 5, 4, 3, 2, 1}, AddressType: 1}
				connectionNameComplete(d, peer, 1, role)
				select {
				case pd := <-accepted:
					if pd.Address != peer.Address || pd.AddressType != peer.AddressType {
						t.Errorf("event address lost without cached metadata: %+v", pd)
					}
				default:
					t.Fatal("connection callback missing")
				}
				shutdownTestDisconnected(d, 1)
			})
		})
	}
}

func TestScanResponseReplacementAtLegacyPayloadLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &HCI{connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{}, plist: make(map[peerAddress]*advertisementEntry)}
		reports := make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		address := [6]byte{0xc0, 5, 4, 3, 2, 1}
		adv := append([]byte{30, 0xff}, bytes.Repeat([]byte{'a'}, 29)...)
		packet := cacheTestReport(advInd, 0, address, adv)
		h.handleAdvertisement(packet)
		for i := range packet {
			packet[i] = 0
		}
		for i := 0; i < 3; i++ {
			response := append([]byte{30, 0xff}, bytes.Repeat([]byte{byte(i)}, 29)...)
			packet = cacheTestReport(scanRsp, 0, address, response)
			h.handleAdvertisement(packet)
			pd := cacheTestReceive(t, reports)
			for i := range packet {
				packet[i] = 0
			}
			want := append(append([]byte(nil), adv...), response...)
			if len(pd.Data) != 62 || !bytes.Equal(pd.Data, want) {
				t.Errorf("maximum-length response %d = %x, want latest 31+31 bytes %x", i, pd.Data, want)
			}
		}
	})
}
