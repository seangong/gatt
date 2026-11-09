package linux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func lruTestAddress(n int) [6]byte {
	return [6]byte{0xc0, 0, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

func newLRUTestHCI() *HCI {
	return &HCI{
		connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{},
		plist:                make(map[peerAddress]*advertisementEntry),
		AdvertisementHandler: func(*PlatData) {},
	}
}

func lruTestFill(h *HCI, start, count int) {
	for i := start; i < start+count; i++ {
		h.handleAdvertisement(cacheTestReport(advInd, 1, lruTestAddress(i), []byte{2, 9, 'n'}))
	}
}

func TestAdvertisementCacheBoundsDistinctAddresses(t *testing.T) {
	h := newLRUTestHCI()
	lruTestFill(h, 0, 10000)
	if got := len(h.plist); got != 4096 {
		t.Fatalf("cache retained %d addresses, want 4096", got)
	}
}

func TestAdvertisementCacheKeepsRecentAdvertisingAndScanResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newLRUTestHCI()
		reports := make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		lruTestFill(h, 0, advertisementCacheCapacity)
		// Both kinds of report refresh the peer. The untouched next peer is
		// evicted instead, and a response to that peer cannot recreate it.
		h.handleAdvertisement(cacheTestReport(advInd, 1, lruTestAddress(0), []byte{2, 9, 'a'}))
		h.handleAdvertisement(cacheTestReport(scanRsp, 1, lruTestAddress(1), []byte{2, 0xff, 'b'}))
		cacheTestReceive(t, reports)
		lruTestFill(h, advertisementCacheCapacity, 1)
		for i := 0; i < 3; i++ {
			_, found := h.plist[peerAddress{lruTestAddress(i), 1}]
			if found != (i != 2) {
				t.Errorf("peer %d retained = %t, want %t", i, found, i != 2)
			}
		}
		h.handleAdvertisement(cacheTestReport(scanRsp, 1, lruTestAddress(2), []byte{2, 9, 'x'}))
		synctest.Wait()
		if len(reports) != 0 || len(h.plist) != advertisementCacheCapacity {
			t.Fatal("an evicted peer's scan response recreated its advertising state")
		}
	})
}

func TestAdvertisementCacheRepeatedReportsDoNotDisplaceOtherPeers(t *testing.T) {
	h := newLRUTestHCI()
	lruTestFill(h, 0, advertisementCacheCapacity)
	for i := 0; i < advertisementCacheCapacity*2; i++ {
		h.handleAdvertisement(cacheTestReport(advInd, 1, lruTestAddress(0), []byte{2, 9, byte(i)}))
	}
	if len(h.plist) != advertisementCacheCapacity || h.plistLRU.Len() != advertisementCacheCapacity {
		t.Fatalf("repeated reports changed cache/list sizes to %d/%d", len(h.plist), h.plistLRU.Len())
	}
	lruTestFill(h, advertisementCacheCapacity, 1)
	if h.plist[peerAddress{lruTestAddress(0), 1}] == nil || h.plist[peerAddress{lruTestAddress(1), 1}] != nil {
		t.Fatal("repeated reports left a stale position in the cache")
	}
}

func TestAdvertisementCacheLRUDistinguishesAddressTypes(t *testing.T) {
	h := newLRUTestHCI()
	address := lruTestAddress(0)
	h.handleAdvertisement(cacheTestReport(advInd, 0, address, []byte{2, 9, 'p'}))
	lruTestFill(h, 0, advertisementCacheCapacity-1)
	// Refreshing the random address must not refresh its public namesake.
	h.handleAdvertisement(cacheTestReport(advInd, 1, address, []byte{2, 9, 'r'}))
	lruTestFill(h, advertisementCacheCapacity-1, 1)
	if h.plist[peerAddress{address, 0}] != nil || h.plist[peerAddress{address, 1}] == nil {
		t.Fatal("LRU positions confused public and random address namespaces")
	}
}

func TestEvictedPendingConnectionKeepsOriginalMetadataAndCancellation(t *testing.T) {
	for _, withResult := range []bool{false, true} {
		t.Run(fmt.Sprintf("result_%t", withResult), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				h.AdvertisementHandler = func(*PlatData) {}
				accepted := make(chan *PlatData, 1)
				h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
				lruTestFill(h, 0, advertisementCacheCapacity)
				originalData := []byte{2, 9, 'p', 2, 0xff, 'a'}
				source := &PlatData{
					Name: "Printer", Address: lruTestAddress(0), AddressType: 1,
					Data: append([]byte(nil), originalData...), RSSI: -57, Connectable: true,
					Conn: d, // A reused Connect argument must not publish its old transport.
				}
				var result <-chan ConnectionResult
				if withResult {
					result = resultTestStart(t, h, d, source, 0)
				} else {
					connectionIdentityConnect(t, h, d, source, 0)
				}
				lruTestFill(h, advertisementCacheCapacity, 1)
				if h.plist[peerAddress{source.Address, source.AddressType}] == nil || h.plist[peerAddress{lruTestAddress(1), 1}] != nil {
					t.Fatal("Connect did not refresh its peer's cached report")
				}
				lruTestFill(h, advertisementCacheCapacity+1, advertisementCacheCapacity)
				if h.plist[peerAddress{source.Address, source.AddressType}] != nil {
					t.Fatal("pending peer was not evicted by unrelated scan results")
				}
				source.Name, source.RSSI, source.Connectable = "changed", -1, false
				source.Data[0] = 0xff
				connectionNameComplete(d, source, 1, 0)
				var connected *PlatData
				if withResult {
					outcome := resultTestRead(t, result)
					if outcome.Err != nil {
						t.Fatal(outcome.Err)
					}
					connected = outcome.Peripheral
				} else {
					connected = <-accepted
				}
				if connected.Name != "Printer" || connected.Address != source.Address || connected.AddressType != 1 ||
					connected.RSSI != -57 || !connected.Connectable || !bytes.Equal(connected.Data, originalData) ||
					connected.Conn == nil || connected.Conn == source.Conn {
					t.Fatalf("connection lost admitted metadata after eviction: %+v", connected)
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

func TestEvictedPendingConnectionCanStillBeCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.AdvertisementHandler = func(*PlatData) {}
		source := &PlatData{Address: lruTestAddress(0), AddressType: 1}
		result := resultTestStart(t, h, d, source, 0)
		lruTestFill(h, 1, advertisementCacheCapacity)
		if h.plist[peerAddress{source.Address, source.AddressType}] != nil {
			t.Fatal("pending peer was not evicted")
		}
		canceled := connectionIdentityCancel(h, source)
		opcode := uint16(cmd.LECreateConnCancel{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		pendingTestTerminal(d, 0x02)
		if err := pendingTestResult(t, canceled); err != nil {
			t.Fatal(err)
		}
		if outcome := resultTestRead(t, result); outcome.Peripheral != source || !errors.Is(outcome.Err, context.Canceled) {
			t.Fatalf("eviction changed original cancellation identity: %+v", outcome)
		}
	})
}

func TestLiveConnectionSurvivesCacheEviction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.AdvertisementHandler = func(*PlatData) {}
		source := &PlatData{Name: "Printer", Address: lruTestAddress(0), AddressType: 1, Data: []byte{2, 9, 'p'}}
		result := resultTestStart(t, h, d, source, 0)
		connectionNameComplete(d, source, 1, 0)
		outcome := resultTestRead(t, result)
		if outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
		connected := outcome.Peripheral
		lruTestFill(h, 1, advertisementCacheCapacity)
		if h.plist[peerAddress{source.Address, source.AddressType}] != nil {
			t.Fatal("connected peer was not evicted")
		}
		select {
		case <-connected.Conn.(*conn).Context().Done():
			t.Fatal("cache eviction closed an established connection")
		default:
		}
		if connected.Name != "Printer" || !bytes.Equal(connected.Data, []byte{2, 9, 'p'}) {
			t.Fatal("cache eviction changed an established connection's report")
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

func TestStopScanningRetainsCacheUntilHCIClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.AdvertisementHandler = func(*PlatData) {}
		lruTestFill(h, 0, 10)
		stopped := make(chan error, 1)
		go func() { stopped <- h.SetScanEnable(false, true) }()
		opcode := uint16(cmd.LESetScanEnable{}.Opcode())
		shutdownTestCommand(t, d, opcode)
		shutdownTestCommandComplete(d, opcode, 0)
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		if len(h.plist) != 10 {
			t.Fatal("stopping a scan discarded connection metadata")
		}
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		if len(h.plist) != 0 || h.plistLRU.Len() != 0 {
			t.Fatal("HCI close retained scan cache entries")
		}
	})
}

func TestAdmittedConnectDoesNotRefillCacheDuringShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		release := make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		if !h.startWorker(func() { <-release }) {
			t.Fatal("protocol worker was not started")
		}
		// Block the admitted caller before cache staging. A running protocol
		// worker keeps final cache cleanup from masking an accidental refill.
		h.plistmu.Lock()
		locked := true
		defer func() {
			if locked {
				h.plistmu.Unlock()
			}
		}()
		source := &PlatData{Address: lruTestAddress(0), AddressType: 1}
		ready := make(chan (<-chan ConnectionResult), 1)
		go func() { ready <- h.ConnectWithResult(source) }()
		admitted := false
		for i := 0; i < 10000; i++ {
			h.connsmu.Lock()
			admitted = h.pending != nil
			h.connsmu.Unlock()
			if admitted {
				break
			}
			runtime.Gosched()
		}
		if !admitted {
			t.Fatal("Connect was not admitted before shutdown")
		}
		cause := errors.New("controller unavailable before cache staging")
		h.requestClose(cause)
		<-d.closed
		h.plistmu.Unlock()
		locked = false
		result := <-ready
		outcome := <-result
		if outcome.Peripheral != source || !errors.Is(outcome.Err, cause) {
			t.Fatalf("admitted connection lost its terminal cause: %+v", outcome)
		}
		if len(h.plist) != 0 || h.plistLRU.Len() != 0 {
			t.Fatal("admitted Connect refilled the cache after shutdown started")
		}
		releaseOnce.Do(func() { close(release) })
		if err := h.Close(); !errors.Is(err, cause) {
			t.Fatalf("Close lost the original failure: %v", err)
		}
	})
}
