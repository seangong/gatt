package linux

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func scanDispatchTestBatch(count int) []byte {
	packet := []byte{byte(evt.LEAdvertisingReport), byte(count)}
	for i := 0; i < count; i++ {
		packet = append(packet, advNonconnInd, 0, byte(i+1), 0, 0, 0, 0, 0, 0, 0xc4)
	}
	return packet
}

func TestScanBatchWithDefaultConnectionLimit(t *testing.T) {
	for _, count := range []int{2, 25} {
		t.Run(fmt.Sprintf("%d reports", count), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := &HCI{
					connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{},
					plist: make(map[peerAddress]*advertisementEntry), maxConn: 1,
				}
				reports := make(chan *PlatData, count)
				h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
				h.handleAdvertisement(scanDispatchTestBatch(count))
				synctest.Wait()
				if len(reports) != count {
					t.Fatalf("default maxConn=1 delivered %d of %d scan reports", len(reports), count)
				}
				seen := make(map[byte]bool, count)
				for range count {
					pd := <-reports
					identity := pd.Address[5]
					if identity < 1 || int(identity) > count || seen[identity] {
						t.Fatalf("scan batch changed or duplicated identity: %+v", pd)
					}
					seen[identity] = true
				}
			})
		})
	}
}

func TestScanCallbacksDoNotUseConnectionCalloutSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := &HCI{
			connsmu: &sync.Mutex{}, plistmu: &sync.Mutex{},
			plist: make(map[peerAddress]*advertisementEntry), maxConn: 1,
		}
		h.initLifecycle()
		h.callbacks <- struct{}{}
		defer func() { <-h.callbacks }()
		reports := make(chan *PlatData, 2)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		h.handleAdvertisement(scanDispatchTestBatch(2))
		synctest.Wait()
		if len(reports) != 2 {
			t.Fatalf("connection callout exhaustion lost %d scan reports", 2-len(reports))
		}
	})
}

func TestScanCallbacksPreserveConfiguredParallelism(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, _ := newShutdownTestHCI(t, false)
		h.maxConn = 2
		first, second := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(second) })
		completed := make(chan byte, 2)
		h.AdvertisementHandler = func(pd *PlatData) {
			if pd.Address[5] == 1 {
				close(first)
				<-second
			} else {
				<-first
				once.Do(func() { close(second) })
			}
			completed <- pd.Address[5]
		}
		h.handleAdvertisement(scanDispatchTestBatch(2))
		synctest.Wait()
		if len(completed) != 2 {
			t.Errorf("maxConn=2 scan callbacks ran serially: %d finished", len(completed))
		}
	})
}

func TestScanCallbackCanCloseHCI(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.maxConn = 1
		result := make(chan error, 1)
		var calls atomic.Int32
		h.AdvertisementHandler = func(*PlatData) {
			calls.Add(1)
			result <- h.Close()
		}
		h.handleAdvertisement(scanDispatchTestBatch(2))
		synctest.Wait()
		if err := pendingTestResult(t, result); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if calls.Load() != 1 || d.closeCount.Load() != 1 {
			t.Fatalf("Close ran %d callbacks and closed the transport %d times", calls.Load(), d.closeCount.Load())
		}
	})
}

func TestScanQueueOverflowEndsSessionWithoutWaitingForCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		h.maxConn = 1
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		var calls atomic.Int32
		h.AdvertisementHandler = func(*PlatData) {
			calls.Add(1)
			close(entered)
			<-release
		}
		h.handleAdvertisement(scanDispatchTestBatch(2))
		<-entered
		command := make(chan error, 1)
		go func() { _, err := h.SendRawCommand(cmd.Reset{}); command <- err }()
		shutdownTestCommand(t, d, uint16(cmd.Reset{}.Opcode()))
		shutdownTestCommandComplete(d, uint16(cmd.Reset{}.Opcode()), 0)
		if err := pendingTestResult(t, command); err != nil {
			t.Fatalf("scan callback blocked HCI command delivery: %v", err)
		}
		for i := 0; i <= 16; i++ {
			h.handleAdvertisement(scanDispatchTestBatch(1))
		}
		synctest.Wait()
		select {
		case <-h.Done():
		default:
			t.Fatal("scan queue overflow did not terminate the HCI session")
		}
		if err := h.Close(); err == nil || !strings.Contains(err.Error(), "scan") {
			t.Fatalf("scan queue overflow lost its shutdown cause: %v", err)
		}
		if calls.Load() != 1 || d.closeCount.Load() != 1 {
			t.Fatal("shutdown waited for or retried the blocked scan callback")
		}
		once.Do(func() { close(release) })
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatal("shutdown delivered remaining reports from an active or queued batch")
		}
	})
}

func TestScanCloseRetainsOnlyAdmittedCallbackSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, _ := newShutdownTestHCI(t, false)
		h.maxConn = 2
		first, second := make(chan struct{}), make(chan struct{})
		closeNow, release := make(chan struct{}), make(chan struct{})
		var closeOnce, releaseOnce sync.Once
		defer closeOnce.Do(func() { close(closeNow) })
		defer releaseOnce.Do(func() { close(release) })
		result := make(chan error, 1)
		var calls atomic.Int32
		h.AdvertisementHandler = func(pd *PlatData) {
			calls.Add(1)
			if pd.Address[5] == 1 {
				close(first)
				<-closeNow
				result <- h.Close()
			} else if pd.Address[5] == 2 {
				close(second)
				<-release
			} else {
				t.Error("Close admitted a report that was still waiting for a scan slot")
			}
		}
		h.handleAdvertisement(scanDispatchTestBatch(3))
		<-first
		<-second
		closeOnce.Do(func() { close(closeNow) })
		synctest.Wait()
		if err := pendingTestResult(t, result); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 || len(h.scanDispatch.slots) != 1 {
			t.Fatalf("Close ran %d callbacks with %d held slots, want 2 callbacks/1 running slot", calls.Load(), len(h.scanDispatch.slots))
		}
		releaseOnce.Do(func() { close(release) })
		synctest.Wait()
		if calls.Load() != 2 || len(h.scanDispatch.slots) != 0 {
			t.Fatal("Close ran a waiting callback or retained a finished callback's slot")
		}
	})
}
