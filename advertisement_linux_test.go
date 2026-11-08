package gatt

import (
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux"
)

func TestDeviceDiscoveryPreservesValidFields(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		defer d.Stop()
		var calls int
		d.peripheralDiscovered = func(p Peripheral, a *Advertisement, rssi int) {
			calls++
			if p.Name() != "ok" || a.LocalName != "ok" || !a.Connectable || rssi != -45 {
				t.Errorf("discovery = name %q, advertisement %+v, RSSI %d", p.Name(), a, rssi)
			}
			if len(a.Services) != 1 || !a.Services[0].Equal(UUID16(0x180d)) {
				t.Errorf("services = %v, want [180d]", a.Services)
			}
		}
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		d.hci.AdvertisementHandler(&linux.PlatData{
			Data: []byte{
				3, typeCompleteName, 'o', 'k',
				1, typeTxPower,
				2, typeSomeUUID16, 0xff,
				3, typeAllUUID16, 0x0d, 0x18,
			},
			Connectable: true, RSSI: -45,
		})
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("discovery calls = %d, want 1", calls)
		}
	})
}

func TestDeviceDiscoveryDropsStructurallyInvalidReport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
		defer d.Stop()
		var calls int
		d.peripheralDiscovered = func(Peripheral, *Advertisement, int) { calls++ }
		if err := d.Init(nil); err != nil {
			t.Fatal(err)
		}
		d.hci.AdvertisementHandler(&linux.PlatData{
			Data: []byte{3, typeCompleteName, 'o', 'k', 0xff, typeCompleteName},
		})
		synctest.Wait()
		if calls != 0 {
			t.Fatalf("discovery calls = %d for structurally invalid report", calls)
		}
	})
}
