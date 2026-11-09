package gatt

import (
	"bytes"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux"
)

func TestDeviceDiscoveryPreservesServiceDataAnd32BitUUID(t *testing.T) {
	for _, tt := range []struct {
		name string
		typ  byte
		wire []byte
		want UUID
	}{
		{"uuid16", typeServiceData16, []byte{0xaa, 0xfe}, UUID16(0xfeaa)},
		{"uuid32", typeServiceData32, []byte{0x78, 0x56, 0x34, 0x12}, MustParseUUID("12345678-0000-1000-8000-00805f9b34fb")},
		{"uuid128", typeServiceData128, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00").b, MustParseUUID("11223344-5566-7788-99aa-bbccddeeff00")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := &device{hci: &linux.HCI{}, closeHCI: func() error { return nil }}
				defer d.Stop()
				result := make(chan *Advertisement, 1)
				d.peripheralDiscovered = func(_ Peripheral, a *Advertisement, _ int) { result <- a }
				if err := d.Init(nil); err != nil {
					t.Fatal(err)
				}
				data := []byte{5, typeAllUUID32, 0x78, 0x56, 0x34, 0x12}
				data = append(data, byte(len(tt.wire)+2), tt.typ)
				data = append(data, tt.wire...)
				data = append(data, 0x42, 2, typeCompleteName, 'n')
				d.hci.AdvertisementHandler(&linux.PlatData{Data: data, Connectable: true})
				synctest.Wait()
				select {
				case a := <-result:
					if a.LocalName != "n" || !a.Connectable || len(a.Services) != 1 || !a.Services[0].Equal(MustParseUUID("12345678-0000-1000-8000-00805f9b34fb")) {
						t.Errorf("discovery advertisement = %+v", a)
					}
					if len(a.ServiceData) != 1 || !a.ServiceData[0].UUID.Equal(tt.want) || !bytes.Equal(a.ServiceData[0].Data, []byte{0x42}) {
						t.Errorf("discovery ServiceData = %+v", a.ServiceData)
					}
				default:
					t.Fatal("no discovery callback")
				}
			})
		})
	}
}

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
