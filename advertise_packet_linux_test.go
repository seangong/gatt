package gatt

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/seangong/gatt/v2/linux/cmd"
)

func TestAdvertiseRejectsPacketErrorBeforeDeviceChanges(t *testing.T) {
	a := (&AdvPacket{}).AppendName(string(bytes.Repeat([]byte{'n'}, 29))).AppendFlags(flagLEOnly)
	advData := &cmd.LESetAdvertisingData{AdvertisingDataLength: 3, AdvertisingData: [31]byte{2, typeFlags, 6}}
	scanResp := &cmd.LESetScanResponseData{ScanResponseDataLength: 4, ScanResponseData: [31]byte{3, typeCompleteName, 'o', 'k'}}
	advParam := &cmd.LESetAdvertisingParameters{AdvertisingChannelMap: 7}
	d := &device{advData: advData, scanResp: scanResp, advParam: advParam}
	// No HCI transport is present: reaching a controller operation would panic.
	if err := d.Advertise(a); !errors.Is(err, ErrEIRPacketTooLong) {
		t.Fatalf("Advertise error = %v, want ErrEIRPacketTooLong", err)
	}
	if d.advData != advData || d.scanResp != scanResp || d.advParam != advParam {
		t.Fatal("Advertise changed pending advertising configuration")
	}
}

func TestNameAndServicesClearsOldScanResponse(t *testing.T) {
	services := []UUID{MustParseUUID("12345678-1234-5678-1234-567812345678")}
	var scanData []byte
	for _, name := range []string{strings.Repeat("n", 27), "new"} {
		adv, scan := nameAndServicesPackets(name, services)
		if scan == nil {
			t.Fatal("replacement left the controller's previous scan response unchanged")
		}
		scanData = append(scanData[:0], scan.ScanResponseData[:scan.ScanResponseDataLength]...)
		var parsed Advertisement
		packet := adv.Bytes()
		merged := append(append([]byte(nil), packet[:adv.Len()]...), scanData...)
		if err := parsed.unmarshall(merged); err != nil {
			t.Fatal(err)
		}
		if parsed.LocalName != name || len(parsed.Services) != 1 || !parsed.Services[0].Equal(services[0]) {
			t.Fatalf("replacement advertisement = name %q services %v", parsed.LocalName, parsed.Services)
		}
	}
	if len(scanData) != 0 {
		t.Fatal("short name left old scan response data in the controller")
	}
}

func TestNameAndServicesScanResponseBounds(t *testing.T) {
	adv, scan := nameAndServicesPackets(strings.Repeat("n", 50), nil)
	if scan == nil || scan.ScanResponseDataLength != MaxEIRPacketLength {
		t.Fatalf("long name scan response = %+v", scan)
	}
	if scan.ScanResponseData[1] != typeShortName || adv.Err() != nil {
		t.Fatal("long name did not retain bounded shortened-name behavior")
	}
}
