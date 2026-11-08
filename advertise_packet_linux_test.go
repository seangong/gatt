package gatt

import (
	"bytes"
	"errors"
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
