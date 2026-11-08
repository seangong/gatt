package evt

import (
	"bytes"
	"strconv"
	"testing"
)

func TestLEAdvertisingReportUnmarshalTruncated(t *testing.T) {
	packet := []byte{LEAdvertisingReport, 1, 3, 0, 1, 2, 3, 4, 5, 6, 3, 2, 9, 'a', 0xc4}
	for length := 0; length < len(packet); length++ {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			defer func() {
				if got := recover(); got != nil {
					t.Errorf("Unmarshal(%x) panicked: %v", packet[:length], got)
				}
			}()
			var report LEAdvertisingReportEP
			if err := report.Unmarshal(packet[:length]); err == nil {
				t.Errorf("Unmarshal(%x) accepted a truncated report", packet[:length])
			}
		})
	}
}

func TestLEAdvertisingReportUnmarshalPreservesLayoutAndOwnership(t *testing.T) {
	// Preserve the existing grouped field layout for multiple reports.
	packet := []byte{LEAdvertisingReport, 2, 0, 3, 0, 1,
		1, 2, 3, 4, 5, 6, 6, 5, 4, 3, 2, 1,
		3, 0, 2, 9, 'a', 0xc4, 0xc5}
	var report LEAdvertisingReportEP
	if err := report.Unmarshal(packet); err != nil {
		t.Fatal(err)
	}
	if report.NumReports != 2 || report.EventType[0] != 0 || report.EventType[1] != 3 || report.AddressType[1] != 1 ||
		report.Address[0] != [6]byte{6, 5, 4, 3, 2, 1} || report.Address[1] != [6]byte{1, 2, 3, 4, 5, 6} ||
		!bytes.Equal(report.Data[0], []byte{2, 9, 'a'}) || len(report.Data[1]) != 0 || report.RSSI[0] != -60 || report.RSSI[1] != -59 {
		t.Fatalf("unexpected report: %+v", report)
	}
	packet[22] = 'x'
	if !bytes.Equal(report.Data[0], []byte{2, 9, 'a'}) {
		t.Fatalf("report retained input buffer: %x", report.Data[0])
	}
}

func FuzzLEAdvertisingReportUnmarshal(f *testing.F) {
	for _, seed := range [][]byte{nil, {LEAdvertisingReport, 0}, {LEAdvertisingReport, 1, 3, 0, 1, 2, 3, 4, 5, 6, 2, 1, 0x0a, 196}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var report LEAdvertisingReportEP
		_ = report.Unmarshal(data)
	})
}
