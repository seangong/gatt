package linux

import (
	"bytes"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func connectionNameComplete(d *shutdownTestTransport, source *PlatData, handle uint16, role byte) {
	packet := make([]byte, 19)
	packet[0], packet[2], packet[3] = byte(evt.LEConnectionComplete), byte(handle), byte(handle>>8)
	packet[4], packet[5], packet[12] = role, source.AddressType, 0x18
	for i := range source.Address {
		packet[6+i] = source.Address[5-i]
	}
	d.input <- shutdownTestEvent(evt.LEMeta, packet...)
	synctest.Wait()
}

func connectionNameScan(t *testing.T, h *HCI, reports <-chan *PlatData, address [6]byte, data []byte, rssi byte) *PlatData {
	t.Helper()
	for _, kind := range []byte{advInd, scanRsp} {
		payload := data
		if kind == scanRsp {
			payload = nil
		}
		packet := []byte{byte(evt.LEAdvertisingReport), 1, kind, 1}
		for i := range address {
			packet = append(packet, address[5-i])
		}
		packet = append(packet, byte(len(payload)))
		packet = append(packet, payload...)
		packet = append(packet, rssi)
		h.handleAdvertisement(packet)
	}
	synctest.Wait()
	select {
	case pd := <-reports:
		return pd
	default:
		t.Fatal("scanning did not publish its owned report")
		return nil
	}
}

func TestOutgoingConnectionNameSurvivesContinuousScanning(t *testing.T) {
	for _, withResult := range []bool{false, true} {
		name := "legacy"
		if withResult {
			name = "result"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				reports, accepted := make(chan *PlatData, 2), make(chan *PlatData, 1)
				h.AdvertisementHandler = func(pd *PlatData) {
					// The adapter parses the name on the callback's owned snapshot.
					pd.Name = string(pd.Data[2 : 1+int(pd.Data[0])])
					reports <- pd
				}
				h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
				address := [6]byte{1, 2, 3, 4, 5, 6}
				initialData := []byte{8, 9, 'P', 'r', 'i', 'n', 't', 'e', 'r'}
				source := connectionNameScan(t, h, reports, address, initialData, 0xb0)
				var result <-chan ConnectionResult
				if withResult {
					result = resultTestStart(t, h, d, source, 0)
				} else {
					connectionIdentityConnect(t, h, d, source, 0)
				}
				// The attempt must retain the name accepted at admission, rather
				// than reading the caller's mutable object again at completion.
				source.Name = "caller-owned"
				latestData := append(append([]byte(nil), initialData...), 2, 0xff, 0x42)
				latest := connectionNameScan(t, h, reports, address, latestData, 0xd0)
				latest.Name = "scan-owned"
				latest.Data[len(latest.Data)-1] = 0x99
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
						t.Fatal("successful connection did not reach the legacy handler")
					}
				}
				if connected.Name != "Printer" || connected.AddressType != 1 || connected.RSSI != -48 || !connected.Connectable || !bytes.Equal(connected.Data, latestData) {
					t.Fatalf("connected metadata = %+v", connected)
				}
				h.plistmu.Lock()
				cached := h.plist[peerAddress{address, source.AddressType}].data
				h.plistmu.Unlock()
				if cached.Name != "" || !bytes.Equal(cached.Data, latestData) || source.Name != "caller-owned" || source.Conn != nil || !bytes.Equal(source.Data, initialData) || latest.Name != "scan-owned" {
					t.Fatal("connection completion changed a cache or published scan snapshot")
				}
				canceled := connectionIdentityCancel(h, source)
				op := uint16(cmd.Disconnect{}.Opcode())
				shutdownTestCommand(t, d, op)
				shutdownTestStatus(d, op)
				shutdownTestDisconnected(d, 1)
				if err := pendingTestResult(t, canceled); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestOutgoingConnectionNameFallbackKeepsLatestNonemptyName(t *testing.T) {
	for _, tt := range []struct{ known, latest, want string }{
		{"Printer", "Updated", "Updated"},
		{"", "Updated", "Updated"},
		{"", "", ""},
	} {
		t.Run(tt.known+"/"+tt.latest, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h, d := newShutdownTestHCI(t, false)
				source := &PlatData{Name: tt.known, Address: [6]byte{1}}
				result := resultTestStart(t, h, d, source, 0)
				h.plistmu.Lock()
				h.advertisementLocked(peerAddress{source.Address, source.AddressType}, true).data = PlatData{Name: tt.latest, Address: source.Address, AddressType: source.AddressType}
				h.plistmu.Unlock()
				connectionNameComplete(d, source, 1, 0)
				got := resultTestRead(t, result)
				if got.Err != nil || got.Peripheral.Name != tt.want || source.Name != tt.known {
					t.Fatalf("connection name = %+v, want %q", got, tt.want)
				}
			})
		})
	}
}

func TestInboundSameAddressDoesNotBorrowPendingOutgoingName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		source := &PlatData{Name: "Outgoing", Address: [6]byte{1}}
		result := resultTestStart(t, h, d, source, 0)
		accepted := make(chan *PlatData, 1)
		h.AcceptMasterHandler = func(pd *PlatData) { accepted <- pd }
		connectionNameComplete(d, source, 2, 1)
		if incoming := <-accepted; incoming.Name != "" {
			t.Fatalf("inbound connection borrowed pending name %q", incoming.Name)
		}
		select {
		case outcome := <-result:
			t.Fatalf("inbound connection completed outgoing attempt: %+v", outcome)
		default:
		}
		other := &PlatData{Name: "Other", Address: source.Address}
		if busy := resultTestRead(t, h.ConnectWithResult(other)); busy.Err == nil {
			t.Fatal("inbound connection released the pending outgoing attempt")
		}
		h.plistmu.Lock()
		h.advertisementLocked(peerAddress{source.Address, source.AddressType}, true).data = PlatData{Address: source.Address, AddressType: source.AddressType}
		h.plistmu.Unlock()
		connectionNameComplete(d, source, 1, 0)
		if got := resultTestRead(t, result); got.Err != nil || got.Peripheral.Name != "Outgoing" {
			t.Fatalf("original outgoing name after a repeated connect = %+v", got)
		}
	})
}

func TestReconnectionCapturesEachAttemptsName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		var previous []*PlatData
		names := []string{"First", "Second", ""}
		for _, name := range names {
			source := &PlatData{Name: name, Address: [6]byte{1}}
			result := resultTestStart(t, h, d, source, 0)
			h.plistmu.Lock()
			h.advertisementLocked(peerAddress{source.Address, source.AddressType}, true).data = PlatData{Address: source.Address, AddressType: source.AddressType}
			h.plistmu.Unlock()
			connectionNameComplete(d, source, 1, 0)
			got := resultTestRead(t, result)
			if got.Err != nil || got.Peripheral.Name != name {
				t.Fatalf("reconnected name = %+v, want %q", got, name)
			}
			previous = append(previous, got.Peripheral)
			shutdownTestDisconnected(d, 1)
		}
		for i, pd := range previous {
			if pd.Name != names[i] {
				t.Fatal("reconnection changed an earlier connection's name")
			}
		}
	})
}
