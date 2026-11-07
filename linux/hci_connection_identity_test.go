package linux

import (
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

func connectionIdentityReport(t *testing.T, h *HCI, reports <-chan *PlatData, address [6]byte) *PlatData {
	t.Helper()
	for _, kind := range []byte{advInd, scanRsp} {
		packet := append([]byte{byte(evt.LEAdvertisingReport), 1, kind, 0}, address[:]...)
		packet = append(packet, 1, 'a', 0)
		h.handleAdvertisement(packet)
	}
	synctest.Wait()
	select {
	case pd := <-reports:
		return pd
	default:
		t.Fatal("advertised peripheral was not reported")
		return nil
	}
}

func connectionIdentityConnect(t *testing.T, h *HCI, d *shutdownTestTransport, pd *PlatData, status byte) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- h.Connect(pd) }()
	opcode := uint16(cmd.LECreateConn{}.Opcode())
	shutdownTestCommand(t, d, opcode)
	d.input <- shutdownTestEvent(evt.CommandStatus, status, 1, byte(opcode), byte(opcode>>8))
	synctest.Wait()
	err := <-result
	if status == 0 && err != nil {
		t.Fatalf("Connect() = %v, want accepted attempt", err)
	}
	if status != 0 && err == nil {
		t.Fatal("Connect() ignored rejected Command Status")
	}
}

func connectionIdentityComplete(t *testing.T, d *shutdownTestTransport, accepted <-chan *PlatData, address [6]byte, handle uint16, role byte) *PlatData {
	t.Helper()
	packet := make([]byte, 19)
	packet[0] = byte(evt.LEConnectionComplete)
	packet[2], packet[3], packet[4], packet[12] = byte(handle), byte(handle>>8), role, 0x18
	copy(packet[6:12], address[:])
	d.input <- shutdownTestEvent(evt.LEMeta, packet...)
	opcode := uint16(cmd.LESetAdvertiseEnable{}.Opcode())
	shutdownTestCommand(t, d, opcode)
	shutdownTestCommandComplete(d, opcode, 0)
	select {
	case pd := <-accepted:
		return pd
	default:
		t.Fatal("successful connection did not reach the connection callback")
		return nil
	}
}

func connectionIdentityCancel(h *HCI, pd *PlatData) <-chan error {
	result := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("CancelConnection panicked: %v", recovered)
			}
			result <- err
		}()
		err = h.CancelConnection(pd)
	}()
	return result
}

func connectionIdentityDisconnect(t *testing.T, h *HCI, d *shutdownTestTransport, pd *PlatData, handle uint16) {
	t.Helper()
	result := connectionIdentityCancel(h, pd)
	synctest.Wait()
	select {
	case err := <-result:
		t.Fatalf("CancelConnection() finished before Disconnect was sent: %v", err)
	default:
	}
	opcode := uint16(cmd.Disconnect{}.Opcode())
	packet := shutdownTestCommand(t, d, opcode)
	if packet[4] != byte(handle) || packet[5] != byte(handle>>8) {
		t.Fatalf("Disconnect = %x, want handle 0x%04x", packet, handle)
	}
	shutdownTestStatus(d, opcode)
	select {
	case err := <-result:
		t.Fatalf("CancelConnection() finished before physical disconnection: %v", err)
	default:
	}
	shutdownTestDisconnected(d, handle)
	if err := <-result; err != nil {
		t.Fatalf("CancelConnection() after physical disconnection = %v", err)
	}
	opcode = uint16(cmd.LESetAdvertiseEnable{}.Opcode())
	shutdownTestCommand(t, d, opcode)
	shutdownTestCommandComplete(d, opcode, 0)
}

func connectionIdentityCancelRetired(t *testing.T, h *HCI, d *shutdownTestTransport, pd *PlatData) {
	t.Helper()
	result := connectionIdentityCancel(h, pd)
	synctest.Wait()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("CancelConnection() for a retired peripheral = %v", err)
		}
	default:
		t.Fatal("CancelConnection() for a retired peripheral remained blocked")
	}
	select {
	case packet := <-d.writes:
		t.Fatalf("retired peripheral sent a command against another connection: %x", packet)
	default:
	}
}

func TestHCIOriginalPeripheralConnectionIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		reports, accepted := make(chan *PlatData, 4), make(chan *PlatData, 2)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
		address := [6]byte{1, 2, 3, 4, 5, 6}
		original := connectionIdentityReport(t, h, reports, address)
		connectionIdentityConnect(t, h, d, original, 0)
		// A fresh advertisement replaces the cache before the connection completes.
		connectionIdentityReport(t, h, reports, address)
		connected := connectionIdentityComplete(t, d, accepted, address, 1, 0)
		connectionIdentityDisconnect(t, h, d, original, 1)

		// Reuse both the peer address and controller handle for a different scan
		// object. Neither object from the previous connection may close this link.
		replacement := connectionIdentityReport(t, h, reports, address)
		connectionIdentityConnect(t, h, d, replacement, 0)
		replacementConnected := connectionIdentityComplete(t, d, accepted, address, 1, 0)
		for i := 0; i < 2; i++ {
			connectionIdentityCancelRetired(t, h, d, original)
			connectionIdentityCancelRetired(t, h, d, connected)
		}
		connectionIdentityDisconnect(t, h, d, replacement, 1)
		connectionIdentityCancelRetired(t, h, d, replacement)
		connectionIdentityCancelRetired(t, h, d, replacementConnected)
	})
}

func TestHCIInboundCompletionPreservesPendingConnectionIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		reports, outbound, inbound := make(chan *PlatData, 2), make(chan *PlatData, 1), make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		h.AcceptSlaveHandler = func(pd *PlatData) { outbound <- pd }
		h.AcceptMasterHandler = func(pd *PlatData) { inbound <- pd }
		address := [6]byte{1, 2, 3, 4, 5, 6}
		original := connectionIdentityReport(t, h, reports, address)
		connectionIdentityConnect(t, h, d, original, 0)
		incoming := connectionIdentityComplete(t, d, inbound, [6]byte{6, 5, 4, 3, 2, 1}, 2, 1)

		// An inbound connection must not admit another outgoing attempt while
		// the first Create Connection is still waiting for its completion event.
		result := make(chan error, 1)
		go func() { result <- h.Connect(&PlatData{Address: [6]byte{9, 8, 7, 6, 5, 4}}) }()
		synctest.Wait()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("inbound completion admitted a second pending outgoing attempt")
			}
		default:
			t.Fatal("inbound completion released the pending outgoing connection gate")
		}
		connectionIdentityComplete(t, d, outbound, address, 1, 0)
		connectionIdentityDisconnect(t, h, d, original, 1)
		connectionIdentityDisconnect(t, h, d, incoming, 2)
	})
}

func TestHCIRejectedAttemptClearsConnectionIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		reports, accepted := make(chan *PlatData, 2), make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
		address := [6]byte{1, 2, 3, 4, 5, 6}
		rejected := connectionIdentityReport(t, h, reports, address)
		connectionIdentityConnect(t, h, d, rejected, 0x0c)

		retry := connectionIdentityReport(t, h, reports, address)
		connectionIdentityConnect(t, h, d, retry, 0)
		connectionIdentityComplete(t, d, accepted, address, 1, 0)
		connectionIdentityCancelRetired(t, h, d, rejected)
		connectionIdentityDisconnect(t, h, d, retry, 1)
	})
}

func TestHCIReusedConnectedPeripheralConnectionIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h, d := newShutdownTestHCI(t, false)
		reports, accepted := make(chan *PlatData, 1), make(chan *PlatData, 1)
		h.AdvertisementHandler = func(pd *PlatData) { reports <- pd }
		h.AcceptSlaveHandler = func(pd *PlatData) { accepted <- pd }
		address := [6]byte{1, 2, 3, 4, 5, 6}
		original := connectionIdentityReport(t, h, reports, address)
		connectionIdentityConnect(t, h, d, original, 0)
		connected := connectionIdentityComplete(t, d, accepted, address, 1, 0)
		connectionIdentityDisconnect(t, h, d, connected, 1)

		// Reusing a connected callback object for Connect must make it control
		// that new attempt rather than only its already closed Conn value.
		connectionIdentityConnect(t, h, d, connected, 0)
		reconnected := connectionIdentityComplete(t, d, accepted, address, 1, 0)
		connectionIdentityDisconnect(t, h, d, connected, 1)
		connectionIdentityCancelRetired(t, h, d, connected)
		connectionIdentityCancelRetired(t, h, d, reconnected)
	})
}
