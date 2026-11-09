package gatt

import (
	"bytes"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux"
	"github.com/seangong/gatt/v2/linux/evt"
)

func TestDeviceCentralAddressUsesDecodedPeerOrder(t *testing.T) {
	for _, tt := range []struct {
		name        string
		addressType byte
		wire        [6]byte
		want        string
	}{
		{"public", 0, [6]byte{0x66, 0x55, 0x44, 0x33, 0x22, 0x11}, "11:22:33:44:55:66"},
		{"random", 1, [6]byte{0xaf, 0x0a, 0, 0x12, 0xab, 0xc0}, "c0:ab:12:00:0a:af"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn := newLifecycleTestConn()
				d := &device{hci: &linux.HCI{}, closeHCI: conn.Close}
				defer d.Stop()
				connected, requested, disconnected := make(chan Central, 1), make(chan Central, 1), make(chan Central, 1)
				d.centralConnected = func(c Central) { connected <- c }
				d.centralDisconnected = func(c Central) { disconnected <- c }
				s := NewService(UUID16(0xff00))
				ch := s.AddCharacteristic(UUID16(0xff01))
				ch.HandleReadFunc(func(w ResponseWriter, r *ReadRequest) {
					requested <- r.Central
					_, _ = w.Write([]byte("ready"))
				})
				if err := d.AddService(s); err != nil {
					t.Fatal(err)
				}
				if err := d.Init(nil); err != nil {
					t.Fatal(err)
				}
				packet := make([]byte, 19)
				packet[0], packet[4], packet[5] = byte(evt.LEConnectionComplete), 1, tt.addressType
				copy(packet[6:12], tt.wire[:])
				var event evt.LEConnectionCompleteEP
				if err := event.Unmarshal(packet); err != nil {
					t.Fatal(err)
				}
				pd := &linux.PlatData{Address: event.PeerAddress, AddressType: event.PeerAddressType, Conn: conn}
				d.hci.AcceptMasterHandler(pd)
				pd.Address = [6]byte{} // The connection must own its address value.
				synctest.Wait()
				central := <-connected
				if got := central.ID(); got != tt.want {
					t.Errorf("Central.ID() = %q, want decoded peer %q", got, tt.want)
				}
				conn.reads <- []byte{attOpReadReq, byte(ch.VHandle()), byte(ch.VHandle() >> 8)}
				synctest.Wait()
				if got := <-requested; got != central || got.ID() != tt.want {
					t.Errorf("read request central = %p %q, want %p %q", got, got.ID(), central, tt.want)
				}
				if got := <-conn.writes; !bytes.Equal(got, append([]byte{attOpReadRsp}, []byte("ready")...)) {
					t.Fatalf("read response = %x", got)
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if got := <-disconnected; got != central || got.ID() != tt.want {
					t.Errorf("disconnected central = %p %q, want %p %q", got, got.ID(), central, tt.want)
				}
			})
		})
	}
}
