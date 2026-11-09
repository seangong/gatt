package gatt

import (
	"fmt"
	"testing"
)

func TestPeripheralDiscoveryConcurrentMetadataGetters(t *testing.T) {
	conn := newPeripheralTestConn()
	p := newPeripheralForTest(conn)
	startPeripheralForTest(t, p, conn)
	s := &Service{h: 1, endh: 5}
	stopReading := readMetadataForTest(t, s)
	defer stopReading()
	var original *Characteristic
	var cccd *Descriptor
	for round := 0; round < 2; round++ {
		var c *Characteristic
		err := runDiscoveryForTest(t, conn, func() error {
			chars, err := p.DiscoverCharacteristics(nil, s)
			if err != nil {
				return err
			}
			if len(chars) != 1 {
				return fmt.Errorf("characteristics = %d, want 1", len(chars))
			}
			c = chars[0]
			return nil
		}, discoveryExchange{characteristicDiscoveryRequest(1, 5), []byte{attOpReadByTypeRsp, 7, 2, 0, byte(CharNotify), 3, 0, 0x19, 0x2a}},
			discoveryExchange{characteristicDiscoveryRequest(3, 5), attErrorRsp(attOpReadByTypeReq, 3, attEcodeAttrNotFound)})
		if err != nil {
			t.Fatal(err)
		}
		err = runDiscoveryForTest(t, conn, func() error {
			descs, err := p.DiscoverDescriptors(nil, c)
			if err != nil {
				return err
			}
			if len(descs) != 1 || c.Descriptor() != descs[0] {
				return fmt.Errorf("descriptor collection does not contain its CCCD")
			}
			return nil
		}, discoveryExchange{descriptorDiscoveryRequest(4, 5), []byte{attOpFindInfoRsp, 1, 4, 0, 2, 0x29}},
			discoveryExchange{descriptorDiscoveryRequest(5, 5), attErrorRsp(attOpFindInfoReq, 5, attEcodeAttrNotFound)})
		if err != nil {
			t.Fatal(err)
		}
		if round == 0 {
			original, cccd = c, c.Descriptor()
		} else if c != original || c.Descriptor() != cccd || s.Characteristics()[0] != original || c.Descriptors()[0] != cccd {
			t.Fatal("rediscovery replaced published metadata objects")
		}
	}
}
