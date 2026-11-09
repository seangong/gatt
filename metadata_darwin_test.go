//go:build gattxpc_test

package gatt

import (
	"testing"

	"github.com/seangong/gatt/v2/xpc"
)

func TestDarwinClientDiscoveryConcurrentMetadataGetters(t *testing.T) {
	s := &Service{h: 1, endh: 10}
	_, p := newDarwinClientTest(t, func(id int, _ xpc.Dict) (int, xpc.Dict) {
		switch id {
		case 62:
			// A public getter must also remain available while IO is pending.
			_ = s.Characteristics()
			return characteristicsDiscovered, xpc.Dict{"kCBMsgArgCharacteristics": xpc.Array{darwinCharacteristicRecord(2, 3, UUID16(0x2a19))}}
		case 70:
			c := s.Characteristics()[0]
			_ = c.Descriptors()
			_ = c.Descriptor()
			return descriptorsDiscovered, xpc.Dict{"kCBMsgArgDescriptors": xpc.Array{darwinDescriptorRecord(4, attrClientCharacteristicConfigUUID)}}
		}
		return 0, nil
	})
	stopReading := readMetadataForTest(t, s)
	defer stopReading()
	var original *Characteristic
	var cccd *Descriptor
	for round := 0; round < 3; round++ {
		chars, err := p.DiscoverCharacteristics(nil, s)
		if err != nil || len(chars) != 1 {
			t.Fatalf("characteristics = %v, %v", chars, err)
		}
		c := chars[0]
		descs, err := p.DiscoverDescriptors(nil, c)
		if err != nil || len(descs) != 1 || c.Descriptor() != descs[0] {
			t.Fatalf("descriptors = %v, %v", descs, err)
		}
		if round == 0 {
			original, cccd = c, descs[0]
		} else if c != original || descs[0] != cccd || s.Characteristics()[0] != original {
			t.Fatal("rediscovery replaced published metadata objects")
		}
	}
}
