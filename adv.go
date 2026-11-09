package gatt

import (
	"errors"
	"log"
)

// MaxEIRPacketLength is the maximum allowed AdvertisingPacket
// and ScanResponsePacket length.
const MaxEIRPacketLength = 31

// ErrEIRPacketTooLong is the error returned when an AdvertisingPacket
// or ScanResponsePacket is too long.
var ErrEIRPacketTooLong = errors.New("max packet length is 31")

// Advertising data field types
const (
	typeFlags             = 0x01 // Flags
	typeSomeUUID16        = 0x02 // Incomplete List of 16-bit Service Class UUIDs
	typeAllUUID16         = 0x03 // Complete List of 16-bit Service Class UUIDs
	typeSomeUUID32        = 0x04 // Incomplete List of 32-bit Service Class UUIDs
	typeAllUUID32         = 0x05 // Complete List of 32-bit Service Class UUIDs
	typeSomeUUID128       = 0x06 // Incomplete List of 128-bit Service Class UUIDs
	typeAllUUID128        = 0x07 // Complete List of 128-bit Service Class UUIDs
	typeShortName         = 0x08 // Shortened Local Name
	typeCompleteName      = 0x09 // Complete Local Name
	typeTxPower           = 0x0A // Tx Power Level
	typeClassOfDevice     = 0x0D // Class of Device
	typeSimplePairingC192 = 0x0E // Simple Pairing Hash C-192
	typeSimplePairingR192 = 0x0F // Simple Pairing Randomizer R-192
	typeSecManagerTK      = 0x10 // Security Manager TK Value
	typeSecManagerOOB     = 0x11 // Security Manager Out of Band Flags
	typeSlaveConnInt      = 0x12 // Slave Connection Interval Range
	typeServiceSol16      = 0x14 // List of 16-bit Service Solicitation UUIDs
	typeServiceSol128     = 0x15 // List of 128-bit Service Solicitation UUIDs
	typeServiceData16     = 0x16 // Service Data - 16-bit UUID
	typePubTargetAddr     = 0x17 // Public Target Address
	typeRandTargetAddr    = 0x18 // Random Target Address
	typeAppearance        = 0x19 // Appearance
	typeAdvInterval       = 0x1A // Advertising Interval
	typeLEDeviceAddr      = 0x1B // LE Bluetooth Device Address
	typeLERole            = 0x1C // LE Role
	typeServiceSol32      = 0x1F // List of 32-bit Service Solicitation UUIDs
	typeServiceData32     = 0x20 // Service Data - 32-bit UUID
	typeServiceData128    = 0x21 // Service Data - 128-bit UUID
	typeLESecConfirm      = 0x22 // LE Secure Connections Confirmation Value
	typeLESecRandom       = 0x23 // LE Secure Connections Random Value
	typeManufacturerData  = 0xFF // Manufacturer Specific Data
)

// Advertising type flags
const (
	flagLimitedDiscoverable = 0x01 // LE Limited Discoverable Mode
	flagGeneralDiscoverable = 0x02 // LE General Discoverable Mode
	flagLEOnly              = 0x04 // BR/EDR Not Supported. Bit 37 of LMP Feature Mask Definitions (Page 0)
	flagBothController      = 0x08 // Simultaneous LE and BR/EDR to Same Device Capable (Controller).
	flagBothHost            = 0x10 // Simultaneous LE and BR/EDR to Same Device Capable (Host).
)

// FIXME: check the unmarshalling of this data structure.
type ServiceData struct {
	UUID UUID
	Data []byte
}

// This is borrowed from core bluetooth.
// Embedded/Linux folks might be interested in more details.
type Advertisement struct {
	LocalName        string
	ManufacturerData []byte
	ServiceData      []ServiceData
	Services         []UUID
	OverflowService  []UUID
	TxPowerLevel     int
	Connectable      bool
	SolicitedService []UUID
}

// advertisementUUID preserves the public UUID widths by expanding wire-format
// 32-bit UUIDs to the Bluetooth base UUID. The result owns its bytes.
func advertisementUUID(b []byte) UUID {
	if len(b) == 4 {
		u := []byte{0xfb, 0x34, 0x9b, 0x5f, 0x80, 0x00, 0x00, 0x80, 0x00, 0x10, 0x00, 0x00, 0, 0, 0, 0}
		copy(u[12:], b)
		return UUID{u}
	}
	return UUID{append([]byte(nil), b...)}
}

// This is only used in Linux port.
func (a *Advertisement) unmarshall(b []byte) error {

	// Utility function for creating a list of uuids.
	uuidList := func(u []UUID, d []byte, w int) []UUID {
		if len(d)%w != 0 {
			return u
		}
		for len(d) > 0 {
			u = append(u, advertisementUUID(d[:w]))
			d = d[w:]
		}
		return u
	}

	for len(b) > 0 {
		l := int(b[0])
		if l == 0 {
			return nil
		}
		if l > len(b)-1 {
			return errors.New("invalid advertise data")
		}
		t := b[1]
		d := b[2 : 1+l]
		switch t {
		case typeFlags:
			// TODO: should we do anything about the discoverability here?
		case typeSomeUUID16:
			a.Services = uuidList(a.Services, d, 2)
		case typeAllUUID16:
			a.Services = uuidList(a.Services, d, 2)
		case typeSomeUUID32:
			a.Services = uuidList(a.Services, d, 4)
		case typeAllUUID32:
			a.Services = uuidList(a.Services, d, 4)
		case typeSomeUUID128:
			a.Services = uuidList(a.Services, d, 16)
		case typeAllUUID128:
			a.Services = uuidList(a.Services, d, 16)
		case typeShortName:
			a.LocalName = string(d)
		case typeCompleteName:
			a.LocalName = string(d)
		case typeTxPower:
			if len(d) > 0 {
				a.TxPowerLevel = int(int8(d[0]))
			}
		case typeServiceSol16:
			a.SolicitedService = uuidList(a.SolicitedService, d, 2)
		case typeServiceSol128:
			a.SolicitedService = uuidList(a.SolicitedService, d, 16)
		case typeServiceSol32:
			a.SolicitedService = uuidList(a.SolicitedService, d, 4)
		case typeManufacturerData:
			a.ManufacturerData = make([]byte, len(d))
			copy(a.ManufacturerData, d)
		case typeServiceData16, typeServiceData32, typeServiceData128:
			w := 2
			switch t {
			case typeServiceData32:
				w = 4
			case typeServiceData128:
				w = 16
			}
			if len(d) < w {
				break
			}
			a.ServiceData = append(a.ServiceData, ServiceData{
				UUID: advertisementUUID(d[:w]),
				Data: append([]byte(nil), d[w:]...),
			})
		default:
			log.Printf("DATA: [ % X ]", d)
		}
		b = b[1+l:]
	}
	return nil
}

// AdvPacket is an utility to help crafting advertisment or scan response data.
type AdvPacket struct {
	b   []byte
	err error
}

// Bytes returns an 31-byte array, which contains up to 31 bytes of the packet.
func (a *AdvPacket) Bytes() [31]byte {
	b := [31]byte{}
	copy(b[:], a.b)
	return b
}

// Len returns the length of the packets with a maximum of 31.
func (a *AdvPacket) Len() int {
	if len(a.b) > 31 {
		return 31
	}
	return len(a.b)
}

// Err reports a field whose header could not fit in the packet.
// Truncating field data to fit does not set an error.
func (a *AdvPacket) Err() error {
	return a.err
}

// AppendField appends a BLE advertising packet field.
// Data is truncated to fit. If the field header does not fit, the packet is
// unchanged and Err reports ErrEIRPacketTooLong.
func (a *AdvPacket) AppendField(typ byte, b []byte) *AdvPacket {
	// A field consists of len, typ, b.
	// Len is 1 byte for typ plus len(b).
	remaining := MaxEIRPacketLength - len(a.b) - 2
	if remaining < 0 {
		a.err = ErrEIRPacketTooLong
		return a
	}
	if len(b) > remaining {
		b = b[:remaining]
	}
	a.b = append(a.b, byte(len(b)+1))
	a.b = append(a.b, typ)
	a.b = append(a.b, b...)
	return a
}

// AppendFlags appends a flag field to the packet.
func (a *AdvPacket) AppendFlags(f byte) *AdvPacket {
	return a.AppendField(typeFlags, []byte{f})
}

// AppendFlags appends a name field to the packet.
// If the name fits in the space, it will be append as a complete name field, otherwise a short name field.
func (a *AdvPacket) AppendName(n string) *AdvPacket {
	typ := byte(typeCompleteName)
	if len(a.b)+2+len(n) > MaxEIRPacketLength {
		typ = byte(typeShortName)
	}
	return a.AppendField(typ, []byte(n))
}

// AppendManufacturerData appends a manufacturer data field to the packet.
func (a *AdvPacket) AppendManufacturerData(id uint16, b []byte) *AdvPacket {
	d := append([]byte{uint8(id), uint8(id >> 8)}, b...)
	return a.AppendField(typeManufacturerData, d)
}

// AppendUUIDFit groups advertised service UUIDs by width, prioritizing them in
// input order until one does not fit. GAP and GATT UUIDs are omitted. Existing
// incomplete lists remain incomplete. Zero and unsupported-width UUIDs are not
// encoded and cause a false result. Malformed existing fields leave the packet
// unchanged. This method does not set Err when UUIDs cannot be appended.
func (a *AdvPacket) AppendUUIDFit(uu []UUID) bool {
	fit, hasUUID := true, false
	for _, u := range uu {
		if u.Equal(attrGAPUUID) || u.Equal(attrGATTUUID) {
			continue
		}
		switch u.Len() {
		case 2, 16:
			hasUUID = true
		default:
			fit = false
		}
	}
	if !hasUUID {
		return fit
	}
	if a.err != nil {
		return false
	}

	type uuidList struct {
		width    int
		present  bool
		complete bool
		data     []byte
	}
	type field struct {
		raw  []byte
		list *uuidList
	}
	small := uuidList{width: 2, complete: true}
	large := uuidList{width: 16, complete: true}
	var fields []field
	size := 0
	// Keep the first list's position and the order of unrelated fields. Raw
	// AppendField calls may have supplied more than one list of the same width.
	for b := a.b; len(b) != 0; {
		n := int(b[0]) + 1
		if n < 2 || n > len(b) {
			return false
		}
		var list *uuidList
		switch b[1] {
		case typeSomeUUID16, typeAllUUID16:
			list = &small
		case typeSomeUUID128, typeAllUUID128:
			list = &large
		}
		if list == nil {
			fields = append(fields, field{raw: b[:n]})
			size += n
		} else {
			if (n-2)%list.width != 0 {
				return false
			}
			if !list.present {
				list.present = true
				fields = append(fields, field{list: list})
				size += 2
			}
			list.data = append(list.data, b[2:n]...)
			size += n - 2
			if b[1] == typeSomeUUID16 || b[1] == typeSomeUUID128 {
				list.complete = false
			}
		}
		b = b[n:]
	}

	blocked := false
	for _, u := range uu {
		if u.Equal(attrGAPUUID) || u.Equal(attrGATTUUID) {
			continue
		}
		var list *uuidList
		switch u.Len() {
		case 2:
			list = &small
		case 16:
			list = &large
		default:
			continue
		}
		cost := list.width
		if !list.present {
			cost += 2
		}
		if blocked || size+cost > MaxEIRPacketLength {
			fit, blocked, list.complete = false, true, false
			continue
		}
		if !list.present {
			list.present = true
			fields = append(fields, field{list: list})
			size += 2
		}
		list.data = append(list.data, u.b...)
		size += list.width
	}
	packet := make([]byte, 0, size)
	for _, f := range fields {
		if f.list == nil {
			packet = append(packet, f.raw...)
			continue
		}
		typ := byte(typeSomeUUID16)
		if f.list.width == 16 {
			typ = typeSomeUUID128
		}
		if f.list.complete {
			if f.list.width == 2 {
				typ = typeAllUUID16
			} else {
				typ = typeAllUUID128
			}
		}
		packet = append(packet, byte(len(f.list.data)+1), typ)
		packet = append(packet, f.list.data...)
	}
	a.b = packet
	return fit
}
