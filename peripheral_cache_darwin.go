package gatt

import (
	"fmt"
	"sort"

	"github.com/seangong/gatt/v2/xpc"
)

// discoveryScope distinguishes a complete UUID query from records merely
// observed in a partial response. Some native backends return extra records.
// All fields are owned by peripheral.metadataGate.
type discoveryScope struct {
	full  bool
	uuids map[string]bool
	seen  map[uint16]bool
}

type darwinAttribute struct {
	uuid       UUID
	h, vh, end uint16
	props      Property
}

// Native UUID filters treat an assigned 16-bit UUID and its Bluetooth-base
// 128-bit form as equivalent. Keep that rule local to discovery bookkeeping.
func darwinUUIDKey(uuid UUID) string {
	key := uuid.String()
	if len(uuid.b) == 2 {
		return "0000" + key + "00001000800000805f9b34fb"
	}
	return key
}

func inDiscoveryScope(filters []UUID, uuid UUID) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		if darwinUUIDKey(filter) == darwinUUIDKey(uuid) {
			return true
		}
	}
	return false
}

func sameDarwinAttribute(a, b darwinAttribute) bool {
	return a.h == b.h && a.vh == b.vh && a.end == b.end && a.props == b.props && darwinUUIDKey(a.uuid) == darwinUUIDKey(b.uuid)
}

func (s discoveryScope) changed(old, found []darwinAttribute, filters []UUID) bool {
	byHandle := make(map[uint16]darwinAttribute, len(old))
	for _, a := range old {
		byHandle[a.h] = a
	}
	incoming := make(map[uint16]bool, len(found))
	for _, a := range found {
		incoming[a.h] = true
		if s.seen[a.h] {
			if prior, ok := byHandle[a.h]; !ok || !sameDarwinAttribute(prior, a) {
				return true
			}
		} else if s.full || s.uuids[darwinUUIDKey(a.uuid)] {
			return true
		}
	}
	for _, a := range old {
		if s.seen[a.h] && inDiscoveryScope(filters, a.uuid) && !incoming[a.h] {
			return true
		}
	}
	return false
}

func (s *discoveryScope) record(found []darwinAttribute, filters []UUID) {
	if s.seen == nil {
		s.seen = make(map[uint16]bool)
	}
	for _, a := range found {
		s.seen[a.h] = true
	}
	if len(filters) == 0 {
		s.full = true
		return
	}
	if s.uuids == nil {
		s.uuids = make(map[string]bool)
	}
	for _, uuid := range filters {
		s.uuids[darwinUUIDKey(uuid)] = true
	}
}

func darwinResult(rsp xpc.Dict) error {
	value, ok := rsp["kCBMsgArgResult"].(int64)
	if !ok {
		return fmt.Errorf("invalid XPC operation result: %w", ErrInvalidLength)
	}
	if value == 0 {
		return nil
	}
	if value < 0 || value > 0xff {
		return fmt.Errorf("XPC operation result %d", value)
	}
	return attEcode(value)
}

func darwinDiscoveryArray(rsp xpc.Dict, key string) (xpc.Array, error) {
	// Preserve successful legacy responses that omit a result field, provided
	// their discovery payload is well formed. Explicit errors always win.
	if _, present := rsp["kCBMsgArgResult"]; present {
		if err := darwinResult(rsp); err != nil {
			return nil, err
		}
	}
	array, ok := rsp[key].(xpc.Array)
	if !ok {
		return nil, fmt.Errorf("invalid XPC discovery array %s: %w", key, ErrInvalidLength)
	}
	return array, nil
}

func darwinRecord(value interface{}) (xpc.Dict, error) {
	record, ok := value.(xpc.Dict)
	if !ok {
		return nil, fmt.Errorf("invalid XPC discovery record: %w", ErrInvalidLength)
	}
	return record, nil
}

func darwinHandle(record xpc.Dict, key string) (uint16, error) {
	value, ok := record[key].(int64)
	if !ok || value <= 0 || value > 0xffff {
		return 0, fmt.Errorf("invalid XPC handle %s: %w", key, ErrInvalidLength)
	}
	return uint16(value), nil
}

func darwinUUID(record xpc.Dict) (UUID, error) {
	value, ok := record["kCBMsgArgUUID"].([]byte)
	if !ok {
		return UUID{}, fmt.Errorf("invalid XPC attribute UUID: %w", ErrInvalidLength)
	}
	uuid, err := ParseUUID(fmt.Sprintf("%x", value))
	if err != nil {
		return UUID{}, fmt.Errorf("invalid XPC attribute UUID: %w", err)
	}
	return uuid, nil
}

func validDarwinAttributes(records []darwinAttribute) error {
	ordered := append([]darwinAttribute(nil), records...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].h < ordered[j].h })
	var previous uint16
	for _, a := range ordered {
		if a.h <= previous {
			return fmt.Errorf("overlapping XPC attributes at 0x%04x: %w", a.h, ErrInvalidLength)
		}
		previous = a.h
		if a.vh > previous {
			previous = a.vh
		}
		if a.end > previous {
			previous = a.end
		}
	}
	return nil
}

func darwinServices(records []*Service) []darwinAttribute {
	attributes := make([]darwinAttribute, len(records))
	for i, s := range records {
		attributes[i] = darwinAttribute{uuid: s.uuid, h: s.h, end: s.endh}
	}
	return attributes
}

func darwinCharacteristics(records []*Characteristic) []darwinAttribute {
	attributes := make([]darwinAttribute, len(records))
	for i, c := range records {
		// endh is not supplied by the native discovery response. In particular,
		// a filtered response cannot establish the next characteristic's handle.
		attributes[i] = darwinAttribute{uuid: c.uuid, h: c.h, vh: c.vh, props: c.props}
	}
	return attributes
}

func darwinDescriptors(records []*Descriptor) []darwinAttribute {
	attributes := make([]darwinAttribute, len(records))
	for i, d := range records {
		attributes[i] = darwinAttribute{uuid: d.uuid, h: d.h}
	}
	return attributes
}
