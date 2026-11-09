package gatt

import (
	"fmt"
	"log"
)

// attr is a BLE attribute. It is not exported;
// managing attributes is an implementation detail.
type attr struct {
	h        uint16   // attribute handle
	nativeID int      // Opaque local attribute ID used by the macOS backend.
	typ      UUID     // attribute type in UUID
	props    Property // attripute property
	secure   Property // attribute secure (implementation specific usage)
	value    []byte   // attribute value

	// Linux connections retain these registration values when the public
	// service objects are reused to build a newer attribute table.
	groupEnd    uint16
	valueHandle uint16
	notifyProps Property
	rhandler    ReadHandler
	whandler    WriteHandler
	nhandler    NotifyHandler

	pvt interface{} // point to the corresponsing Serveice/Characteristic/Descriptor
}

// A attrRange is a contiguous range of attributes.
type attrRange struct {
	aa   []attr
	base uint16 // handle for first attr in aa
}

const (
	tooSmall = -1
	tooLarge = -2
)

// idx returns the index into aa corresponding to attr a.
// If h is too small, idx returns tooSmall (-1).
// If h is too large, idx returns tooLarge (-2).
func (r *attrRange) idx(h int) int {
	if h < int(r.base) {
		return tooSmall
	}
	if int(h) >= int(r.base)+len(r.aa) {
		return tooLarge
	}
	return h - int(r.base)
}

// At returns attr a.
func (r *attrRange) At(h uint16) (a attr, ok bool) {
	i := r.idx(int(h))
	if i < 0 {
		return attr{}, false
	}
	return r.aa[i], true
}

// Subrange returns attributes in range [start, end]; it may
// return an empty slice. Subrange does not panic for
// out-of-range start or end.
func (r *attrRange) Subrange(start, end uint16) []attr {
	startidx := r.idx(int(start))
	switch startidx {
	case tooSmall:
		startidx = 0
	case tooLarge:
		return []attr{}
	}

	endidx := r.idx(int(end) + 1) // [start, end] includes its upper bound!
	switch endidx {
	case tooSmall:
		return []attr{}
	case tooLarge:
		endidx = len(r.aa)
	}
	return r.aa[startidx:endidx]
}

func dumpAttributes(aa []attr) {
	log.Printf("Generating attribute table:")
	log.Printf("handle\ttype\tprops\tsecure\tpvt\tvalue")
	for _, a := range aa {
		log.Printf("0x%04X\t0x%s\t0x%02X\t0x%02x\t%T\t[ % X ]",
			a.h, a.typ, int(a.props), int(a.secure), a.pvt, a.value)
	}
}

// Registration checks capacity before generation can change public handles.
// Services and their configuration are prepared before registration.
func validateAttributeCount(ss []*Service) error {
	remaining := 0xffff
	for _, s := range ss {
		if remaining < 1 {
			return fmt.Errorf("attribute table exceeds 65535 handles: %w", ErrInvalidLength)
		}
		remaining--
		for _, c := range s.Characteristics() {
			if remaining < 2 {
				return fmt.Errorf("attribute table exceeds 65535 handles: %w", ErrInvalidLength)
			}
			remaining -= 2
			descriptors := len(c.Descriptors())
			if descriptors > remaining {
				return fmt.Errorf("attribute table exceeds 65535 handles: %w", ErrInvalidLength)
			}
			remaining -= descriptors
		}
	}
	return nil
}

func generateAttributes(ss []*Service, base uint16) *attrRange {
	var aa []attr
	h := base
	last := len(ss) - 1
	for i, s := range ss {
		var a []attr
		h, a = generateServiceAttributes(s, h, i == last)
		aa = append(aa, a...)
	}
	dumpAttributes(aa)
	return &attrRange{aa: aa, base: base}
}

func generateServiceAttributes(s *Service, h uint16, last bool) (uint16, []attr) {
	s.SetHandle(h)
	// endh set later
	a := attr{
		h:     h,
		typ:   attrPrimaryServiceUUID,
		value: s.uuid.b,
		props: CharRead,
		pvt:   s,
	}
	aa := []attr{a}
	h++

	for _, c := range s.Characteristics() {
		var a []attr
		h, a = generateCharAttributes(c, h)
		aa = append(aa, a...)
	}

	endHandle := h - 1
	if last {
		h = 0xFFFF
		endHandle = h
	}
	s.SetEndHandle(endHandle)
	aa[0].groupEnd = endHandle

	return h, aa
}

func generateCharAttributes(c *Characteristic, h uint16) (uint16, []attr) {
	valueHandle := h + 1
	c.descsMu.Lock()
	c.h, c.vh = h, valueHandle
	c.descsMu.Unlock()
	ca := attr{
		h:     h,
		typ:   attrCharacteristicUUID,
		value: append([]byte{byte(c.props), byte(valueHandle), byte(valueHandle >> 8)}, c.uuid.b...),
		props: CharRead,
		pvt:   c,
	}
	va := attr{
		h:        valueHandle,
		typ:      c.uuid,
		value:    c.value,
		props:    c.props,
		rhandler: c.rhandler,
		whandler: c.whandler,
		pvt:      c,
	}
	h += 2

	aa := []attr{ca, va}
	for _, d := range c.Descriptors() {
		a := generateDescAttributes(d, h)
		if d.uuid.Equal(attrClientCharacteristicConfigUUID) {
			a.valueHandle = valueHandle
			a.notifyProps = c.props & (CharNotify | CharIndicate)
			a.nhandler = c.nhandler
		}
		aa = append(aa, a)
		h++
	}

	return h, aa
}

func generateDescAttributes(d *Descriptor, h uint16) attr {
	d.SetHandle(h)
	a := attr{
		h:        h,
		typ:      d.uuid,
		value:    d.value,
		props:    d.props,
		rhandler: d.rhandler,
		whandler: d.whandler,
		pvt:      d,
	}
	return a
}
