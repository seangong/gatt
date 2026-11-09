package linux

const advertisementCacheCapacity = 4096

// advertisementLocked requires plistmu. Reading or creating an entry makes it
// most recently used; an unknown scan response does not create an entry.
func (h *HCI) advertisementLocked(key peerAddress, create bool) *advertisementEntry {
	entry := h.plist[key]
	if entry == nil {
		if !create {
			return nil
		}
		if h.plist == nil {
			h.plist = make(map[peerAddress]*advertisementEntry)
		}
		entry = &advertisementEntry{}
		h.plist[key] = entry
	}
	if entry.element == nil {
		entry.element = h.plistLRU.PushFront(key)
	} else {
		h.plistLRU.MoveToFront(entry.element)
	}
	for len(h.plist) > advertisementCacheCapacity {
		oldest := h.plistLRU.Back()
		oldKey := oldest.Value.(peerAddress)
		h.plistLRU.Remove(oldest)
		delete(h.plist, oldKey)
	}
	return entry
}
