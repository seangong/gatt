package gatt

func attErrorForStatus(status byte) attEcode {
	switch status {
	case StatusSuccess:
		return attEcodeSuccess
	case StatusInvalidOffset:
		return attEcodeInvalidOffset
	default:
		return attEcodeUnlikely
	}
}
