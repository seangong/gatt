//go:build darwin
// +build darwin

package socket

// For compile time compatibility
const AF_BLUETOOTH = 0

func openSocket(domain, typ, proto int) (int, error) {
	return socketCloseOnExec(domain, typ, proto)
}
