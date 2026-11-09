//go:build linux
// +build linux

package socket

import "syscall"

const AF_BLUETOOTH = syscall.AF_BLUETOOTH

func openSocket(domain, typ, proto int) (int, error) {
	fd, err := syscall.Socket(domain, typ|syscall.SOCK_CLOEXEC, proto)
	if err != syscall.EINVAL {
		return fd, err
	}
	return socketCloseOnExec(domain, typ&^syscall.SOCK_CLOEXEC, proto)
}
