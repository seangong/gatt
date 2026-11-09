package socket

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
)

const socketExecLowWater = 7513

func TestSocketExecHelper(t *testing.T) {
	value := os.Getenv("GATT_SOCKET_EXEC_FD")
	if value == "" {
		return
	}
	fd, err := strconv.Atoi(value)
	if err != nil {
		os.Exit(2)
	}
	// The runtime may reuse a closed fd. Match this socket's distinct option
	// rather than mistaking an unrelated descriptor for an inherited socket.
	lowWater, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVLOWAT)
	if err == nil && lowWater == socketExecLowWater {
		fmt.Fprintf(os.Stderr, "child inherited socket fd %d\n", fd)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestSocketIsClosedOnExec(t *testing.T) {
	testSocketClosedOnExec(t, Socket)
}

func TestSocketFallbackIsClosedOnExec(t *testing.T) {
	testSocketClosedOnExec(t, socketCloseOnExec)
}

func testSocketClosedOnExec(t *testing.T, open func(int, int, int) (int, error)) {
	t.Helper()
	fd, err := open(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVLOWAT, socketExecLowWater); err != nil {
		t.Fatal(err)
	}
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	if flags&syscall.FD_CLOEXEC == 0 {
		t.Error("socket has no close-on-exec flag")
	}
	child := exec.Command(os.Args[0], "-test.run=^TestSocketExecHelper$")
	child.Env = append(os.Environ(), "GATT_SOCKET_EXEC_FD="+strconv.Itoa(fd))
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("exec child: %v: %s", err, output)
	}
}
