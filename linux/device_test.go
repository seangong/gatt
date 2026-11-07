package linux

import (
	"bytes"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"
)

func newSocketTestDevice(t *testing.T, socketType int) (*device, int) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, socketType, 0)
	if err != nil {
		t.Fatal(err)
	}
	d, err := newPollDevice(fds[0])
	if err != nil {
		if closeErr := syscall.Close(fds[1]); closeErr != nil {
			t.Errorf("close peer after initialization failure: %v", closeErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("close transport: %v", err)
		}
		if err := syscall.Close(fds[1]); err != nil {
			t.Errorf("close peer: %v", err)
		}
	})
	return d, fds[1]
}

func fillDeviceSendBuffer(t *testing.T, d *device) {
	t.Helper()
	var fillErr error
	if err := d.raw.Control(func(fd uintptr) {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096); err != nil {
			fillErr = err
			return
		}
		packet := []byte{0x5a}
		for i := 0; i < 65536; i++ {
			_, err := syscall.Write(int(fd), packet)
			if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
				return
			}
			if err != nil {
				fillErr = err
				return
			}
		}
		fillErr = errors.New("socket send buffer did not fill")
	}); err != nil {
		t.Fatal(err)
	}
	if fillErr != nil {
		t.Fatal(fillErr)
	}
}

func TestDeviceCloseCancelsIO(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			d, _ := newSocketTestDevice(t, syscall.SOCK_STREAM)
			if operation == "write" {
				fillDeviceSendBuffer(t, d)
			}
			result := make(chan error, 1)
			go func() {
				var err error
				if operation == "read" {
					_, err = d.Read(make([]byte, 256))
				} else {
					_, err = d.Write([]byte{0x5a})
				}
				result <- err
			}()
			select {
			case err := <-result:
				t.Fatalf("%s before Close returned %v, want pending IO", operation, err)
			case <-time.After(20 * time.Millisecond):
			}
			closed := make(chan error, 2)
			for i := 0; i < cap(closed); i++ {
				go func() { closed <- d.Close() }()
			}
			for i := 0; i < cap(closed); i++ {
				select {
				case err := <-closed:
					if err != nil {
						t.Errorf("Close() = %v, want nil", err)
					}
				case <-time.After(time.Second):
					t.Fatal("Close did not release pending IO")
				}
			}
			select {
			case err := <-result:
				if !errors.Is(err, os.ErrClosed) {
					t.Fatalf("%s after Close = %v, want closed file", operation, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("%s remained blocked after Close", operation)
			}
		})
	}
}

func TestDeviceDeadlineCancelsIO(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			d, _ := newSocketTestDevice(t, syscall.SOCK_STREAM)
			if operation == "write" {
				fillDeviceSendBuffer(t, d)
			}
			if err := d.SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			var n int
			var err error
			if operation == "read" {
				n, err = d.Read(make([]byte, 256))
			} else {
				n, err = d.Write([]byte{0x5a})
			}
			if n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("%s with deadline = (%d, %v), want (0, deadline exceeded)", operation, n, err)
			}
		})
	}
}

func TestDevicePreservesPacketBoundaries(t *testing.T) {
	d, peer := newSocketTestDevice(t, syscall.SOCK_DGRAM)
	packets := [][]byte{{2, 1, 0, 3, 0, 0x0a, 1, 0}, {4, 0x0e, 4, 1, 3, 0x0c, 0}}
	for _, packet := range packets {
		if n, err := d.Write(packet); err != nil || n != len(packet) {
			t.Fatalf("Write(%x) = (%d, %v)", packet, n, err)
		}
	}
	buf := make([]byte, 1024)
	for _, want := range packets {
		n, err := syscall.Read(peer, buf)
		if err != nil || !bytes.Equal(buf[:n], want) {
			t.Fatalf("peer Read() = (%x, %v), want %x", buf[:max(n, 0)], err, want)
		}
		if _, err := syscall.Write(peer, want); err != nil {
			t.Fatal(err)
		}
		if n, err := d.Read(buf); err != nil || !bytes.Equal(buf[:n], want) {
			t.Fatalf("Read() = (%x, %v), want %x", buf[:n], err, want)
		}
	}
	if err := d.SetDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read() after deadline = %v", err)
	}
	if err := d.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, err := syscall.Write(peer, packets[0]); err != nil {
		t.Fatal(err)
	}
	if n, err := d.Read(buf); err != nil || !bytes.Equal(buf[:n], packets[0]) {
		t.Fatalf("Read() after resetting deadline = (%x, %v)", buf[:n], err)
	}
}

func TestDeviceRejectsShortPacketWrite(t *testing.T) {
	d, peer := newSocketTestDevice(t, syscall.SOCK_STREAM)
	var socketErr error
	if err := d.raw.Control(func(fd uintptr) {
		socketErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096)
	}); err != nil {
		t.Fatal(err)
	}
	if socketErr != nil {
		t.Fatal(socketErr)
	}
	if err := d.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	packet := bytes.Repeat([]byte{0x5a}, 1<<20)
	n, err := d.Write(packet)
	if n <= 0 || n >= len(packet) || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Write() = (%d, %v), want partial packet and short write", n, err)
	}
	if err := syscall.SetNonblock(peer, true); err != nil {
		t.Fatal(err)
	}
	var received []byte
	buf := make([]byte, 4096)
	for {
		count, err := syscall.Read(peer, buf)
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, buf[:count]...)
	}
	if len(received) != n || !bytes.Equal(received, packet[:n]) {
		t.Fatalf("short packet delivered %d bytes, want exactly %d", len(received), n)
	}
}

func TestDeviceInitializationFailureClosesFD(t *testing.T) {
	fd, err := syscall.Open(os.DevNull, syscall.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	d, err := newPollDevice(fd)
	if d != nil || !errors.Is(err, os.ErrNoDeadline) {
		if d != nil {
			if closeErr := d.Close(); closeErr != nil {
				t.Errorf("close unexpected transport: %v", closeErr)
			}
		}
		t.Fatalf("initialize unsupported device = (%v, %v), want unsupported deadline error", d, err)
	}
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("failed transport left its fd open: Fstat error = %v", err)
	}
}
