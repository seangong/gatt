package linux

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/seangong/gatt/v2/linux/gioctl"
	"github.com/seangong/gatt/v2/linux/socket"
)

type device struct {
	file      *os.File
	raw       syscall.RawConn
	dev       int
	name      string
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func newDevice(n int, chk bool) (_ *device, err error) {
	fd, err := socket.Socket(socket.AF_BLUETOOTH, syscall.SOCK_RAW, socket.BTPROTO_HCI)
	if err != nil {
		return nil, err
	}
	d, err := newPollDevice(fd)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if closeErr := d.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	if n != -1 {
		d.name, err = newSocket(fd, n, chk)
		if err != nil {
			return nil, err
		}
		d.dev = n
		return d, nil
	}

	req := devListRequest{devNum: hciMaxDevices}
	if err := gioctl.Ioctl(uintptr(fd), hciGetDeviceList, uintptr(unsafe.Pointer(&req))); err != nil {
		return nil, err
	}
	d.dev, d.name, err = selectDevice(req, func(id int) (string, error) {
		return newSocket(fd, id, chk)
	})
	if err != nil {
		return nil, err
	}
	log.Printf("dev: %s opened", d.name)
	return d, nil
}

// selectDevice uses the identifiers returned by the kernel; they need not be
// contiguous after controllers have been attached or removed.
func selectDevice(req devListRequest, probe func(int) (string, error)) (int, string, error) {
	if int(req.devNum) > len(req.devRequest) {
		return 0, "", errors.New("invalid HCI device list length")
	}
	for _, entry := range req.devRequest[:int(req.devNum)] {
		id := int(entry.id)
		if name, err := probe(id); err == nil {
			return id, name, nil
		}
	}
	return 0, "", errors.New("no supported devices available")
}

// newPollDevice takes ownership of fd, including when initialization fails.
func newPollDevice(fd int) (_ *device, err error) {
	if err := syscall.SetNonblock(fd, true); err != nil {
		return nil, errors.Join(fmt.Errorf("set HCI socket non-blocking: %w", err), syscall.Close(fd))
	}
	f := os.NewFile(uintptr(fd), "hci")
	defer func() {
		if err != nil {
			if closeErr := f.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	// A non-blocking descriptor alone does not guarantee that the runtime
	// poller can manage it. Reject a transport whose IO cannot be canceled.
	if err := f.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("initialize HCI socket deadlines: %w", err)
	}
	raw, err := f.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("initialize HCI socket IO: %w", err)
	}
	return &device{file: f, raw: raw}, nil
}

func newSocket(fd, n int, chk bool) (string, error) {
	i := hciDevInfo{id: uint16(n)}
	if err := gioctl.Ioctl(uintptr(fd), hciGetDeviceInfo, uintptr(unsafe.Pointer(&i))); err != nil {
		return "", err
	}
	name := string(i.name[:])
	// Check the feature list returned feature list.
	if chk && i.features[4]&0x40 == 0 {
		err := errors.New("does not support LE")
		log.Printf("dev: %s %s", name, err)
		return "", err
	}
	log.Printf("dev: %s up", name)
	if err := gioctl.Ioctl(uintptr(fd), hciUpDevice, uintptr(n)); err != nil {
		if err != syscall.EALREADY {
			return "", err
		}
		log.Printf("dev: %s reset", name)
		if err := gioctl.Ioctl(uintptr(fd), hciResetDevice, uintptr(n)); err != nil {
			return "", err
		}
	}
	log.Printf("dev: %s down", name)
	if err := gioctl.Ioctl(uintptr(fd), hciDownDevice, uintptr(n)); err != nil {
		return "", err
	}

	// Attempt to use the linux 3.14 feature, if this fails with EINVAL fall back to raw access
	// on older kernels.
	sa := socket.SockaddrHCI{Dev: n, Channel: socket.HCI_CHANNEL_USER}
	if err := socket.Bind(fd, &sa); err != nil {
		if err != syscall.EINVAL {
			return "", err
		}
		log.Printf("dev: %s can't bind to hci user channel, err: %s.", name, err)
		sa := socket.SockaddrHCI{Dev: n, Channel: socket.HCI_CHANNEL_RAW}
		if err := socket.Bind(fd, &sa); err != nil {
			log.Printf("dev: %s can't bind to hci raw channel, err: %s.", name, err)
			return "", err
		}
	}
	return name, nil
}

func (d *device) Read(b []byte) (int, error) {
	return d.file.Read(b)
}

func (d *device) Write(b []byte) (int, error) {
	return d.writePacket(b, nil)
}

func (d *device) writeACL(b []byte, submit aclSubmitFunc) (int, error) {
	return d.writePacket(b, submit)
}

func (d *device) writePacket(b []byte, submit aclSubmitFunc) (int, error) {
	var n int
	var writeErr error
	err := d.raw.Write(func(fd uintptr) bool {
		for {
			attempt := func() (int, error) { return syscall.Write(int(fd), b) }
			if submit == nil {
				n, writeErr = attempt()
			} else {
				n, writeErr = submit(attempt)
			}
			if writeErr == syscall.EINTR {
				continue
			}
			// Each HCI packet must be written by one system call. Only retry
			// after EAGAIN, which has not sent any part of this packet.
			return writeErr != syscall.EAGAIN && writeErr != syscall.EWOULDBLOCK
		}
	})
	if err != nil {
		// RawConn exposes the poller's closing error rather than os.ErrClosed.
		if d.closed.Load() {
			err = os.ErrClosed
		}
		return 0, fmt.Errorf("write HCI packet: %w", err)
	}
	if writeErr != nil {
		return max(n, 0), os.NewSyscallError("write", writeErr)
	}
	if n != len(b) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (d *device) SetDeadline(t time.Time) error {
	return d.file.SetDeadline(t)
}

func (d *device) Close() error {
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		d.closeErr = d.file.Close()
	})
	return d.closeErr
}
