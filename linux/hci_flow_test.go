package linux

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seangong/gatt/v2/linux/cmd"
	"github.com/seangong/gatt/v2/linux/evt"
)

type flowTestDevice struct {
	input     chan []byte
	done      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	writes    [][]byte
}

func (d *flowTestDevice) Read(b []byte) (int, error) {
	select {
	case packet := <-d.input:
		return copy(b, packet), nil
	case <-d.done:
		return 0, io.EOF
	}
}

func (d *flowTestDevice) Write(b []byte) (int, error) {
	select {
	case <-d.done:
		return 0, io.ErrClosedPipe
	default:
	}
	d.mu.Lock()
	d.writes = append(d.writes, append([]byte(nil), b...))
	d.mu.Unlock()
	// Each ACL fragment completes only after mainLoop reads this event.
	d.input <- []byte{byte(typEventPkt), evt.NumberOfCompletedPkts, 5, 1, 1, 0, 1, 0}
	return len(b), nil
}

func (d *flowTestDevice) writeACL(b []byte, submit aclSubmitFunc) (int, error) {
	return submit(func() (int, error) { return d.Write(b) })
}

func (d *flowTestDevice) Close() error {
	d.closeOnce.Do(func() { close(d.done) })
	return nil
}

func TestHCIReceivesCommandsWhileWritingLargeResponse(t *testing.T) {
	const handle = 1
	d := &flowTestDevice{input: make(chan []byte, 32), done: make(chan struct{})}
	h := &HCI{
		d:       d,
		c:       cmd.NewCmd(d),
		e:       evt.NewEvt(),
		bufCnt:  make(chan struct{}, 14),
		bufSize: 27,
		connsmu: &sync.Mutex{},
		conns:   make(map[uint16]*conn),
	}
	c := newConn(h, handle)
	h.conns[handle] = c
	completed := make(chan struct{}, 32)
	h.registerEventHandlers()
	h.e.HandleEvent(evt.NumberOfCompletedPkts, evt.HandlerFunc(func(b []byte) error {
		if err := h.handleNumberOfCompletedPkts(b); err != nil {
			return err
		}
		completed <- struct{}{}
		return nil
	}))
	request := []byte{0x0a, 1, 0}
	command := []byte{0x52, 3, 0, 'a'}
	for _, payload := range [][]byte{request, command} {
		pdu := []byte{byte(len(payload)), byte(len(payload) >> 8), 4, 0}
		pdu = append(pdu, payload...)
		packet := append([]byte{byte(typACLDataPkt)}, aclPacketForTest(handle, 0x02, pdu...)...)
		d.input <- packet
	}
	response := append([]byte{0x0b}, bytes.Repeat([]byte{0x5a}, 512)...)
	abort := make(chan struct{})
	consumerDone := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		defer close(consumerDone)
		buf := make([]byte, 672)
		if n, err := c.Read(buf); err != nil || !bytes.Equal(buf[:n], request) {
			result <- fmt.Errorf("read request = %x, %v, want %x", buf[:n], err, request)
			return
		}
		if n, err := c.Write(response); err != nil || n != len(response) {
			result <- fmt.Errorf("write response = %d, %v, want %d, nil", n, err, len(response))
			return
		}
		select {
		case <-abort:
			return
		default:
		}
		n, err := c.Read(buf)
		if err != nil || !bytes.Equal(buf[:n], command) {
			result <- fmt.Errorf("read command = %x, %v, want %x", buf[:n], err, command)
			return
		}
		result <- nil
	}()
	mainDone := make(chan struct{})
	go func() {
		defer close(mainDone)
		h.mainLoop()
	}()
	defer func() {
		close(abort)
		select {
		case <-consumerDone:
		default:
			// Rescue the old implementation's blocked ATT delivery so that a
			// failed regression test does not leave its goroutines behind.
			select {
			case <-c.aclc:
			case <-consumerDone:
			case <-time.After(2 * time.Second):
				t.Error("could not release blocked ATT delivery")
			}
			select {
			case <-consumerDone:
			case <-time.After(2 * time.Second):
				t.Error("ATT consumer did not exit")
			}
		}
		d.mu.Lock()
		fragments := len(d.writes)
		d.mu.Unlock()
		for i := 0; i < fragments; i++ {
			select {
			case <-completed:
			case <-time.After(2 * time.Second):
				t.Error("completion event handler did not exit")
				i = fragments
			}
		}
		d.Close()
		select {
		case <-mainDone:
		case <-time.After(2 * time.Second):
			t.Error("HCI main loop did not exit")
		}
		h.Close()
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("large response and following Write Command blocked HCI flow control")
	}
	d.mu.Lock()
	packets := append([][]byte(nil), d.writes...)
	d.mu.Unlock()
	if len(packets) != 20 {
		t.Fatalf("ACL fragments = %d, want 20", len(packets))
	}
	var pdu []byte
	for i, packet := range packets {
		if len(packet) < 5 || int(binary.LittleEndian.Uint16(packet[3:5])) != len(packet)-5 {
			t.Fatalf("fragment %d has invalid ACL length: %x", i, packet)
		}
		pdu = append(pdu, packet[5:]...)
	}
	want := append([]byte{byte(len(response)), byte(len(response) >> 8), 4, 0}, response...)
	if !bytes.Equal(pdu, want) {
		t.Fatalf("reassembled response = %x, want %x", pdu, want)
	}
}
