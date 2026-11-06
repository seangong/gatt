package gatt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

type notificationTestConn struct {
	mu          sync.Mutex
	packets     [][]byte
	writeResult func([]byte) (int, error)
}

func (c *notificationTestConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *notificationTestConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.packets = append(c.packets, append([]byte(nil), b...))
	c.mu.Unlock()
	if c.writeResult != nil {
		return c.writeResult(b)
	}
	return len(b), nil
}

func (c *notificationTestConn) Close() error { return nil }

func newNotificationTestNotifier(t *testing.T) (*notifier, *notificationTestConn) {
	t.Helper()
	s := NewService(MustParseUUID("180f"))
	s.AddCharacteristic(MustParseUUID("2a19")).HandleNotifyFunc(func(Request, Notifier) {})
	attrs := generateAttributes([]*Service{s}, 1)
	conn := &notificationTestConn{}
	c := newCentral(attrs, nil, conn)
	for i := range attrs.aa {
		a := &attrs.aa[i]
		if a.typ.Equal(attrClientCharacteristicConfigUUID) {
			return newNotifier(c, a, c.MTU()-3), conn
		}
	}
	t.Fatal("notification characteristic has no CCCD")
	return nil, nil
}

func TestNotificationWrite(t *testing.T) {
	transportErr := errors.New("notification transport failed")
	tests := []struct {
		name        string
		payloadSize func(int) int
		writeResult func([]byte) (int, error)
		wantN       int
		wantErr     error
		wantWrites  int
	}{
		{
			name:        "at capacity",
			payloadSize: func(capacity int) int { return capacity },
			wantN:       -1,
			wantWrites:  1,
		},
		{
			name:        "over capacity",
			payloadSize: func(capacity int) int { return capacity + 1 },
			wantErr:     io.ErrShortWrite,
		},
		{
			name:        "empty payload",
			payloadSize: func(int) int { return 0 },
			wantWrites:  1,
		},
		{
			name:        "transport error before writing",
			writeResult: func([]byte) (int, error) { return 0, transportErr },
			wantErr:     transportErr,
			wantWrites:  1,
		},
		{
			name:        "transport error after partial payload",
			writeResult: func([]byte) (int, error) { return 5, transportErr },
			wantN:       2,
			wantErr:     transportErr,
			wantWrites:  1,
		},
		{
			name:        "transport error after full payload",
			writeResult: func(b []byte) (int, error) { return len(b), transportErr },
			wantN:       -1,
			wantErr:     transportErr,
			wantWrites:  1,
		},
		{
			name:        "short write with no bytes",
			writeResult: func([]byte) (int, error) { return 0, nil },
			wantErr:     io.ErrShortWrite,
			wantWrites:  1,
		},
		{
			name:        "short write inside header",
			writeResult: func([]byte) (int, error) { return 2, nil },
			wantErr:     io.ErrShortWrite,
			wantWrites:  1,
		},
		{
			name:        "short write with header only",
			writeResult: func([]byte) (int, error) { return 3, nil },
			wantErr:     io.ErrShortWrite,
			wantWrites:  1,
		},
		{
			name:        "short write inside payload",
			writeResult: func([]byte) (int, error) { return 5, nil },
			wantN:       2,
			wantErr:     io.ErrShortWrite,
			wantWrites:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, conn := newNotificationTestNotifier(t)
			conn.writeResult = tt.writeResult
			size := 4
			if tt.payloadSize != nil {
				size = tt.payloadSize(n.Cap())
			}
			payload := bytes.Repeat([]byte{0x5a}, size)
			wantN := tt.wantN
			if wantN == -1 {
				wantN = len(payload)
			}
			gotN, err := n.Write(payload)
			if gotN != wantN || !errors.Is(err, tt.wantErr) {
				t.Fatalf("Write() = (%d, %v), want (%d, %v)", gotN, err, wantN, tt.wantErr)
			}
			if len(conn.packets) != tt.wantWrites {
				t.Fatalf("transport writes = %d, want %d", len(conn.packets), tt.wantWrites)
			}
			if tt.wantWrites == 0 {
				return
			}
			handle := n.a.pvt.(*Descriptor).char.vh
			want := []byte{attOpHandleNotify, byte(handle), byte(handle >> 8)}
			want = append(want, payload...)
			if !bytes.Equal(conn.packets[0], want) {
				t.Fatalf("notification = %x, want %x", conn.packets[0], want)
			}
		})
	}
}

func TestNotifierCapacityAfterMTU(t *testing.T) {
	n, conn := newNotificationTestNotifier(t)
	for _, mtu := range []uint16{64, 32} {
		var request [2]byte
		binary.LittleEndian.PutUint16(request[:], mtu)
		n.central.handleMTU(request[:])
		if got := n.Cap(); got != int(mtu)-3 {
			t.Fatalf("Cap() after MTU %d = %d, want %d", mtu, got, int(mtu)-3)
		}
		payload := bytes.Repeat([]byte{0x5a}, n.Cap())
		if count, err := n.Write(payload); count != len(payload) || err != nil {
			t.Fatalf("Write() at capacity after MTU %d = (%d, %v)", mtu, count, err)
		}
		writes := len(conn.packets)
		if count, err := n.Write(append(payload, 0)); count != 0 || !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("Write() over capacity after MTU %d = (%d, %v)", mtu, count, err)
		}
		if len(conn.packets) != writes {
			t.Fatalf("oversized notification after MTU %d reached transport", mtu)
		}
	}
}

func TestNotifierConcurrentMTU(t *testing.T) {
	n, _ := newNotificationTestNotifier(t)
	initialMTU := n.central.MTU()
	const negotiatedMTU = 64
	const iterations = 1000
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			mtu := uint16(initialMTU)
			if i%2 == 0 {
				mtu = negotiatedMTU
			}
			var request [2]byte
			binary.LittleEndian.PutUint16(request[:], mtu)
			n.central.handleMTU(request[:])
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			capacity := n.Cap()
			if capacity != initialMTU-3 && capacity != negotiatedMTU-3 {
				errs <- fmt.Errorf("Cap() = %d during MTU negotiation", capacity)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if count, err := n.Write([]byte{0x5a}); count != 1 || err != nil {
				errs <- fmt.Errorf("Write() during MTU negotiation = (%d, %v)", count, err)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
