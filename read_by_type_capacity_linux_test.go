package gatt

import (
	"bytes"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

func TestReadByTypeDoesNotCallReaderBeyondPageCapacity(t *testing.T) {
	for _, tt := range []struct {
		mtu   uint16
		value int
		rows  int
	}{
		{23, 19, 1},
		{23, 8, 2},
		{23, 0, 10},
		{517, 253, 2},
	} {
		t.Run(fmt.Sprintf("mtu=%d/value=%d", tt.mtu, tt.value), func(t *testing.T) {
			var services []*Service
			calls := 0
			var want []byte
			for i := 0; i <= tt.rows; i++ {
				s := NewService(UUID16(uint16(0xff00 + i)))
				c := s.AddCharacteristic(UUID16(0xfe01))
				c.HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
					calls++
					_, _ = w.Write(bytes.Repeat([]byte{'v'}, tt.value))
				})
				services = append(services, s)
			}
			c := newCentral(generateAttributes(services, 1), nil, nil)
			defer c.cancel()
			c.mtu = tt.mtu
			want = []byte{attOpReadByTypeRsp, byte(tt.value + 2)}
			for _, s := range services[:tt.rows] {
				h := s.Characteristics()[0].VHandle()
				want = append(want, byte(h), byte(h>>8))
				want = append(want, bytes.Repeat([]byte{'v'}, tt.value)...)
			}
			got := c.handleReq([]byte{attOpReadByTypeReq, 1, 0, 0xff, 0xff, 1, 0xfe})
			if !bytes.Equal(got, want) || calls != tt.rows {
				t.Fatalf("page=%x calls=%d, want %x and %d calls", got, calls, want, tt.rows)
			}
			// The unreturned attribute still belongs to the next page.
			h := services[tt.rows].Characteristics()[0].VHandle()
			got = c.handleReq([]byte{attOpReadByTypeReq, byte(h), byte(h >> 8), 0xff, 0xff, 1, 0xfe})
			next := append([]byte{attOpReadByTypeRsp, byte(tt.value + 2), byte(h), byte(h >> 8)}, bytes.Repeat([]byte{'v'}, tt.value)...)
			if !bytes.Equal(got, next) || calls != tt.rows+1 {
				t.Fatalf("next page=%x calls=%d, want %x and %d calls", got, calls, next, tt.rows+1)
			}
		})
	}
}

func TestReadByTypeFullPageDoesNotWaitForLaterHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := newPeripheralTestConn()
		first, second := NewService(UUID16(0xff00)), NewService(UUID16(0xff10))
		first.AddCharacteristic(UUID16(0xff01)).SetValue(bytes.Repeat([]byte{'a'}, 19))
		entered, release := make(chan struct{}), make(chan struct{})
		second.AddCharacteristic(UUID16(0xff01)).HandleReadFunc(func(w ResponseWriter, _ *ReadRequest) {
			close(entered)
			<-release
			_, _ = w.Write(bytes.Repeat([]byte{'b'}, 19))
		})
		c := newCentral(generateAttributes([]*Service{first, second}, 1), nil, conn)
		defer c.Close()
		done := make(chan bool, 1)
		go func() { done <- c.handleAndRespond([]byte{attOpReadByTypeReq, 1, 0, 0xff, 0xff, 1, 0xff}) }()
		synctest.Wait()
		select {
		case <-entered:
			t.Error("full page called the next attribute's handler")
		default:
		}
		time.Sleep(31 * time.Second)
		close(release)
		synctest.Wait()
		if !<-done || c.Context().Err() != nil {
			t.Fatal("unused later handler prevented the complete first page from being returned")
		}
		want := append([]byte{attOpReadByTypeRsp, 21, 3, 0}, bytes.Repeat([]byte{'a'}, 19)...)
		if got := peripheralWriteForTest(t, conn); !bytes.Equal(got, want) {
			t.Fatalf("full page=%x, want %x", got, want)
		}
	})
}

func TestReadByTypeStaticLongValuesKeepWireCapacity(t *testing.T) {
	var services []*Service
	for i := 0; i < 3; i++ {
		s := NewService(UUID16(uint16(0xff00 + i)))
		s.AddCharacteristic(UUID16(0xfe01)).SetValue(bytes.Repeat([]byte{'v'}, 512))
		services = append(services, s)
	}
	c := newCentral(generateAttributes(services, 1), nil, nil)
	defer c.cancel()
	c.mtu = 517
	got := c.handleReq([]byte{attOpReadByTypeReq, 1, 0, 0xff, 0xff, 1, 0xfe})
	want := []byte{attOpReadByTypeRsp, 255}
	for _, s := range services[:2] {
		h := s.Characteristics()[0].VHandle()
		want = append(want, byte(h), byte(h>>8))
		want = append(want, bytes.Repeat([]byte{'v'}, 253)...)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("long-value page length=%d, want %d", len(got), len(want))
	}
}
