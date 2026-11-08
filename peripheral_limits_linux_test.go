package gatt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestPeripheralSingleWriteLimits(t *testing.T) {
	for _, mtu := range []uint16{23, 517} {
		limit := min(int(mtu)-3, attMaxValueLen)
		for _, length := range []int{0, limit, limit + 1} {
			for _, kind := range []string{"request", "command", "descriptor"} {
				t.Run(fmt.Sprintf("%s/mtu%d/value%d", kind, mtu, length), func(t *testing.T) {
					c := newPeripheralTestConn()
					p := newPeripheralForTest(c)
					p.mtu = mtu
					startPeripheralForTest(t, p, c)
					result := make(chan error, 1)
					value := bytes.Repeat([]byte{'x'}, length)
					go func() {
						if kind == "descriptor" {
							result <- p.WriteDescriptor(&Descriptor{h: 4}, value)
						} else {
							result <- p.WriteCharacteristic(&Characteristic{vh: 3}, value, kind == "command")
						}
					}()
					if length <= limit {
						packet := peripheralWriteForTest(t, c)
						if len(packet) != length+3 || !bytes.Equal(packet[3:], value) {
							t.Fatalf("write packet = %x, want whole value", packet)
						}
						if kind != "command" {
							c.reads <- peripheralTestRead{b: []byte{attOpWriteRsp}}
						}
						if err := peripheralResultForTest(t, result); err != nil {
							t.Fatal(err)
						}
					} else {
						select {
						case err := <-result:
							if !errors.Is(err, ErrInvalidLength) {
								t.Fatalf("oversized write error = %v", err)
							}
						case packet := <-c.writes:
							t.Fatalf("oversized write reached transport: %x", packet)
						case <-time.After(time.Second):
							t.Fatal("oversized write did not return")
						}
					}
					// A local validation failure must leave the bearer usable.
					read := make(chan peripheralReadResult, 1)
					go func() {
						value, err := p.ReadCharacteristic(&Characteristic{vh: 3})
						read <- peripheralReadResult{value, err}
					}()
					peripheralWriteForTest(t, c)
					c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'r'}}
					got := peripheralReadResultForTest(t, c, read)
					if got.err != nil || string(got.value) != "r" {
						t.Fatalf("read after write = %q, %v", got.value, got.err)
					}
				})
			}
		}
	}
}

func TestPeripheralSetMTURejectsUnsupportedLocalValues(t *testing.T) {
	for _, mtu := range []uint16{0, 22, 518, 0xffff} {
		t.Run(fmt.Sprint(mtu), func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			result := make(chan error, 1)
			go func() { result <- p.SetMTU(mtu) }()
			select {
			case err := <-result:
				if !errors.Is(err, ErrInvalidLength) {
					t.Fatalf("SetMTU error = %v", err)
				}
			case packet := <-c.writes:
				t.Fatalf("invalid local MTU reached transport: %x", packet)
			case <-time.After(time.Second):
				t.Fatal("invalid local MTU did not return")
			}
		})
	}
}

func TestPeripheralPeerMTUNormalizationPreservesShortRead(t *testing.T) {
	for _, serverMTU := range []uint16{0, 22, 23, 64, 0xffff} {
		t.Run(fmt.Sprint(serverMTU), func(t *testing.T) {
			c := newPeripheralTestConn()
			p := newPeripheralForTest(c)
			startPeripheralForTest(t, p, c)
			mtuResult := make(chan error, 1)
			go func() { mtuResult <- p.SetMTU(64) }()
			peripheralWriteForTest(t, c)
			c.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, byte(serverMTU), byte(serverMTU >> 8)}}
			if err := peripheralResultForTest(t, mtuResult); err != nil {
				t.Fatal(err)
			}
			want := uint16(attDefaultMTU)
			if serverMTU >= attDefaultMTU {
				want = min(uint16(64), serverMTU)
			}
			if p.mtu != want {
				t.Fatalf("effective MTU = %d, want %d", p.mtu, want)
			}
			read := make(chan peripheralReadResult, 1)
			go func() {
				value, err := p.ReadLongCharacteristic(&Characteristic{vh: 3})
				read <- peripheralReadResult{value, err}
			}()
			peripheralWriteForTest(t, c)
			c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'v'}}
			got := peripheralReadResultForTest(t, c, read)
			if got.err != nil || string(got.value) != "v" {
				t.Fatalf("short long-read = %q, %v", got.value, got.err)
			}
		})
	}
}

func TestPeripheralWriteUsesMTUWhenDequeued(t *testing.T) {
	for _, nextMTU := range []uint16{23, 64} {
		t.Run(fmt.Sprint(nextMTU), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := newPeripheralTestConn()
				p := newPeripheralForTest(c)
				if nextMTU == 23 {
					p.mtu = 64
				}
				startPeripheralForTest(t, p, c)
				mtuResult := make(chan error, 1)
				go func() { mtuResult <- p.SetMTU(nextMTU) }()
				peripheralWriteForTest(t, c)
				writeResult := make(chan error, 1)
				go func() {
					writeResult <- p.WriteCharacteristic(&Characteristic{vh: 3}, bytes.Repeat([]byte{'x'}, 40), false)
				}()
				synctest.Wait()
				c.reads <- peripheralTestRead{b: []byte{attOpMtuRsp, byte(nextMTU), byte(nextMTU >> 8)}}
				if err := peripheralResultForTest(t, mtuResult); err != nil {
					t.Fatal(err)
				}
				if nextMTU == 64 {
					packet := peripheralWriteForTest(t, c)
					if len(packet) != 43 {
						t.Fatalf("write PDU length = %d", len(packet))
					}
					c.reads <- peripheralTestRead{b: []byte{attOpWriteRsp}}
					if err := peripheralResultForTest(t, writeResult); err != nil {
						t.Fatal(err)
					}
				} else {
					select {
					case err := <-writeResult:
						if !errors.Is(err, ErrInvalidLength) {
							t.Fatalf("write after MTU shrink = %v", err)
						}
					case packet := <-c.writes:
						t.Fatalf("write exceeded negotiated MTU: %x", packet)
					}
				}
			})
		})
	}
}

func TestPeripheralATTTimeoutRetiresBearer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		done := startPeripheralForTest(t, p, c)
		result := make(chan error, 2)
		go func() { _, err := p.ReadCharacteristic(&Characteristic{vh: 3}); result <- err }()
		peripheralWriteForTest(t, c)
		go func() { _, err := p.ReadDescriptor(&Descriptor{h: 4}); result <- err }()
		synctest.Wait()
		time.Sleep(29 * time.Second)
		select {
		case err := <-result:
			t.Fatalf("transaction ended before timeout: %v", err)
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		for i := 0; i < 2; i++ {
			if err := peripheralResultForTest(t, result); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("request timeout = %v", err)
			}
		}
		<-done
		// A late response cannot satisfy another operation on the retired bearer.
		c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'x'}}
		if _, err := p.ReadCharacteristic(&Characteristic{vh: 3}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read on timed-out bearer = %v", err)
		}
		if len(c.writes) != 0 {
			t.Fatal("queued request reached timed-out transport")
		}
	})
}

func TestPeripheralATTTimeoutStartsAfterSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		releaseWrite := make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(releaseWrite) })
		c.writeResult = func(b []byte) (int, error) { <-releaseWrite; return len(b), nil }
		p := newPeripheralForTest(c)
		startPeripheralForTest(t, p, c)
		read := make(chan peripheralReadResult, 1)
		go func() {
			value, err := p.ReadCharacteristic(&Characteristic{vh: 3})
			read <- peripheralReadResult{value, err}
		}()
		peripheralWriteForTest(t, c)
		time.Sleep(31 * time.Second)
		if p.closedError() != nil {
			t.Fatal("local write waiting consumed the response timeout")
		}
		releaseOnce.Do(func() { close(releaseWrite) })
		synctest.Wait()
		time.Sleep(29 * time.Second)
		c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'v'}}
		got := peripheralReadResultForTest(t, c, read)
		if got.err != nil || string(got.value) != "v" {
			t.Fatalf("response within its transaction timeout = %q, %v", got.value, got.err)
		}
	})
}

func TestPeripheralQueuedRequestGetsOwnATTTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		startPeripheralForTest(t, p, c)
		first := make(chan peripheralReadResult, 1)
		go func() {
			value, err := p.ReadCharacteristic(&Characteristic{vh: 3})
			first <- peripheralReadResult{value, err}
		}()
		peripheralWriteForTest(t, c)
		second := make(chan error, 1)
		go func() { _, err := p.ReadDescriptor(&Descriptor{h: 4}); second <- err }()
		synctest.Wait()
		time.Sleep(29 * time.Second)
		c.reads <- peripheralTestRead{b: []byte{attOpReadRsp, 'v'}}
		select {
		case got := <-first:
			if got.err != nil {
				t.Fatal(got.err)
			}
		case <-time.After(time.Second):
			t.Fatal("first transaction did not finish")
		}
		peripheralWriteForTest(t, c)
		time.Sleep(29 * time.Second)
		select {
		case err := <-second:
			t.Fatalf("queued time consumed transaction timeout: %v", err)
		default:
		}
		time.Sleep(time.Second)
		if err := peripheralResultForTest(t, second); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second transaction error = %v", err)
		}
	})
}

func TestPeripheralWriteCommandHasNoATTResponseTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPeripheralTestConn()
		p := newPeripheralForTest(c)
		startPeripheralForTest(t, p, c)
		result := make(chan error, 1)
		go func() { result <- p.WriteCharacteristic(&Characteristic{vh: 3}, []byte{'v'}, true) }()
		peripheralWriteForTest(t, c)
		if err := peripheralResultForTest(t, result); err != nil {
			t.Fatal(err)
		}
		time.Sleep(31 * time.Second)
		if err := p.closedError(); err != nil {
			t.Fatalf("command started a response timeout: %v", err)
		}
	})
}
