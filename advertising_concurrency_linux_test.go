package gatt

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/seangong/gatt/v2/linux/cmd"
)

type advertisingTestCommand struct {
	op   int
	data []byte
}

type advertisingTestController struct {
	mu       sync.Mutex
	commands []advertisingTestCommand
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	failOp   int
	err      error
}

func (c *advertisingTestController) SendCmdWithAdvOff(cp cmd.CmdParam) error {
	b := make([]byte, cp.Len())
	cp.Marshal(b)
	c.mu.Lock()
	c.commands = append(c.commands, advertisingTestCommand{cp.Opcode(), b})
	c.mu.Unlock()
	if c.entered != nil {
		c.once.Do(func() { close(c.entered); <-c.release })
	}
	runtime.Gosched()
	if c.failOp == cp.Opcode() {
		return c.err
	}
	return nil
}

func (c *advertisingTestController) SetAdvertiseEnable(en bool) error {
	value := byte(0)
	if en {
		value = 1
	}
	c.mu.Lock()
	c.commands = append(c.commands, advertisingTestCommand{(cmd.LESetAdvertiseEnable{}).Opcode(), []byte{value}})
	c.mu.Unlock()
	return nil
}

func TestAdvertisingConfigurationsStayTogether(t *testing.T) {
	d, controller := &device{}, &advertisingTestController{}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				a, scan := nameAndServicesPackets(strings.Repeat(string(rune('a'+i)), 27), []UUID{UUID16(uint16(0xff00 + i))})
				if err := d.advertise(controller, a, scan); err != nil {
					t.Errorf("advertise: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if len(controller.commands) != 300 {
		t.Fatalf("commands = %d, want 100 complete configurations", len(controller.commands))
	}
	for i := 0; i < len(controller.commands); i += 3 {
		scan, adv, enable := controller.commands[i], controller.commands[i+1], controller.commands[i+2]
		if scan.op != (cmd.LESetScanResponseData{}).Opcode() || adv.op != (cmd.LESetAdvertisingData{}).Opcode() || enable.op != (cmd.LESetAdvertiseEnable{}).Opcode() || enable.data[0] != 1 {
			t.Fatalf("configuration commands interleaved at %d", i)
		}
		var decoded Advertisement
		data := append(append([]byte(nil), adv.data[1:1+int(adv.data[0])]...), scan.data[1:1+int(scan.data[0])]...)
		if err := decoded.unmarshall(data); err != nil {
			t.Fatal(err)
		}
		if len(decoded.LocalName) != 27 || len(decoded.Services) != 1 {
			t.Fatalf("incomplete configuration: %+v", decoded)
		}
		want := UUID16(0xff00 + uint16(decoded.LocalName[0]-'a'))
		if !decoded.Services[0].Equal(want) {
			t.Fatalf("name and services from different calls: %+v", decoded)
		}
	}
}

func TestAdvertisingStopFollowsCompleteConfiguration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &device{}
		controller := &advertisingTestController{entered: make(chan struct{}), release: make(chan struct{})}
		configured, stopped := make(chan error, 1), make(chan error, 1)
		a, scan := nameAndServicesPackets("box", nil)
		go func() { configured <- d.advertise(controller, a, scan) }()
		<-controller.entered
		go func() { stopped <- d.stopAdvertising(controller) }()
		synctest.Wait()
		close(controller.release)
		if err := <-configured; err != nil {
			t.Fatal(err)
		}
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		if len(controller.commands) != 4 || controller.commands[2].data[0] != 1 || controller.commands[3].data[0] != 0 {
			t.Fatalf("configuration and stop order = %+v", controller.commands)
		}
	})
}

func TestDeviceShutdownBypassesAdvertisingGate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		controller := &advertisingTestController{entered: make(chan struct{}), release: make(chan struct{}), failOp: (cmd.LESetScanResponseData{}).Opcode(), err: io.ErrClosedPipe}
		d := &device{closeHCI: func() error { close(controller.release); return nil }}
		d.initLifecycleLocked()
		active, queued := make(chan error, 1), make(chan error, 1)
		a, scan := nameAndServicesPackets("box", nil)
		go func() { active <- d.advertise(controller, a, scan) }()
		<-controller.entered
		go func() { queued <- d.stopAdvertising(controller) }()
		synctest.Wait()
		if err := d.StopContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-active; !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("active configuration after close = %v", err)
		}
		if err := <-queued; !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("queued stop after close = %v", err)
		}
		if len(controller.commands) != 1 {
			t.Fatalf("queued operation sent after shutdown: %+v", controller.commands)
		}
	})
}

func TestAdvertisingOptionsRetainConstructorAndReentryBehavior(t *testing.T) {
	d := &device{maxConn: 1}
	data := &cmd.LESetAdvertisingData{}
	if err := d.Option(LnxSetAdvertisingData(data), LnxMaxConnections(3), func(dev Device) error {
		return dev.Option(LnxSetScanResponseData(&cmd.LESetScanResponseData{}))
	}); err != nil {
		t.Fatal(err)
	}
	if d.ctx != nil || d.advData != data {
		t.Fatal("constructor options initialized lifecycle or lost staged data")
	}
	d.initLifecycleLocked()
	defer d.cancel()
	if cap(d.lifecycleSlots) != 6 {
		t.Fatal("advertising gate fixed capacity before later constructor options")
	}
	controller := &advertisingTestController{failOp: (cmd.LESetScanResponseData{}).Opcode(), err: errors.New("controller rejected response")}
	d.advParam = &cmd.LESetAdvertisingParameters{}
	a, scan := nameAndServicesPackets("box", nil)
	if err := d.advertise(controller, a, scan); err != controller.err {
		t.Fatalf("configuration error = %v", err)
	}
	if d.advParam != nil || d.scanResp != scan || d.advData == nil {
		t.Fatal("partial failure lost pending settings or restored an applied setting")
	}
	controller.err = nil
	if err := d.advertise(controller, a, nil); err != nil {
		t.Fatal(err)
	}
	if d.scanResp != nil || d.advData != nil {
		t.Fatal("successful retry left applied settings pending")
	}
}
