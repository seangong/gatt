package gatt

import (
	"runtime"
	"sync/atomic"
	"testing"
)

// Read the public collections while discovery publishes them. The objects in
// a published collection remain immutable throughout this test.
func readMetadataForTest(t *testing.T, s *Service) func() {
	t.Helper()
	ready, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var reads atomic.Uint64
	go func() {
		defer close(done)
		first := true
		for {
			chars := s.Characteristics()
			for _, c := range chars {
				for _, d := range c.Descriptors() {
					reads.Add(uint64(d.Handle()))
				}
				if d := c.Descriptor(); d != nil {
					reads.Add(uint64(d.Handle()))
				}
			}
			reads.Add(1)
			if first {
				close(ready)
				first = false
			}
			select {
			case <-stop:
				return
			default:
			}
			runtime.Gosched()
		}
	}()
	<-ready
	return func() { close(stop); <-done }
}
