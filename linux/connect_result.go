package linux

import "context"

// ConnectionResult is the final outcome of one outgoing connection attempt.
// Peripheral is the original argument on failure and the connected peripheral
// on success. A successful result always has a non-nil Peripheral.Conn.
type ConnectionResult struct {
	Peripheral *PlatData
	Err        error
}

// ConnectWithResult submits Create Connection synchronously, just like Connect,
// and returns after command submission completes. Its buffered channel receives
// exactly one outcome after connection setup succeeds or the attempt fails.
// Err is nil on success and context.Canceled for an explicitly canceled attempt
// or normal device shutdown. Other errors preserve connection or transport
// failures. HCI closes the channel after publishing its one result.
//
// This method delivers the outgoing connection through the result channel,
// rather than AcceptSlaveHandler. Connect retains its existing return value and
// AcceptSlaveHandler behavior. Receiving a result never runs a user callback in
// the HCI receiver or its shutdown workers.
func (h *HCI) ConnectWithResult(pd *PlatData) <-chan ConnectionResult {
	a, err := h.connect(pd, true)
	if a != nil {
		return a.result
	}
	result := make(chan ConnectionResult, 1)
	result <- ConnectionResult{Peripheral: pd, Err: err}
	close(result)
	return result
}

// finishResult is independent of done/terminalErr, which describe the physical
// outcome and cancellation cleanup. The caller holds HCI.connsmu.
func (a *connectionAttempt) finishResult(pd *PlatData, err error) bool {
	if a.resultDone {
		return false
	}
	if a.cancelStarted {
		err = context.Canceled
	}
	if pd == nil {
		pd = a.source
	}
	a.resultDone = true
	a.result <- ConnectionResult{Peripheral: pd, Err: err}
	close(a.result)
	return true
}

// finishConnect runs with HCI.connsmu held. The connection keeps its attempt
// only until setup publishes a result, so shutdown can resolve a setup in flight.
func (c *conn) finishConnect(err error) {
	if c.attempt != nil {
		c.attempt.finishResult(nil, err)
		c.attempt = nil
	}
}
