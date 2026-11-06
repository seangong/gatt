package gatt

import (
	"bytes"
	"io"
	"testing"
)

func TestHandleReqRejectsInvalidHandleRanges(t *testing.T) {
	service := NewService(attrGAPUUID)
	service.AddCharacteristic(attrDeviceNameUUID).SetValue([]byte("Gopher"))
	c := newCentral(generateAttributes([]*Service{service}, 1), nil, nil)

	for _, operation := range []struct {
		name   string
		op     byte
		suffix []byte
	}{
		{"find information", attOpFindInfoReq, nil},
		{"find by type value", attOpFindByTypeValueReq, []byte{0x00, 0x28, 0x00, 0x18}},
		{"read by type", attOpReadByTypeReq, []byte{0x00, 0x28}},
		{"read by group", attOpReadByGroupReq, []byte{0x00, 0x28}},
	} {
		for _, handles := range []struct {
			name       string
			start, end uint16
		}{
			{"zero starting handle", 0, 3},
			{"reversed handles within the attribute table", 3, 1},
		} {
			t.Run(operation.name+"/"+handles.name, func(t *testing.T) {
				request := []byte{operation.op, byte(handles.start), byte(handles.start >> 8), byte(handles.end), byte(handles.end >> 8)}
				request = append(request, operation.suffix...)
				want := attErrorRsp(operation.op, handles.start, attEcodeInvalidHandle)
				if got := c.handleReq(request); !bytes.Equal(got, want) {
					t.Fatalf("handleReq(%x) = %x, want %x", request, got, want)
				}
			})
		}
	}
}

type attValidationConn struct {
	requests  [][]byte
	responses [][]byte
	closed    bool
}

func (c *attValidationConn) Read(p []byte) (int, error) {
	if len(c.requests) == 0 {
		return 0, io.EOF
	}
	request := c.requests[0]
	c.requests = c.requests[1:]
	if len(request) > len(p) {
		return 0, io.ErrShortBuffer
	}
	return copy(p, request), nil
}

func (c *attValidationConn) Write(p []byte) (int, error) {
	c.responses = append(c.responses, append([]byte(nil), p...))
	return len(p), nil
}

func (c *attValidationConn) Close() error {
	c.closed = true
	return nil
}

func TestCentralLoopContinuesAfterInvalidATTRequests(t *testing.T) {
	service := NewService(attrGAPUUID)
	characteristic := service.AddCharacteristic(attrDeviceNameUUID)
	var reads int
	var written [][]byte
	characteristic.HandleReadFunc(func(rsp ResponseWriter, _ *ReadRequest) {
		reads++
		if _, err := rsp.Write([]byte("ready")); err != nil {
			t.Errorf("write read response: %v", err)
		}
	})
	characteristic.HandleWriteFunc(func(_ Request, value []byte) byte {
		written = append(written, append([]byte(nil), value...))
		return StatusSuccess
	})
	attributes := generateAttributes([]*Service{service}, 1)
	low, high := byte(characteristic.vh), byte(characteristic.vh>>8)
	conn := &attValidationConn{requests: [][]byte{
		{attOpReadReq, low},
		{attOpWriteReq},
		{attOpWriteCmd, low},
		{attOpReadReq, low, high},
		{attOpWriteReq, low, high, 'n', 'e', 'x', 't'},
		{attOpWriteCmd, low, high, 'l', 'a', 's', 't'},
	}}

	newCentral(attributes, nil, conn).loop()

	wantResponses := [][]byte{
		attErrorRsp(attOpReadReq, 0, attEcodeInvalidPDU),
		attErrorRsp(attOpWriteReq, 0, attEcodeInvalidPDU),
		append([]byte{attOpReadRsp}, []byte("ready")...),
		{attOpWriteRsp},
	}
	if len(conn.responses) != len(wantResponses) {
		t.Fatalf("responses = %x, want %x", conn.responses, wantResponses)
	}
	for i, want := range wantResponses {
		if got := conn.responses[i]; !bytes.Equal(got, want) {
			t.Errorf("response %d = %x, want %x", i, got, want)
		}
	}
	if reads != 1 || len(written) != 2 || !bytes.Equal(written[0], []byte("next")) || !bytes.Equal(written[1], []byte("last")) {
		t.Errorf("application reads = %d, writes = %q, want 1 read and [next last]", reads, written)
	}
	if !conn.closed || len(conn.requests) != 0 {
		t.Errorf("loop finished with closed=%t, remaining requests=%d", conn.closed, len(conn.requests))
	}
}
