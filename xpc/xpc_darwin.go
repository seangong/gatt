package xpc

/*
#include "xpc_wrapper_darwin.h"
#include "sys/utsname.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"log"
	r "reflect"
	"runtime/cgo"
	"sync"
	"unsafe"
)

// XPC values share ownership of a connection. Call Close when it is no longer
// needed; a Go finalizer cannot collect a handler retained by the native peer.
type XPC struct {
	state *connectionState
}

type connectionState struct {
	mu       sync.Mutex
	conn     C.xpc_connection_t
	err      error
	done     chan struct{}
	released chan struct{} // Closed only after the last native callback finishes.
	handler  XpcEventHandler
}

// ErrClosed reports an explicitly closed or uninitialized connection.
var ErrClosed = errors.New("xpc connection closed")

// Send preserves the original fire-and-forget API. Use SendError to observe a
// closed connection instead of silently discarding the send attempt.
func (x *XPC) Send(msg interface{}, verbose bool) {
	_ = x.SendError(msg, verbose)
}

// SendError enqueues a message or reports that the connection is closed. A nil
// error means it was queued locally, not that the service has processed it.
func (x *XPC) SendError(msg interface{}, verbose bool) error {
	if x == nil || x.state == nil {
		return ErrClosed
	}
	s := x.state
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return err
	}
	conn := s.conn
	C.xpc_retain(C.xpc_object_t(unsafe.Pointer(conn)))
	s.mu.Unlock()
	defer C.xpc_release(C.xpc_object_t(unsafe.Pointer(conn)))
	// verbose == true converts the type from bool to C._Bool
	C.XpcSendMessage(conn, goToXpc(msg), true, verbose == true)
	return nil
}

// Close initiates asynchronous cancellation. It is safe to call from a handler
// and does not wait for a running handler to return.
func (x *XPC) Close() error {
	if x != nil && x.state != nil {
		x.state.close(ErrClosed, true)
	}
	return nil
}

// Done is closed as soon as cancellation starts or the connection is invalid.
// It does not indicate that native callbacks and resource release have finished.
func (x *XPC) Done() <-chan struct{} {
	if x == nil || x.state == nil {
		return closedConnection
	}
	return x.state.done
}

// Err returns the first terminal error, or nil while the connection is live.
func (x *XPC) Err() error {
	if x == nil || x.state == nil {
		return ErrClosed
	}
	x.state.mu.Lock()
	defer x.state.mu.Unlock()
	return x.state.err
}

var closedConnection = func() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

func (s *connectionState) close(err error, cancel bool) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.err = err
	conn := s.conn
	s.conn = nil
	close(s.done)
	s.mu.Unlock()
	if conn != nil {
		if cancel {
			C.xpc_connection_cancel(conn)
		}
		C.xpc_release(C.xpc_object_t(unsafe.Pointer(conn)))
	}
}

//
// minimal XPC support required for BLE
//

// a dictionary of things
type Dict map[string]interface{}

func (d Dict) Contains(k string) bool {
	_, ok := d[k]
	return ok
}

func (d Dict) MustGetDict(k string) Dict {
	return d[k].(Dict)
}

func (d Dict) MustGetArray(k string) Array {
	return d[k].(Array)
}

func (d Dict) MustGetBytes(k string) []byte {
	return d[k].([]byte)
}

func (d Dict) MustGetHexBytes(k string) string {
	return fmt.Sprintf("%x", d[k].([]byte))
}

func (d Dict) MustGetInt(k string) int {
	return int(d[k].(int64))
}

func (d Dict) MustGetUUID(k string) []byte {
	u := d[k].(UUID)
	return u[:]
}

func (d Dict) GetString(k, defv string) string {
	if v := d[k]; v != nil {
		//log.Printf("GetString %s %#v\n", k, v)
		return v.(string)
	} else {
		//log.Printf("GetString %s default %#v\n", k, defv)
		return defv
	}
}

func (d Dict) GetBytes(k string, defv []byte) []byte {
	if v := d[k]; v != nil {
		//log.Printf("GetBytes %s %#v\n", k, v)
		return v.([]byte)
	} else {
		//log.Printf("GetBytes %s default %#v\n", k, defv)
		return defv
	}
}

func (d Dict) GetInt(k string, defv int) int {
	if v := d[k]; v != nil {
		//log.Printf("GetString %s %#v\n", k, v)
		return int(v.(int64))
	} else {
		//log.Printf("GetString %s default %#v\n", k, defv)
		return defv
	}
}

func (d Dict) GetUUID(k string) UUID {
	return GetUUID(d[k])
}

// an array of things
type Array []interface{}

func (a Array) GetUUID(k int) UUID {
	return GetUUID(a[k])
}

// a UUID
type UUID []byte

func MakeUUID(s string) UUID {
	var sl []byte
	fmt.Sscanf(s, "%32x", &sl)

	var uuid [16]byte
	copy(uuid[:], sl)
	return UUID(uuid[:])
}

func (uuid UUID) String() string {
	return fmt.Sprintf("%x", []byte(uuid))
}

func GetUUID(v interface{}) UUID {
	if v == nil {
		return UUID{}
	}

	if uuid, ok := v.(UUID); ok {
		return uuid
	}

	if bytes, ok := v.([]byte); ok {
		uuid := make(UUID, len(bytes))
		copy(uuid, bytes)
		return uuid
	}

	log.Fatalf("invalid type for UUID: %#v", v)
	return UUID{}
}

var (
	CONNECTION_INVALID     = errors.New("connection invalid")
	CONNECTION_INTERRUPTED = errors.New("connection interrupted")
	CONNECTION_TERMINATED  = errors.New("connection terminated")

	TYPE_OF_UUID  = r.TypeOf(UUID{})
	TYPE_OF_BYTES = r.TypeOf([]byte{})
)

type XpcEventHandler interface {
	HandleXpcEvent(event Dict, err error)
}

// XpcConnect creates and activates a connection. Err reports creation failures;
// subsequent events are asynchronous and can begin before this function returns.
func XpcConnect(service string, eh XpcEventHandler) XPC {
	s, handle := newConnectionState(eh)
	cservice := C.CString(service)
	defer C.free(unsafe.Pointer(cservice))
	return activateConnection(s, handle, C.XpcConnect(cservice, C.uintptr_t(handle)))
}

func newConnectionState(eh XpcEventHandler) (*connectionState, cgo.Handle) {
	s := &connectionState{
		done:     make(chan struct{}),
		released: make(chan struct{}),
		handler:  eh,
	}
	return s, cgo.NewHandle(s)
}

func activateConnection(s *connectionState, handle cgo.Handle, conn C.xpc_connection_t) XPC {
	x := XPC{state: s}
	if conn == nil {
		s.close(fmt.Errorf("create xpc connection: %w", CONNECTION_INVALID), false)
		handle.Delete() // Ownership was never handed to a native connection.
		close(s.released)
		return x
	}
	s.mu.Lock()
	s.conn = conn
	// Activation may immediately deliver INVALID. Keep the connection alive even
	// if that callback releases the state's main reference before resume returns.
	C.xpc_retain(C.xpc_object_t(unsafe.Pointer(conn)))
	s.mu.Unlock()
	C.xpc_connection_resume(conn)
	C.xpc_release(C.xpc_object_t(unsafe.Pointer(conn)))
	return x
}

//export handleXpcEvent
func handleXpcEvent(event C.xpc_object_t, token C.uintptr_t) {
	s := cgo.Handle(token).Value().(*connectionState)
	t := C.xpc_get_type(event)
	var err error
	if t == C.TYPE_ERROR {
		if event == C.ERROR_CONNECTION_INVALID {
			err = CONNECTION_INVALID
			s.close(err, false)
		} else if event == C.ERROR_CONNECTION_INTERRUPTED {
			err = CONNECTION_INTERRUPTED
		} else if event == C.ERROR_CONNECTION_TERMINATED {
			err = CONNECTION_TERMINATED
			s.close(err, true)
		} else {
			err = fmt.Errorf("%v", event)
		}
		if s.handler != nil {
			s.handler.HandleXpcEvent(nil, err)
		}
		return
	}
	if s.handler != nil {
		s.handler.HandleXpcEvent(xpcToGo(event).(Dict), nil)
	}
}

//export releaseXpcHandle
func releaseXpcHandle(token C.uintptr_t) {
	handle := cgo.Handle(token)
	s := handle.Value().(*connectionState)
	handle.Delete()
	close(s.released)
}

// goToXpc converts a go object to an xpc object
func goToXpc(o interface{}) C.xpc_object_t {
	return valueToXpc(r.ValueOf(o))
}

// valueToXpc converts a go Value to an xpc object
//
// note that not all the types are supported, but only the subset required for Blued
func valueToXpc(val r.Value) C.xpc_object_t {
	if !val.IsValid() {
		return nil
	}

	var xv C.xpc_object_t

	switch val.Kind() {
	case r.Int, r.Int8, r.Int16, r.Int32, r.Int64:
		xv = C.xpc_int64_create(C.int64_t(val.Int()))

	case r.Uint, r.Uint8, r.Uint16, r.Uint32:
		xv = C.xpc_int64_create(C.int64_t(val.Uint()))

	case r.String:
		xv = C.xpc_string_create(C.CString(val.String()))

	case r.Map:
		xv = C.xpc_dictionary_create(nil, nil, 0)
		for _, k := range val.MapKeys() {
			v := valueToXpc(val.MapIndex(k))
			C.xpc_dictionary_set_value(xv, C.CString(k.String()), v)
			if v != nil {
				C.xpc_release(v)
			}
		}

	case r.Array, r.Slice:
		if val.Type() == TYPE_OF_UUID {
			// array of bytes
			var uuid [16]byte
			r.Copy(r.ValueOf(uuid[:]), val)
			xv = C.xpc_uuid_create(C.ptr_to_uuid(unsafe.Pointer(&uuid[0])))
		} else if val.Type() == TYPE_OF_BYTES {
			// slice of bytes
			xv = C.xpc_data_create(unsafe.Pointer(val.Pointer()), C.size_t(val.Len()))
		} else {
			xv = C.xpc_array_create(nil, 0)
			l := val.Len()

			for i := 0; i < l; i++ {
				v := valueToXpc(val.Index(i))
				C.xpc_array_append_value(xv, v)
				if v != nil {
					C.xpc_release(v)
				}
			}
		}

	case r.Interface, r.Ptr:
		xv = valueToXpc(val.Elem())

	default:
		log.Fatalf("unsupported %#v", val.String())
	}

	return xv
}

//export arraySet
func arraySet(u C.uintptr_t, i C.int, v C.xpc_object_t) {
	a := cgo.Handle(u).Value().(Array)
	a[i] = xpcToGo(v)
}

//export dictSet
func dictSet(u C.uintptr_t, k *C.char, v C.xpc_object_t) {
	d := cgo.Handle(u).Value().(Dict)
	d[C.GoString(k)] = xpcToGo(v)
}

// xpcToGo converts an xpc object to a go object
//
// note that not all the types are supported, but only the subset required for Blued
func xpcToGo(v C.xpc_object_t) interface{} {
	t := C.xpc_get_type(v)

	switch t {
	case C.TYPE_ARRAY:
		a := make(Array, C.int(C.xpc_array_get_count(v)))
		h := cgo.NewHandle(a)
		defer h.Delete()
		C.XpcArrayApply(C.uintptr_t(h), v)
		return a

	case C.TYPE_DATA:
		return C.GoBytes(C.xpc_data_get_bytes_ptr(v), C.int(C.xpc_data_get_length(v)))

	case C.TYPE_DICT:
		d := make(Dict)
		h := cgo.NewHandle(d)
		defer h.Delete()
		C.XpcDictApply(C.uintptr_t(h), v)
		return d

	case C.TYPE_INT64:
		return int64(C.xpc_int64_get_value(v))

	case C.TYPE_STRING:
		return C.GoString(C.xpc_string_get_string_ptr(v))

	case C.TYPE_UUID:
		a := [16]byte{}
		C.XpcUUIDGetBytes(unsafe.Pointer(&a), v)
		return UUID(a[:])

	default:
		log.Fatalf("unexpected type %#v, value %#v", t, v)
	}

	return nil
}

// xpc_release is needed by tests, since they can't use CGO
func xpc_release(xv C.xpc_object_t) {
	C.xpc_release(xv)
}

// this is used to check the OS version

type Utsname struct {
	Sysname  string
	Nodename string
	Release  string
	Version  string
	Machine  string
}

func Uname(utsname *Utsname) error {
	var cstruct C.struct_utsname
	if err := C.uname(&cstruct); err != 0 {
		return errors.New("utsname error")
	}

	// XXX: this may crash if any value is exactly 256 characters (no 0 terminator)
	utsname.Sysname = C.GoString(&cstruct.sysname[0])
	utsname.Nodename = C.GoString(&cstruct.nodename[0])
	utsname.Release = C.GoString(&cstruct.release[0])
	utsname.Version = C.GoString(&cstruct.version[0])
	utsname.Machine = C.GoString(&cstruct.machine[0])

	return nil
}
