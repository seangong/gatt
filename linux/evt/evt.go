package evt

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/seangong/gatt/v2/linux/util"
)

type EventHandler interface {
	HandleEvent([]byte) error
}

type HandlerFunc func(b []byte) error

func (f HandlerFunc) HandleEvent(b []byte) error {
	return f(b)
}

type Evt struct {
	evtHandlers map[int]EventHandler
}

func NewEvt() *Evt {
	return &Evt{
		evtHandlers: map[int]EventHandler{},
	}
}

func (e *Evt) HandleEvent(c int, h EventHandler) {
	e.evtHandlers[c] = h
}

func (e *Evt) Dispatch(b []byte) error {
	h := &EventHeader{}
	if err := h.Unmarshal(b); err != nil {
		return err
	}
	b = b[2:] // Skip Event Header (uint8 + uint8)
	if f, found := e.evtHandlers[h.code]; found {
		e.trace("> HCI Event: %s (0x%02X) plen %d: [ % X ])\n", h.code, uint8(h.code), h.plen, b)
		return f.HandleEvent(b)
	}
	e.trace("> HCI Event: no handler for %s (0x%02X)\n", h.code, uint8(h.code))
	return nil
}

func (e *Evt) trace(fmt string, v ...interface{}) {}

const (
	InquiryComplete                              = 0x01 // Inquiry Complete
	InquiryResult                                = 0x02 // Inquiry Result
	ConnectionComplete                           = 0x03 // Connection Complete
	ConnectionRequest                            = 0x04 // Connection Request
	DisconnectionComplete                        = 0x05 // Disconnection Complete
	AuthenticationComplete                       = 0x06 // Authentication
	RemoteNameReqComplete                        = 0x07 // Remote Name Request Complete
	EncryptionChange                             = 0x08 // Encryption Change
	ChangeConnectionLinkKeyComplete              = 0x09 // Change Conection Link Key Complete
	MasterLinkKeyComplete                        = 0x0A // Master Link Keye Complete
	ReadRemoteSupportedFeaturesComplete          = 0x0B // Read Remote Supported Features Complete
	ReadRemoteVersionInformationComplete         = 0x0C // Read Remote Version Information Complete
	QoSSetupComplete                             = 0x0D // QoSSetupComplete
	CommandComplete                              = 0x0E // Command Complete
	CommandStatus                                = 0x0F // Command status
	HardwareError                                = 0x10 // Hardware Error
	FlushOccurred                                = 0x11 // Flush Occured
	RoleChange                                   = 0x12 // Role Change
	NumberOfCompletedPkts                        = 0x13 // Number Of Completed Packets
	ModeChange                                   = 0x14 // Mode Change
	ReturnLinkKeys                               = 0x15 // Return Link Keys
	PinCodeRequest                               = 0x16 // PIN Code Request
	LinkKeyRequest                               = 0x17 // Link Key Request
	LinkKeyNotification                          = 0x18 // Link Key Notification
	LoopbackCommand                              = 0x19 // Loopback Command
	DataBufferOverflow                           = 0x1A // Data Buffer Overflow
	MaxSlotsChange                               = 0x1B // Max Slots Change
	ReadClockOffsetComplete                      = 0x1C // Read Clock Offset Complete
	ConnectionPtypeChanged                       = 0x1D // Connection Packet Type Changed
	QoSViolation                                 = 0x1E // QoS Violation
	PageScanRepetitionModeChange                 = 0x20 // Page Scan Repetition Mode Change
	FlowSpecificationComplete                    = 0x21 // Flow Specification
	InquiryResultWithRssi                        = 0x22 // Inquery Result with RSSI
	ReadRemoteExtendedFeaturesComplete           = 0x23 // Read Remote Extended Features Complete
	SyncConnectionComplete                       = 0x2C // Synchronous Connection Complete
	SyncConnectionChanged                        = 0x2D // Synchronous Connection Changed
	SniffSubrating                               = 0x2E // Sniff Subrating
	ExtendedInquiryResult                        = 0x2F // Extended Inquiry Result
	EncryptionKeyRefreshComplete                 = 0x30 // Encryption Key Refresh Complete
	IOCapabilityRequest                          = 0x31 // IO Capability Request
	IOCapabilityResponse                         = 0x32 // IO Capability Changed
	UserConfirmationRequest                      = 0x33 // User Confirmation Request
	UserPasskeyRequest                           = 0x34 // User Passkey Request
	RemoteOOBDataRequest                         = 0x35 // Remote OOB Data
	SimplePairingComplete                        = 0x36 // Simple Pairing Complete
	LinkSupervisionTimeoutChanged                = 0x38 // Link Supervision Timeout Changed
	EnhancedFlushComplete                        = 0x39 // Enhanced Flush Complete
	UserPasskeyNotify                            = 0x3B // User Passkey Notification
	KeypressNotify                               = 0x3C // Keypass Notification
	RemoteHostFeaturesNotify                     = 0x3D // Remote Host Supported Features Notification
	LEMeta                                       = 0x3E // LE Meta
	PhysicalLinkComplete                         = 0x40 // Physical Link Complete
	ChannelSelected                              = 0x41 // Channel Selected
	DisconnectionPhysicalLinkComplete            = 0x42 // Disconnection Physical Link Complete
	PhysicalLinkLossEarlyWarning                 = 0x43 // Physical Link Loss Early Warning
	PhysicalLinkRecovery                         = 0x44 // Physical Link Recovery
	LogicalLinkComplete                          = 0x45 // Logical Link Complete
	DisconnectionLogicalLinkComplete             = 0x46 // Disconnection Logical Link Complete
	FlowSpecModifyComplete                       = 0x47 // Flow Spec Modify Complete
	NumberOfCompletedBlocks                      = 0x48 // Number Of Completed Data Blocks
	AMPStartTest                                 = 0x49 // AMP Start Test
	AMPTestEnd                                   = 0x4A // AMP Test End
	AMPReceiverReport                            = 0x4b // AMP Receiver Report
	AMPStatusChange                              = 0x4D // AMP status Change
	TriggeredClockCapture                        = 0x4e // Triggered Clock Capture
	SynchronizationTrainComplete                 = 0x4F // Synchronization Train Complete
	SynchronizationTrainReceived                 = 0x50 // Synchronization Train Received
	ConnectionlessSlaveBroadcastReceive          = 0x51 // Connectionless Slave Broadcast Receive
	ConnectionlessSlaveBroadcastTimeout          = 0x52 // Connectionless Slave Broadcast Timeout
	TruncatedPageComplete                        = 0x53 // Truncated Page Complete
	SlavePageResponseTimeout                     = 0x54 // Slave Page Response Timeout
	ConnectionlessSlaveBroadcastChannelMapChange = 0x55 // Connectionless Slave Broadcast Channel Map Change
	InquiryResponseNotification                  = 0x56 // Inquiry Response Notification
	AuthenticatedPayloadTimeoutExpired           = 0x57 // Authenticated Payload Timeout Expired
)

type LEEventCode int

const (
	LEConnectionComplete               LEEventCode = 0x01 // LE Connection Complete
	LEAdvertisingReport                            = 0x02 // LE Advertising Report
	LEConnectionUpdateComplete                     = 0x03 // LE Connection Update Complete
	LEReadRemoteUsedFeaturesComplete               = 0x04 // LE Read Remote Used Features Complete
	LELTKRequest                                   = 0x05 // LE LTK Request
	LERemoteConnectionParameterRequest             = 0x06 // LE Remote Connection Parameter Request
)

type EventHeader struct {
	code int
	plen uint8
}

func (h *EventHeader) Unmarshal(b []byte) error {
	if len(b) < 2 {
		return errors.New("malformed header")
	}
	h.code = int(b[0])
	h.plen = b[1]
	if len(b) != 2+int(h.plen) {
		return errors.New("wrong length")
	}
	return nil
}

var o = util.Order

// Event Parameters

type InquiryCompleteEP struct {
	Status uint8
}

type InquiryResultEP struct {
	NumResponses           uint8
	BDAddr                 [][6]byte
	PageScanRepetitionMode []uint8
	Reserved1              []byte
	Reserved2              []byte
	ClassOfDevice          [][3]byte
	ClockOffset            []uint16
}

type ConnectionCompleteEP struct {
	Status            uint8
	ConnectionHandle  uint16
	BDAddr            [6]byte
	LinkType          uint8
	EncryptionEnabled uint8
}

type ConnectionRequestEP struct {
	BDAddr        [6]byte
	ClassofDevice [3]byte
	LinkType      uint8
}

type DisconnectionCompleteEP struct {
	Status           uint8
	ConnectionHandle uint16
	Reason           uint8
}

func (e *DisconnectionCompleteEP) Unmarshal(b []byte) error {
	if len(b) != 4 {
		return errors.New("invalid disconnection-complete event length")
	}
	*e = DisconnectionCompleteEP{Status: b[0], ConnectionHandle: o.Uint16(b[1:3]), Reason: b[3]}
	return nil
}

type CommandCompleteEP struct {
	NumHCICommandPackets uint8
	CommandOPCode        uint16
	ReturnParameters     []byte
}

func (e *CommandCompleteEP) Unmarshal(b []byte) error {
	buf := bytes.NewBuffer(b)
	if err := binary.Read(buf, binary.LittleEndian, &e.NumHCICommandPackets); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &e.CommandOPCode); err != nil {
		return err
	}
	e.ReturnParameters = buf.Bytes()
	return nil
}

type CommandStatusEP struct {
	Status               uint8
	NumHCICommandPackets uint8
	CommandOpcode        uint16
}

func (e *CommandStatusEP) Unmarshal(b []byte) error {
	if len(b) != 4 {
		return errors.New("invalid command-status event length")
	}
	*e = CommandStatusEP{Status: b[0], NumHCICommandPackets: b[1], CommandOpcode: o.Uint16(b[2:4])}
	return nil
}

type NumOfCompletedPkt struct {
	ConnectionHandle   uint16
	NumOfCompletedPkts uint16
}

type NumberOfCompletedPktsEP struct {
	NumberOfHandles uint8
	Packets         []NumOfCompletedPkt
}

func (e *NumberOfCompletedPktsEP) Unmarshal(b []byte) error {
	if len(b) < 1 || len(b) != 1+4*int(b[0]) {
		return errors.New("invalid completed-packets event length")
	}
	n := int(b[0])
	packets := make([]NumOfCompletedPkt, n)
	for i := range packets {
		entry := b[1+4*i : 1+4*(i+1)]
		packets[i] = NumOfCompletedPkt{
			ConnectionHandle:   binary.LittleEndian.Uint16(entry[:2]) & 0x0fff,
			NumOfCompletedPkts: binary.LittleEndian.Uint16(entry[2:]),
		}
	}
	*e = NumberOfCompletedPktsEP{NumberOfHandles: b[0], Packets: packets}
	return nil
}

// LE Meta Subevents
type LEConnectionCompleteEP struct {
	SubeventCode        uint8
	Status              uint8
	ConnectionHandle    uint16
	Role                uint8
	PeerAddressType     uint8
	PeerAddress         [6]byte
	ConnInterval        uint16
	ConnLatency         uint16
	SupervisionTimeout  uint16
	MasterClockAccuracy uint8
}

func (e *LEConnectionCompleteEP) Unmarshal(b []byte) error {
	if len(b) != 19 {
		return errors.New("invalid LE connection-complete event length")
	}
	*e = LEConnectionCompleteEP{
		SubeventCode: b[0], Status: b[1], ConnectionHandle: o.Uint16(b[2:4]),
		Role: b[4], PeerAddressType: b[5], PeerAddress: o.MAC(b[6:12]),
		ConnInterval: o.Uint16(b[12:14]), ConnLatency: o.Uint16(b[14:16]),
		SupervisionTimeout: o.Uint16(b[16:18]), MasterClockAccuracy: b[18],
	}
	return nil
}

type LEAdvertisingReportEP struct {
	SubeventCode uint8
	NumReports   uint8
	EventType    []uint8
	AddressType  []uint8
	Address      [][6]byte
	Length       []uint8
	Data         [][]byte
	RSSI         []int8
}

func (e *LEAdvertisingReportEP) Unmarshal(b []byte) error {
	if len(b) < 2 {
		return errors.New("invalid LE advertising report header")
	}
	n := int(b[1])
	report := LEAdvertisingReportEP{SubeventCode: b[0], NumReports: b[1]}
	b = b[2:]
	// Each report needs event/address types, an address, data length and RSSI.
	if len(b) < n*10 {
		return errors.New("invalid LE advertising report length")
	}
	report.EventType = make([]uint8, n)
	report.AddressType = make([]uint8, n)
	report.Address = make([][6]byte, n)
	report.Length = make([]uint8, n)
	report.Data = make([][]byte, n)
	report.RSSI = make([]int8, n)

	for i := 0; i < n; i++ {
		if len(b) < 10 {
			return errors.New("invalid LE advertising report length")
		}
		report.EventType[i] = b[0]
		report.AddressType[i] = b[1]
		report.Address[i] = o.MAC(b[2:8])
		report.Length[i] = b[8]
		length := int(report.Length[i])
		if len(b) < 10+length {
			return errors.New("invalid LE advertising report data length")
		}
		report.Data[i] = make([]byte, length)
		copy(report.Data[i], b[9:9+length])
		report.RSSI[i] = o.Int8(b[9+length:])
		b = b[10+length:]
	}
	*e = report
	return nil
}

type LEConnectionUpdateCompleteEP struct {
	SubeventCode       uint8
	Status             uint8
	ConnectionHandle   uint16
	ConnInterval       uint16
	ConnLatency        uint16
	SupervisionTimeout uint16
}

func (e *LEConnectionUpdateCompleteEP) Unmarshal(b []byte) error {
	return binary.Read(bytes.NewBuffer(b), binary.LittleEndian, e)
}

type LEReadRemoteUsedFeaturesCompleteEP struct {
	SubeventCode     uint8
	Status           uint8
	ConnectionHandle uint16
	LEFeatures       uint64
}

func (e *LEReadRemoteUsedFeaturesCompleteEP) Unmarshal(b []byte) error {
	return binary.Read(bytes.NewBuffer(b), binary.LittleEndian, e)
}

type LELTKRequestEP struct {
	SubeventCode          uint8
	ConnectionHandle      uint16
	RandomNumber          uint64
	EncryptionDiversifier uint16
}

func (e *LELTKRequestEP) Unmarshal(b []byte) error {
	return binary.Read(bytes.NewBuffer(b), binary.LittleEndian, e)
}

type LERemoteConnectionParameterRequestEP struct {
	SubeventCode     uint8
	ConnectionHandle uint16
	IntervalMin      uint16
	IntervalMax      uint16
	Latency          uint16
	Timeout          uint16
}

func (e *LERemoteConnectionParameterRequestEP) Unmarshal(b []byte) error {
	return binary.Read(bytes.NewBuffer(b), binary.LittleEndian, e)
}
