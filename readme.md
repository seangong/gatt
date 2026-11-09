# gatt

A Go implementation of Bluetooth Low Energy GATT for peripherals (servers) and
centrals (clients).

- **Peripheral:** define services, characteristics and descriptors; advertise;
  handle reads, writes and subscriptions.
- **Central:** scan, connect, discover attributes, read and write values, and
  subscribe to value changes.

Import path: `github.com/seangong/gatt/v2`.

## Platforms and compatibility

This README describes the current source tree. Older tagged releases may not
contain all the APIs and fixes documented here.

| Platform | Implementation | Important limits |
| --- | --- | --- |
| Linux | Direct HCI access; ATT/GATT handled by this package | Requires exclusive access to a compatible LE controller and suitable kernel, driver and firmware. |
| macOS | Legacy XPC integration with `com.apple.blued` | Uses private system interfaces; compatibility with a particular macOS release must be verified. Capabilities differ from Linux. |

Support for BLE alone does not guarantee interoperability with every controller
or remote device. Test the hardware, firmware, operating system, device role and
GATT operations used by your application. A working independent CoreBluetooth
client does not establish compatibility of this package's macOS backend.

Current limitations include:

- **Linux security:** no complete BLE pairing, bonding or key-management flow.
  Do not assume that devices or characteristics requiring an encrypted or
  authenticated link will work.
- **Linux advertising:** built-in paths use legacy advertising and scanning;
  extended and periodic advertising are not implemented.
- **Linux writes:** the server does not implement Read Multiple, Prepare Write,
  Execute Write or Signed Write. The client sends single ATT writes rather than
  implementing reliable long writes. A client write must fit both the current
  `MTU-3` limit and the 512-byte attribute limit; oversized values return an
  error without being sent or split.
- **Linux client APIs:** `DiscoverIncludedServices` is unimplemented and returns
  `nil, nil`; `ReadRSSI` returns `-1`. Scan and discovery UUID filters are not
  implemented, so filter returned results in your application.
- **Controller capacity:** Linux ACL transmit-buffer size and packet-count settings
  are fixed in the implementation rather than fully negotiated from capacity.
- **macOS APIs:** `DiscoverIncludedServices` and `SetMTU` are unimplemented;
  `SetIndicateValue` is a placeholder that returns `nil` without subscribing.
- **macOS server writes:** the existing write handler cannot implement atomic
  native batches. Batches with multiple writes and writes with a nonzero offset
  are rejected before any business handler executes. This also rejects batches
  of multiple writes without response that older code might have processed.
- **macOS server descriptors:** static descriptor values are supported; dynamic
  descriptor read or write handlers are rejected with `errors.ErrUnsupported`.
  The legacy backend has no verified registration path for those callbacks.
- **macOS server notifications:** notifications are not targeted to one central.
  The legacy backend sends an empty target list, described by its original
  implementation as broadcasting to all subscribed centrals. Do not assume
  per-central notification isolation; native behavior requires hardware testing.

The presence of an API, or a placeholder returning `nil`, is not evidence that
an operation is implemented. See the
[API reference](https://pkg.go.dev/github.com/seangong/gatt/v2) and
platform-specific source for individual operations.

## Installation

The current source requires **Go 1.26.4 or newer**, as declared in
[`go.mod`](go.mod). Run dependency commands from your application's module.

To use the development branch, then inspect and pin its resolved version:

```sh
go get github.com/seangong/gatt/v2@master
go list -m github.com/seangong/gatt/v2
```

Use the resolved commit or pseudo-version for reproducible builds. Branch
queries can be cached by module proxies; see the troubleshooting section below
and [Go version queries](https://go.dev/ref/mod#version-queries).

The earlier snapshot `v2.0.1-0.20261107051419-c03f1f502cb4` contains the shutdown,
read-status, CCCD, indication and advertising-initialization fixes, but predates
the advertising-parser, `AdvPacket.Err()` and client-MTU changes described here.
Unpublished local changes require an explicit local module replacement; fetching
`@master` only retrieves published commits.

For a published release:

```sh
go get github.com/seangong/gatt/v2@latest
```

`@latest` follows Go's release-version rules; it does not mean the latest commit
on `master`. In particular, `v2.0.0` does not contain the current `StopContext`
implementation or the subsequent Linux fixes.

```go
import "github.com/seangong/gatt/v2"
```

`NewDevice` and `Device.Option` apply options in order and stop at the first
error. Already applied options are not rolled back. `NewDevice` returns an
option error before opening the HCI or XPC connection; wrapped errors remain
identifiable with `errors.Is` and `errors.As`.

On Linux, `LnxSendHCIRawCommand` and `LnxSetAdvertisingEnable` require a device
returned by `NewDevice`; use them with `Device.Option`. Passing these runtime
options to `NewDevice` returns an error before opening the controller. Raw
command options also return response-writer errors; a short write matches
`io.ErrShortWrite`. A response-writer failure does not undo the HCI command.

## Linux setup

Use a kernel with HCI user-channel support, introduced in Linux 3.14, and a
working driver and firmware for your controller. UART controllers must already
be attached by the platform's Bluetooth initialization process.

The package normally binds `HCI_CHANNEL_USER` and takes exclusive control of the
selected adapter. It cannot share that adapter with BlueZ or another owner.
The implementation has a raw-channel fallback for older kernels; that does not
guarantee compatibility with those systems.

Initialization reads the controller's standard feature report after reset.
LE-only controllers skip the legacy BR/EDR configuration; dual-mode controllers
retain it. Required LE command failures still stop initialization. This does
not add controller-capacity negotiation or pairing support.

Inspect the adapters and select the one your application will own:

```sh
hciconfig
sudo hciconfig hci0 down
```

Replace `hci0` with the intended adapter. If the system Bluetooth service manages
it, stop that service before starting the application. For example, on a system
using systemd:

```sh
sudo systemctl stop bluetooth
```

On systems using the older service interface, use `sudo service bluetooth stop`
instead. Stopping the service affects the Bluetooth devices it manages.

Run the compiled application as root:

```sh
sudo ./gatt-server
```

Alternatively, on Linux filesystems supporting file capabilities:

```sh
sudo setcap 'cap_net_raw,cap_net_admin=eip' ./gatt-server
./gatt-server
```

Reapply capabilities after replacing the executable. In code,
`LnxDeviceID(0, true)` selects `hci0` and checks LE support; `-1` requests automatic
adapter selection.

## macOS setup

Build natively with CGO enabled and an Apple C toolchain. Choose the client or
server role through the macOS options shown in
[`examples/option/option_darwin.go`](examples/option/option_darwin.go).
The legacy XPC backend needs separate verification on your macOS release; the
Linux lifecycle and interoperability results do not apply to it automatically.

macOS devices expose an optional `Close() error` method outside the portable
`Device` interface. After a successful macOS `NewDevice`, use it at shutdown:

```go
if err := d.(interface{ Close() error }).Close(); err != nil {
	return err
}
```

`Close` is safe to repeat. It starts XPC cancellation and releases library
request waiters, without waiting for the native finalizer or application
callbacks to finish. Handlers should observe `Request.Context()` and
`Notifier.Done()` so they can exit on cancellation. A blocked handler that
ignores cancellation can delay native connection and handler-token release.
This is a different shutdown contract from Linux `StopContext`.

An XPC interruption ends the current GATT device session and cancels its pending
requests. macOS devices also expose optional `Done() <-chan struct{}` and
`Err() error` methods. Observe them when waiting for asynchronous operations
such as scanning, which have no error return:

```go
session := d.(interface {
	Done() <-chan struct{}
	Err() error
})
d.Scan(nil, false)
<-session.Done()
return session.Err()
```

`Err` is nil while the session is live. Once `Done` closes, `Err` reports the
first termination cause; later errors and repeated `Close` calls do not replace
it. Observers can subscribe before or after termination. `Done` closes on
explicit `Close` as well, and signals that the session is unavailable, not that
connection cleanup or native resource release has finished. Existing state
callbacks continue to report Bluetooth state changes.

Business writes are not automatically replayed. Create a new device with
`NewDevice` and reinitialize application state before continuing.

macOS client discovery stages and validates each response before updating the
cache. Unchanged attributes reuse their existing objects and subscriptions.
UUID-filtered queries preserve other known attributes and CCCDs; partial
responses are not treated as a complete database. A confirmed layout change
returns `ErrDatabaseChanged` and cancels only that peripheral connection. Wait
for its disconnection callback before reconnecting to the same device; other
connections remain usable.
Failed subscription changes preserve the previous callback.

On macOS, Connected and Disconnected callbacks run in order for each connected
owner. Notifications use a separate FIFO, so Connected can subscribe and wait
for a notification, and a notification callback can cancel and wait for
Disconnected. Native request responses stay outside both callback queues;
synchronous discovery, reads and subscription changes remain usable inside
callbacks. Different connections and reconnect generations are independent.

Termination discards notification callbacks that have not started. A callback
already running may finish after disconnection or Close. At most 16 notifications
may wait for one owner; overflowing that queue cancels that owner and reports
one Disconnected callback with the queue error. Client connection callback groups
are limited to 16 per device, with at most two workers per connected group.
Pending/rejected Connect error callbacks use the same group allowance. A group
remains occupied until its handlers actually return; exhaustion terminates the
device session and is observable through Done/Err. These are library resource
bounds, not controller capacity limits; they do not cover scan, state or server
notification handlers. Close does not wait for application handlers.

Repeated macOS `Connect` calls for the same UUID still send the native connection
command, but preserve the current pending or connected session and its requests,
cache and subscriptions. Duplicate success events do not publish another session
or success callback. A pending attempt keeps its first request identity.

On macOS, `RemoveAllServices` releases the local attribute registrations and
stops their notifiers after the removal command is queued locally. Old notifiers
then reject writes. Success does not confirm that the system has completed the
removal. `SetServices` applies services in order; if an add fails, services
already added by that call remain. An add failure releases only its own local
registrations. Notification handlers should observe `Notifier.Done()` when a
subscription or service is removed.

`AddService` rejects unsupported dynamic descriptors before sending native
commands or allocating local attribute IDs. `SetServices` checks the entire
replacement list for this local error before removing existing services. This
preflight does not make native service replacement atomic; native add failures
retain the ordered, partial-success behavior described above. Automatic CCCDs
and static descriptors retain their existing registration paths.

## Minimal Linux server

Save this as `main.go` in your application module after installing a snapshot
containing `StopContext`. It advertises one service with a read-only
characteristic and releases the controller on SIGINT or SIGTERM.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/seangong/gatt/v2"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() (err error) {
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	d, err := gatt.NewDevice(gatt.LnxDeviceID(0, true))
	if err != nil {
		return fmt.Errorf("open Bluetooth: %w", err)
	}
	stopper := d.(interface {
		StopContext(context.Context) error
	})
	defer func() {
		if stopErr := stopper.StopContext(context.Background()); stopErr != nil {
			err = errors.Join(err, fmt.Errorf("stop Bluetooth: %w", stopErr))
		}
	}()

	serviceUUID := gatt.MustParseUUID("9A3D0001-7F31-4EB2-9F66-14B489BDF001")
	valueUUID := gatt.MustParseUUID("9A3D0002-7F31-4EB2-9F66-14B489BDF001")
	service := gatt.NewService(serviceUUID)
	service.AddCharacteristic(valueUUID).SetValue([]byte("ready"))
	setup := make(chan error, 1)
	if err := d.Init(func(device gatt.Device, state gatt.State) {
		if state == gatt.StatePoweredOff {
			stopSignals()
			return
		}
		if state != gatt.StatePoweredOn {
			return
		}
		if err := device.AddService(service); err != nil {
			setup <- err
			return
		}
		setup <- device.AdvertiseNameAndServices("Gopher", []gatt.UUID{service.UUID()})
	}); err != nil {
		return fmt.Errorf("initialize Bluetooth: %w", err)
	}

	select {
	case err := <-setup:
		if err != nil {
			return fmt.Errorf("start advertising: %w", err)
		}
	case <-ctx.Done():
		return nil
	}
	log.Print("advertising Gopher; press Ctrl+C to stop")
	<-ctx.Done()
	return nil
}
```

```sh
go build -o gatt-server .
sudo ./gatt-server
```

Configure services and handlers before advertising. Legacy advertising and
scan-response packets each have a 31-byte budget, including field headers.
Advertise a discovery UUID that fits; clients can discover the remaining
services after connecting.

Linux `AddService` and `SetServices` support at most 65,535 attributes, including
service and characteristic declarations and descriptors. A larger table returns
an error matching `ErrInvalidLength` before changing the published table or
assigning handles.

`AppendUUIDFit` combines service UUIDs of the same width into one list. It
preserves input priority and includes only whole UUIDs. A `false` result means
some requested UUIDs were omitted; capacity exhaustion does not set `Err()`.
Completeness is reported separately for each UUID width. Repeated calls merge
with existing lists, and an incomplete list stays incomplete.

On Linux, automatic advertising recovery follows the last explicit enable or
stop request. Connections do not enable advertising for a client that has never
requested it, and a completed stop is not undone by a queued recovery task.
`AdvertiseNameAndServices` clears the previous scan response when its new name
fits in the advertising packet. Raw `Advertise` and custom scan-response options
retain their existing behavior.

Linux serializes complete advertising configurations and explicit advertising
stops. Built-in advertising options use the same operation gate; option lists
retain their ordered, partial-application behavior. Device shutdown cancels
queued operations and interrupts controller IO without waiting for this gate.

`AdvPacket` append methods retain their existing payload-truncation behavior.
If even a field's two-byte header cannot fit, the packet keeps its previous
contents and `Err()` reports `ErrEIRPacketTooLong`. `Advertise` returns that
error before changing advertising state or sending commands. Construct a new
packet after a build error. When using `Bytes()` with low-level advertising
options, check `Err()` first; `Bytes()` and `Len()` alone do not report errors.

Linux scanning preserves valid fields when another advertising field is
malformed. Padding and unparseable tails are removed separately from each
advertising or scan-response payload before they are combined.

The Linux scan cache distinguishes public and random address types, even when
their address bytes are identical. A scan response replaces the previous scan
response, including when empty; it is combined with the latest advertising
payload. A new advertising report starts a new pair. Scan and connection
callbacks receive independent data snapshots. Public MAC ID formatting is
unchanged.

The Linux scan cache holds at most 4096 addresses and evicts the least recently
used entry when full. Stopping scanning keeps this bounded cache; final shutdown
releases it. A pending connection retains an independent copy of its original
report, so cache eviction does not lose its original metadata or cancellation
identity. An orphan scan response for an evicted address must wait for a new
advertising report before it can be combined.

For scannable advertising, the current Linux discovery callback waits for the
scan response. Receiving only the advertising report does not produce a
discovery callback, including when the peer does not answer the scan request.
This existing callback timing is unchanged.

Service UUID lists, solicitation lists and Service Data support 16-, 32- and
128-bit UUIDs on the wire. Linux expands 32-bit UUIDs to their Bluetooth-base
128-bit form, so their strings can be parsed by the existing `ParseUUID` API.
Service Data entries retain their order and duplicates; an empty data payload
is valid. A field with an incomplete UUID is skipped without discarding other
valid fields.

## Linux reads, writes and subscriptions

Linux server Read By Type and Read By Group Type requests match a 16-bit
attribute type with its equivalent Bluetooth-base 128-bit UUID. Public
`UUID.Equal` keeps its existing comparison by length and bytes, as does the
attribute-value comparison in Find By Type Value requests.

Dynamic read handlers receive `ReadRequest.Cap` and `ReadRequest.Offset`. Return
the requested slice bounded by `Cap`; use `ResponseWriter.SetStatus` for an
invalid offset or another failure. The library does not apply the offset a
second time to a dynamic handler's response.

Read and write handlers use API statuses, not raw ATT error codes:

| API status | Linux ATT result |
| --- | --- |
| `StatusSuccess` | Success |
| `StatusInvalidOffset` | Invalid Offset (`0x07`) |
| `StatusUnexpectedError`, or an unknown status | Unlikely Error (`0x0E`) |

Write-without-response commands never receive an ATT response, including when a
handler rejects the write. Use an application-level acknowledgement when the
sender needs to know the business result.
Unsupported commands, including Signed Write, are discarded without a response.
Linux routes only L2CAP CID 4 into ATT; unsupported channels do not become ATT
responses. This does not add pairing or SMP support.

Linux connections keep the attribute registration accepted when they connected.
`AddService`, `SetServices` and `RemoveAllServices` affect new connections; existing
connections retain their handles, handlers and subscriptions until disconnected.
Public service objects report the newest registered handles. An empty service
table returns normal ATT errors rather than ending the connection. Configure
objects before registration and synchronize application-owned mutable state.

The default ATT MTU is 23. Linux accepts MTUs up to 517, while a single
notification or indication value is limited to `min(MTU - 3, 512)` bytes.
Linux client connections also start at MTU 23; an explicit `SetMTU` call is not
required before `ReadLongCharacteristic`. A successful MTU exchange takes
effect before the next ATT request, and each read uses its transaction's MTU
when determining whether more data remains.
`ReadDescriptor` also continues with Read Blob requests when its first response
fills the transaction's MTU. It returns the complete value or an error rather
than silently returning the first fragment. A matching Invalid Offset response
terminates descriptor continuation; characteristic long-read error handling is
unchanged. Long reads are a sequence of ATT transactions, not an atomic snapshot
of a value that the remote application changes during the read.
Read `Notifier.Cap()` after negotiation rather than assuming a fixed size.
Oversized writes return `0, io.ErrShortWrite` without sending a value. The library
does not split a value into multiple ATT notifications.

`HandleNotify` declares both notification and indication support. On Linux,
CCCD configuration is kept per connection:

| CCCD | Delivery mode |
| --- | --- |
| `0` | Unsubscribed; the notifier is stopped |
| `1` | Notification |
| `2` | Indication |
| `3` | Notification |

For indications, `Notifier.Write` returns after transmission, without waiting
for confirmation. Only one indication may await confirmation per connection,
across all characteristics. Another indication returns `ErrIndicationPending`;
the caller can retry later with appropriate pacing. Notifications remain
possible while an indication is pending. A 30-second confirmation timeout or
an uncertain indication write failure closes the connection.

Handlers should observe `Request.Context()` when doing blocking work.
Notification handlers should also check `Notifier.Done()` when unsubscribed.
A transport write already in progress may finish after `Done()` becomes true.

On macOS, server read and write handlers run in one bounded FIFO outside the
native XPC receiver, so they can call synchronous device methods. When the
queue is full, requests requiring a response return Insufficient Resources
without invoking a business handler. Close cancels queued work without waiting
for a handler that ignores cancellation.

On macOS, canceling the last notification subscription releases its central
registry entry. Other active characteristic subscriptions remain live, and a
later subscription retains the same central ID. Closing the device stops all
notifiers and releases the registry.

Linux client discovery publishes its cache only after a complete successful
discovery. Repeating discovery with the same attribute layout reuses the
existing objects, CCCD and subscriptions. A failed or incomplete discovery
leaves the previous cache intact. If a later complete discovery finds a changed
layout, it returns an error matching `errors.Is(err, gatt.ErrDatabaseChanged)`
and closes that connection. Reconnect and rediscover before using attribute
handles again. Discovery UUID filters remain unimplemented as listed above.

`Service.Characteristics`, `Characteristic.Descriptors` and
`Characteristic.Descriptor` may be read concurrently with discovery. Published
collections retain their existing object identities. Returned slices still
refer to the collection; callers must synchronize any direct mutations.
Configuration still belongs before registration. Use `Service` and
`Characteristic` through pointers; do not copy them after first use.

The Linux client supports local `SetMTU` values from 23 through 517. An invalid
local value is rejected without sending a request. A peer MTU below 23 leaves
the effective MTU at the default 23; a larger peer value is bounded by the local
request. These bounds describe this implementation's supported receive size.

Linux also validates received ATT PDU sizes against the effective MTU. A server
rejects an oversized request before calling business handlers and silently
discards oversized commands. An oversized client response retires that bearer
with an error matching `ErrInvalidLength` and releases queued operations.
Oversized notifications are discarded; oversized indications are confirmed and
discarded without delivering their values. MTU changes take effect in protocol
order so notifications following an exchange use the negotiated size. These
checks apply to each PDU and do not change cumulative long-read behavior.

After a complete ATT request is sent, the Linux client waits up to 30 seconds
for its response. Timeout errors match `context.DeadlineExceeded`; the bearer
is closed and queued operations are released. Establish a new connection before
retrying, and do not automatically replay business writes. Queueing, controller
credit waits and local transport writes precede this response timer, so it is
not a total API-call deadline. Writes without response have no response timer.
Responses received while no ATT request is awaiting a response are discarded
without blocking notifications or carrying a result into a future request.

## Linux shutdown and reopening

Linux devices expose optional `StopContext(context.Context) error` and
`Stop() error` methods. They are not part of the portable `Device` interface;
use a capability assertion as in the example above. Other platforms do not
provide the same shutdown contract.

An HCI Hardware Error event terminates the Linux session through the existing
fault shutdown path, releasing pending commands and connections. `Close`
reports the controller's hardware code. Recovery requires reopening the device;
this event does not trigger an automatic reset or replay of commands.

Asynchronous HCI termination also starts the device's shared shutdown, cancels
protocol work, and delivers `StatePoweredOff` after the existing core cleanup.
`Stop` or `StopContext` returns the original HCI failure once cleanup completes.
`Init` rejects an HCI session already terminating. Low-level `linux.HCI.Done()`
signals the start of shutdown; it does not mean resources have been released.
Use `Close` to wait for low-level cleanup. The portable `Device` interface is
unchanged.

Linux HCI commands remain serialized and also respect the controller's reported
command allowance. Sending waits while that allowance is zero. Shutdown rejects
unsent ordinary commands while keeping cleanup commands admissible; cleanup
still needs controller allowance and uses the shared shutdown deadline. If the
controller never grants it, shutdown follows the existing forced-close path.

Library-generated Disconnect commands retain their connection identity while
waiting for the command slot, controller allowance or socket writability. A
retired connection's unsent cleanup is canceled before submission, so a reused
handle does not target its replacement. Once fully submitted, Disconnect still
waits for its own command response even if physical disconnection arrives first.

A complete command write accompanied by an error returns the original error
without refunding its allowance, and retires the command processor. This prevents
a late response from completing a later command with the same opcode. Zero-byte
and partial failed writes retain their existing refund and continued-send
behavior; commands are not automatically retried.

- `StopContext` rejects new operations, cancels connection IO and starts one
  shared shutdown. Concurrent callers wait for the same cleanup and result.
- Graceful shutdown waits for physical disconnection events with a four-second
  grace budget. If it cannot finish, it closes the pollable HCI transport and
  joins the library's receiver and protocol workers. Forced timeout errors
  remain identifiable with `errors.Is(err, context.DeadlineExceeded)`.
- Canceling the caller's context only ends that caller's wait; shared cleanup
  continues. Allow cleanup to finish before exiting the process.
- Stopping advertising or removing services does not release the controller.
  After final shutdown, construct a new device to reopen it. `Init` is allowed
  once per device.
- Application callbacks run outside the core shutdown wait. Handlers that
  ignore cancellation may continue after shutdown returns; their concurrency
  is bounded until they actually return.
- Lifecycle callbacks remain ordered per connection. Server `CentralConnected`
  finishes before ATT handling starts; client `PeripheralConnected` may issue
  synchronous discovery requests. `Disconnected` and `PoweredOff` callbacks
  may finish after shutdown returns and may themselves request shutdown.

`Scan`, `Connect` and `CancelConnection` do not return errors through the portable
`Device` interface. Returning from a call is not a connection-success signal;
observe completion callbacks and use application-level timeouts where needed.

On Linux, `Central.ID()` reports the peer's MAC address in its normal displayed
byte order. Older revisions reversed that address; applications that persisted
those reversed IDs must correct their mappings. The same central object is
used for connected, request and disconnected callbacks.

For Linux outgoing connections, command rejection, connection failure and setup
failure are reported through `PeripheralConnected` with the original request
object and a non-nil error. That object does not represent a usable connection.
Explicit cancellation and normal shutdown suppress pending connection callbacks.
An admitted Connect's non-cancellation failure survives fault-driven device
shutdown, preserving the original request identity and error. An unreported
success interrupted by that fault reports failure rather than Disconnected.
Fault callbacks may start after core cleanup and can reenter Stop to retrieve
the same shutdown result. Joining a fault shutdown, including from PoweredOff,
does not turn it into an explicit cancellation; an explicit Stop that initiates
shutdown retains its callback-suppression behavior. Calls made after shutdown
starts are not admitted as new connection attempts.
Low-level HCI callers can use `ConnectWithResult` to receive one buffered result;
the existing HCI `Connect` return value and success handler remain unchanged.
Successful outgoing connections retain the name known when the attempt began
if later scan reports have no name. Continuing a scan does not erase that name.

## Repository examples

From this repository, build each example as a single file:

```sh
go build -o /tmp/gatt-example-server examples/server.go
go build -o /tmp/gatt-discoverer examples/discoverer.go
go build -o /tmp/gatt-explorer examples/explorer.go
```

On Linux, prepare the adapter and run with the permissions described above:

```sh
sudo /tmp/gatt-example-server
sudo /tmp/gatt-discoverer
sudo /tmp/gatt-explorer '<peripheral ID>'
```

These examples demonstrate individual APIs; use the shutdown and error handling
shown above when integrating them into an application. For Linux-specific
options, see [`examples/server_lnx.go`](examples/server_lnx.go) and
[`examples/option/option_linux.go`](examples/option/option_linux.go).

For an ARM64 Linux target:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/gatt-server-arm64 examples/server.go
```

Copy the executable to the target, prepare its controller, and run it there.
Building successfully does not establish hardware compatibility.

## Troubleshooting

### Advertising fails with HCI `0x200A` / status `0x0C`

`0x200A` controls both enabling and disabling advertising. Inspect the error
context (`enabled=false`, `enabled=true`, `configure` or `restore`) and capture
the HCI command and response with `btmon` where available.

Current initialization skips a redundant disable before the first advertising
configuration and checks configuration return statuses. Controllers can still
reject commands because of their state or parameters; the status code alone
does not identify the cause. Attribute-table logs show local service setup,
not successful advertising.

### Services or characteristic properties look out of date

Clients may cache GATT attributes across connections. If the service database
changes, follow the client's cache-invalidation procedure and rediscover the
attributes. Reconnecting alone may retain cached values. Confirm properties
and CCCD writes on the wire before diagnosing a subscription failure.

### `go get ...@master` still selects an older commit

Check the selected dependency, branch query and remote branch:

```sh
go list -m github.com/seangong/gatt/v2
go list -m -json github.com/seangong/gatt/v2@master
git ls-remote https://github.com/seangong/gatt.git refs/heads/master
```

A module proxy may return an older branch resolution. Pin the intended commit
or resolved pseudo-version. `GOPROXY=direct` bypasses the module proxy, but
requires repository and checksum-service connectivity. With a comma-separated
`GOPROXY` list, a successful proxy response does not trigger a direct lookup;
fallback occurs on HTTP 404 or 410. See
[Go's proxy protocol](https://go.dev/ref/mod#communicating-with-proxies).

Editing a separate local clone does not update an application's module-cache
dependency. Update its required version or explicitly configure a local
replacement before rebuilding.

## Development checks

Build the packages on the intended platform:

```sh
go build ./...
```

Run tests and static checks on the native platform. Race testing requires a
supported architecture and a C toolchain:

```sh
go test ./...
go test -race ./...
go vet ./...
```

The macOS XPC conversion tests exercise synchronous value conversion without
connecting to the Bluetooth service. Run the controlled native connection and
callback-lifecycle tests separately:

```sh
go test -race -tags gattxpc_test . ./xpc
GOEXPERIMENT=cgocheck2 go test -tags gattxpc_test . ./xpc
```

These tests use local XPC connections; they do not establish compatibility with
the private `com.apple.blued` interface on a particular macOS release. Keep
controller/peer interoperability testing separate from unit tests; record the
hardware, firmware, role and negotiated MTU.

## Background and license

gatt started as a Go port of [bleno](https://github.com/sandeepmistry/bleno).
Related BLE projects include [noble](https://github.com/sandeepmistry/noble).

Released under the [BSD-style license](LICENSE.md).
