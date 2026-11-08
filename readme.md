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
  implementing reliable long writes.
- **Linux client APIs:** `DiscoverIncludedServices` is unimplemented and returns
  `nil, nil`; `ReadRSSI` returns `-1`. Scan and discovery UUID filters are not
  implemented, so filter returned results in your application.
- **Controller capacity:** Linux transmit-buffer size and packet-count settings
  are fixed in the implementation rather than fully negotiated from capacity.
- **macOS APIs:** `DiscoverIncludedServices` and `SetMTU` are unimplemented;
  `SetIndicateValue` is a placeholder that returns `nil` without subscribing.

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

## Linux setup

Use a kernel with HCI user-channel support, introduced in Linux 3.14, and a
working driver and firmware for your controller. UART controllers must already
be attached by the platform's Bluetooth initialization process.

The package normally binds `HCI_CHANNEL_USER` and takes exclusive control of the
selected adapter. It cannot share that adapter with BlueZ or another owner.
The implementation has a raw-channel fallback for older kernels; that does not
guarantee compatibility with those systems.

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

## Minimal Linux server

Save this as `main.go` in your application module after installing a snapshot
containing `StopContext`. It advertises one service with a read-only
characteristic and releases the controller on SIGINT or SIGTERM.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/seangong/gatt/v2"
)

func main() {
	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	d, err := gatt.NewDevice(gatt.LnxDeviceID(0, true))
	if err != nil {
		log.Printf("open Bluetooth: %v", err)
		return
	}
	stopper := d.(interface {
		StopContext(context.Context) error
	})
	defer func() {
		if err := stopper.StopContext(context.Background()); err != nil {
			log.Printf("stop Bluetooth: %v", err)
		}
	}()

	serviceUUID := gatt.MustParseUUID("9A3D0001-7F31-4EB2-9F66-14B489BDF001")
	valueUUID := gatt.MustParseUUID("9A3D0002-7F31-4EB2-9F66-14B489BDF001")
	service := gatt.NewService(serviceUUID)
	service.AddCharacteristic(valueUUID).SetValue([]byte("ready"))
	setup := make(chan error, 1)
	if err := d.Init(func(device gatt.Device, state gatt.State) {
		if state != gatt.StatePoweredOn {
			return
		}
		if err := device.AddService(service); err != nil {
			setup <- err
			return
		}
		setup <- device.AdvertiseNameAndServices("Gopher", []gatt.UUID{service.UUID()})
	}); err != nil {
		log.Printf("initialize Bluetooth: %v", err)
		return
	}

	select {
	case err := <-setup:
		if err != nil {
			log.Printf("start advertising: %v", err)
			return
		}
	case <-ctx.Done():
		return
	}
	log.Print("advertising Gopher; press Ctrl+C to stop")
	<-ctx.Done()
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

`AdvPacket` append methods retain their existing payload-truncation behavior.
If even a field's two-byte header cannot fit, the packet keeps its previous
contents and `Err()` reports `ErrEIRPacketTooLong`. `Advertise` returns that
error before changing advertising state or sending commands. Construct a new
packet after a build error. When using `Bytes()` with low-level advertising
options, check `Err()` first; `Bytes()` and `Len()` alone do not report errors.

Linux scanning preserves valid fields when another advertising field is
malformed. Padding and unparseable tails are removed separately from each
advertising or scan-response payload before they are combined.

## Linux reads, writes and subscriptions

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

The default ATT MTU is 23. Linux accepts MTUs up to 517, while a single
notification or indication value is limited to `min(MTU - 3, 512)` bytes.
Linux client connections also start at MTU 23; an explicit `SetMTU` call is not
required before `ReadLongCharacteristic`. A successful MTU exchange takes
effect before the next ATT request, and each read uses its transaction's MTU
when determining whether more data remains.
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

## Linux shutdown and reopening

Linux devices expose optional `StopContext(context.Context) error` and
`Stop() error` methods. They are not part of the portable `Device` interface;
use a capability assertion as in the example above. Other platforms do not
provide the same shutdown contract.

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

On Linux, run tests with vet disabled while the existing HCI logging vet issues
remain. Race testing also requires a supported architecture and a C toolchain:

```sh
go test -vet=off ./...
go test -race -vet=off ./...
```

These commands do not establish a clean `go vet` result. On macOS, existing XPC
tests also have compilation errors, so `go test ./...` is not currently a clean
check. Keep controller/peer interoperability testing separate from unit tests;
record the hardware, firmware, role and negotiated MTU.

## Background and license

gatt started as a Go port of [bleno](https://github.com/sandeepmistry/bleno).
Related BLE projects include [noble](https://github.com/sandeepmistry/noble).

Released under the [BSD-style license](LICENSE.md).
