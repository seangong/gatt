# Package gatt provides a Bluetooth Low Energy GATT implementation.

Gatt (Generic Attribute Profile) is the protocol used to write BLE peripherals (servers) and centrals (clients).

As a peripheral, you can create services, characteristics, and descriptors,
advertise, accept connections, and handle requests.

As a central, you can scan, connect, discover services, and make requests.

## SETUP

### gatt supports both Linux and OS X.

### On Linux:
To gain complete and exclusive control of the HCI device, gatt uses
HCI_CHANNEL_USER (introduced in Linux v3.14) instead of HCI_CHANNEL_RAW.
Those who must use an older kernel may patch in these relevant commits
from Marcel Holtmann:

    Bluetooth: Introduce new HCI socket channel for user operation
    Bluetooth: Introduce user channel flag for HCI devices
    Bluetooth: Refactor raw socket filter into more readable code

Note that because gatt uses HCI_CHANNEL_USER, once gatt has opened the
device no other program may access it.

Before starting a gatt program, make sure that your BLE device is down:

    sudo hciconfig
    sudo hciconfig hci0 down  # or whatever hci device you want to use

If you have BlueZ 5.14+ (or aren't sure), stop the built-in
bluetooth server, which interferes with gatt, e.g.:

    sudo service bluetooth stop

Because gatt programs administer network devices, they must
either be run as root, or be granted appropriate capabilities:

    sudo <executable>
    # OR
    sudo setcap 'cap_net_raw,cap_net_admin=eip' <executable>
    <executable>

## Usage

Install version 2.0.0:

```sh
go get github.com/seangong/gatt/v2@v2.0.0
```

Use the v2 module path in imports:

```go
import "github.com/seangong/gatt/v2"
```

Please see [pkg.go.dev](https://pkg.go.dev/github.com/seangong/gatt/v2) for documentation.

### Final shutdown on Linux

Linux devices provide the optional `StopContext(context.Context) error` and
`Stop() error` methods. The `Device` interface is unchanged, so callers can use
a capability assertion. `StopContext` is not available in v2.0.0; consumers
pinned to that version need a release containing these changes to use it:

```go
if stopper, ok := d.(interface {
    StopContext(context.Context) error
}); ok {
    if err := stopper.StopContext(ctx); err != nil {
        log.Printf("Bluetooth shutdown: %v", err)
    }
}
```

Final shutdown rejects new operations, cancels connection IO, and gives all
connections one shared graceful deadline. A successful Disconnect command is
not enough: shutdown waits for `DisconnectionComplete`. After the graceful
deadline, or a transport failure, it closes the command processor and the
pollable HCI socket, then joins the library's receiver and protocol workers.
The graceful phase currently has a four-second budget, leaving room within the
five-second shutdown policy for local cleanup. Forced shutdown reports
`context.DeadlineExceeded`; underlying errors remain available through
`errors.Is`. The HCI socket must support runtime polling and IO deadlines.

Concurrent shutdown callers share the same cleanup and result. Canceling the
context passed to `StopContext` stops only that caller's wait; cleanup continues.
Stopping advertising or removing services alone does not release the device.
Initialization is allowed once per device. Final shutdown is irreversible;
construct a new device to start again.

Application callbacks are outside the core shutdown wait. `Request.Context()`
is canceled on connection shutdown so read, write, and notification handlers
can stop blocking work cooperatively. Callbacks that ignore cancellation may
continue after shutdown returns; the library bounds their concurrency and keeps
their slots occupied until they actually return. Lifecycle callbacks remain
ordered per connection. Server `CentralConnected` finishes before ATT requests
start; client `PeripheralConnected` can make synchronous discovery requests.
`Disconnected` and `PoweredOff` callbacks may run after shutdown returns, and
can themselves request shutdown without waiting on their own execution.

## Examples

### Build and run the examples on a native environment (Linux or OS X)

Go is a compiled language, which means to run the examples you need to build them first.

    # Build the sample server.
    go build examples/server.go
    # Start the sample server.
    sudo ./server

Alternatively, you can use "go run" to build and run the examples in a single step:

    # Build and run the sample server.
    sudo go run examples/server.go

Discoverer and explorer demonstrates central (client) functions:

    # Discover surrounding peripherals.
    sudo go run examples/discoverer.go

    # Connect to and explorer a peripheral device.
    sudo go run examples/explorer.go <peripheral ID>

### Cross-compile and deploy to a target device

    # Build and run the server example on a ARMv5 target device.
    GOARCH=arm GOARM=5 GOOS=linux go build examples/server.go
    cp server <target device>
    # Start the server on the target device
    sudo ./server

See the server.go, discoverer.go, and explorer.go in the examples/
directory for writing server or client programs that run on Linux
and OS X.

Users, especially on Linux platforms, seeking finer-grained control
over the devices can see the examples/server_lnx.go for the usage
of Option, which are platform specific.

See the rest of the docs for other options and finer-grained control.

## Note
Note that some BLE central devices, particularly iOS, may aggressively
cache results from previous connections. If you change your services or
characteristics, you may need to reboot the other device to pick up the
changes. This is a common source of confusion and apparent bugs. For an
OS X central, see http://stackoverflow.com/questions/20553957.

## Known Issues

Currently OS X vesion  does not support subscribing to indications. 
Please check [#32](https://github.com/seangong/gatt/issues/32) for the status of this issue.

## REFERENCES

gatt started life as a port of bleno, to which it is indebted:
https://github.com/sandeepmistry/bleno. If you are having
problems with gatt, particularly around installation, issues
filed with bleno might also be helpful references.

To try out your GATT server, it is useful to experiment with a
generic BLE client. LightBlue is a good choice. It is available
free for both iOS and OS X.

gatt is similar to [bleno](https://github.com/sandeepmistry/bleno) and [noble](https://github.com/sandeepmistry/noble), which offer BLE GATT implementations for node.js.

Gatt is released under a [BSD-style license](./LICENSE.md).
