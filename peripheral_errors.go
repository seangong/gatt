package gatt

import "errors"

// ErrDatabaseChanged means rediscovery found a different remote attribute
// layout. The client closes the connection to prevent use of stale
// handles; reconnect and discover the database again before continuing.
var ErrDatabaseChanged = errors.New("remote GATT database changed")
