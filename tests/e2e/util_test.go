//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// uniqSuffix returns a process-unique string fragment safe to append
// to emails / workspace names / channel names. Tests don't share a
// stack, but they do share the same server.db within a stack (when
// future tests opt in to stack-sharing), so collision-free names
// keep assertions deterministic.
var uniqCounter uint64

func uniqSuffix() string {
	n := atomic.AddUint64(&uniqCounter, 1)
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), n)
}

// httpHeaderWithOrigin builds a single-entry Header with the named
// Origin — small helper used by device session bind cases that want a
// rejected handshake (no full MockExtension wrap).
func httpHeaderWithOrigin(origin string) http.Header {
	h := http.Header{}
	if origin != "" {
		h.Set("Origin", origin)
	}
	return h
}

// dialWS is a thin wrapper around websocket.DefaultDialer.DialContext
// that imposes a short handshake timeout so failure cases don't hang
// the test budget.
func dialWS(ctx context.Context, wsURL string, header http.Header) (*websocket.Conn, *http.Response, error) {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	return dialer.DialContext(ctx, wsURL, header)
}
