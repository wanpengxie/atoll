package link

import (
	"time"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// NewCommittingWriteHandleForTest exposes committingWriteHandle to the external
// link_test package for 期11 review #D's守测: the wrapper that fires
// Committed(reservationID) — and reclaims coord on a Lost reply — after the
// local write lands.
func NewCommittingWriteHandleForTest(d *Dialer, wh accessdoor.LocalWriteHandle, reservationID, coord string) accessdoor.LocalWriteHandle {
	return &committingWriteHandle{LocalWriteHandle: wh, dialer: d, reservationID: reservationID, coord: coord}
}

// SetYamuxKeepAliveIntervalForTest overrides the yamux session's own keepalive
// cadence (normally yamux's DefaultConfig 30s) for every *linkSession built
// AFTER this call, until reset() runs. It exists so a link_test.go guard test
// can prove the Lease's TTL judgment survives a REAL, fast-firing yamux
// keepalive (rather than relying on the keepalive's normal 30s cadence never
// firing inside a short test's window, which would prove nothing about the
// bug the two-heartbeat design guards against). Not for production use — see
// yamuxKeepAliveIntervalNS's doc (linksession.go).
func SetYamuxKeepAliveIntervalForTest(d time.Duration) (reset func()) {
	old := yamuxKeepAliveIntervalNS.Load()
	yamuxKeepAliveIntervalNS.Store(int64(d))
	return func() { yamuxKeepAliveIntervalNS.Store(old) }
}
