package link

import (
	"io"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// HandleLaneInboundForTest drives the daemon's inbound lane handler (#19's
// unified write-completion arm) over a caller-supplied conn, so an external test
// can prove the OpWrite arm routes through committingWriteHandle: Lost reclaims,
// NAK/transport-error does not, and a reservation-less transfer takes the plain
// Commit path.
func (d *Dialer) HandleLaneInboundForTest(conn io.ReadWriteCloser) { d.handleLaneInbound(conn) }

// NewCommittingWriteHandleForTest exposes committingWriteHandle to the external
// link_test package for 期11 review #D's守测: the wrapper that fires
// Committed(reservationID) — and reclaims coord on a Lost reply — after the
// local write lands.
func NewCommittingWriteHandleForTest(d *Dialer, wh accessdoor.LocalWriteHandle, reservationID, coord string) accessdoor.LocalWriteHandle {
	return &committingWriteHandle{LocalWriteHandle: wh, dialer: d, reservationID: reservationID, coord: coord}
}

// LaneLinkPresentForTest reports whether daemonID still has a lane-relay link
// registered on this Acceptor (a.laneLink != nil). It is the external guard for
// G-1's "no dead lane session residual after a link tears down": once a link
// unwinds, deregisterLaneLink must have dropped its entry, so this returns
// false. Test-only accessor over the unexported lane-relay table.
func (a *Acceptor) LaneLinkPresentForTest(daemonID string) bool {
	return a.laneLink(daemonID) != nil
}
