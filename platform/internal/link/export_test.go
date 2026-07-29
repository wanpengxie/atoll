package link

import (
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// NewCommittingWriteHandleForTest exposes committingWriteHandle to the external
// link_test package for 期11 review #D's守测: the wrapper that fires
// Committed(reservationID) — and reclaims coord on a Lost reply — after the
// local write lands.
func NewCommittingWriteHandleForTest(d *Dialer, wh accessdoor.LocalWriteHandle, reservationID, coord string) accessdoor.LocalWriteHandle {
	return &committingWriteHandle{LocalWriteHandle: wh, dialer: d, reservationID: reservationID, coord: coord}
}
