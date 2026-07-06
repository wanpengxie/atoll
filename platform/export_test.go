package platform

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// HandleCancelUpstreamForTest exposes the unexported KindCancelRequest disposition
// (reverse-resolve target from the log + sender validation, then Home.CancelRequest)
// so an external test can drive its four failure branches deterministically without
// standing up a full daemon wire (the wire relay itself is covered separately by the
// port-level and cross-wire e2e tests). It is a package-level function, NOT a Home
// method, so it does not widen Home's guarded public surface (TestHomePublicSurface).
func HandleCancelUpstreamForTest(h *Home, boundID actor.ActorID, requestID message.ID) {
	h.handleCancelUpstream(boundID, requestID)
}
