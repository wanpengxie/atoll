package platform

import (
	"context"
	"net/http"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// hooks is the actorbase engine's per-host wiring for every Proc-shaped def
// this Home builds (spec §3's out-generation matrix, row 1): a cell host always
// has a live CancelRequest reach, so Hooks.Canceller is never nil here (the
// daemon-side gap — no caller-side cancel upstream frame — is a DIFFERENT
// host, wired in compute.go instead).
func (h *Home) hooks() actorbase.Hooks {
	return actorbase.Hooks{Canceller: h.CancelRequest}
}

// CancelRequest reaches the request-scope of cancel(scope) for one in-flight
// request `target` is running under `requestID`. Home holds the runtime
// directly (cell/port hosting is transport-neutral inside it — CancelRequest
// reaches a daemon-hosted port the same way it reaches a local cell) so this
// is a direct call, no Acceptor indirection needed. No-op if the request
// already closed or `target` has no live embodiment — cancel is a
// best-effort hint, the caller's closure owns the terminal.
func (h *Home) CancelRequest(target actor.ActorID, requestID message.ID) {
	h.channel.Cells().CancelRequest(target, requestID)
}

// handleCancelUpstream is the home's disposition for one KindCancelRequest frame
// (the caller-side upstream twin of CancelRequest): a daemon-hosted caller,
// identified by the connection's authenticated bound id, abandons one of ITS OWN
// outbound requests named by requestID. The caller self-reports NEITHER the
// request's target NOR its own identity — the home takes both from truth: it
// reverse-resolves the request envelope from the log by id, reads the target from
// its audience, and validates that the request's sender == the connection's bound
// id (a half-trusted daemon may only revoke a request it actually authored). Four
// failure branches — not found / non-request kind / empty audience / sender
// mismatch — all silently drop + log (best-effort no-ack semantics: an upstream
// cancel is a hint, never a verdict; the caller's own closure already owns the
// terminal and the request's deadline still collapses its reqCtx). On the happy
// path it fires Home.CancelRequest(target, requestID) — the exact same reach a
// local cell's Hooks.Canceller takes.
func (h *Home) handleCancelUpstream(boundID actor.ActorID, requestID message.ID) {
	req, ok, err := h.cs.Requests.FindByID(context.Background(), requestID)
	if err != nil || !ok {
		h.logger.Info("platform.home.cancel_upstream.not_found", "request", string(requestID), "sender", string(boundID), "err", err)
		return
	}
	if req.Kind != message.KindRequest {
		h.logger.Info("platform.home.cancel_upstream.not_a_request", "request", string(requestID), "kind", string(req.Kind), "sender", string(boundID))
		return
	}
	if len(req.Audience) == 0 {
		h.logger.Info("platform.home.cancel_upstream.empty_audience", "request", string(requestID), "sender", string(boundID))
		return
	}
	if req.Sender.ID != boundID {
		h.logger.Info("platform.home.cancel_upstream.sender_mismatch", "request", string(requestID), "sender", string(boundID), "authored_by", string(req.Sender.ID))
		return
	}
	h.CancelRequest(req.Audience[0], requestID)
}

// KickDaemon closes every link this compute currently holds (the substrate
// half of a revocation, §8.3) and returns the count closed. It is a write
// handle (unlike View, a read-only face) — revoking access is a write. The
// authority to decide WHEN to kick (a daemon's credential was just revoked)
// lives entirely in the app layer; this method only executes the mechanical
// teardown. Kicked ports fall silent (quiet-stop, no receiver_unavailable) —
// a kick is a voluntary revocation, not an observed death.
func (h *Home) KickDaemon(computeID string) int {
	return h.links.KickDaemon(computeID)
}

// ServeAttach is the attach admission surface: the app hands an upgraded WS request here so a
// daemon can attach its actor streams. Home keeps the internal link acceptor and
// only exposes this capability — the acceptor object never escapes.
func (h *Home) ServeAttach(w http.ResponseWriter, r *http.Request, daemonID string) {
	if h.closed.Load() || h.links == nil {
		http.Error(w, "home closed", http.StatusServiceUnavailable)
		return
	}
	h.links.Serve(w, r, daemonID)
}

// Subscribe is the subscription registration surface (client push): a client stream subscribes to
// the commit Signal and reads forward from its own seq cursor. It returns the
// wake channel and the unsubscribe func — the internal Signal never escapes.
func (h *Home) Subscribe() (<-chan struct{}, func()) {
	if h.closed.Load() {
		ch := make(chan struct{})
		close(ch)
		return ch, func() {}
	}
	return h.signal.Subscribe()
}
