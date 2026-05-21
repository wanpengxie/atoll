package transit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// HumanCaller is the wire object the server attaches to
// `control.write_message` so the daemon can authenticate the human
// origin of a write (L2 §9.1, T1.9 / FIX-T2 spec).
//
// The field layout MUST match daemonbus.HumanCaller
// byte-for-byte — the daemon recomputes the HMAC over the same structured
// input encoding used by the gateway.
type HumanCaller = daemonbus.HumanCaller

// WriteMessageBody is the daemonbus `control.write_message` payload.
// The daemon receives one per HTTP write the gateway accepts.
//
// `EnvelopePartial` carries the caller-shaped envelope including
// `id` (caller-supplied per L0 §1.1, enforced at the gateway per
// L3 §1.8.1). The daemon fills `sender` from `HumanCaller.
// MemberActorID` + the actor_registry record, then invokes the
// harness chain — which drives L1 §2.3 Step 3 dedupe off the
// caller-supplied id. The daemon MUST NOT regenerate envelope.id
// (R4-3 fix).
type WriteMessageBody = daemonbus.WriteMessageBody

// WriteMessageAckBody is the daemon → server reply. One ack per
// inbound `control.write_message` frame.
type WriteMessageAckBody = daemonbus.WriteMessageAckBody

// Daemon-edge reject reasons. These are NOT part of L1 §10.3.1; they
// surface failures that happen BEFORE the harness chain runs.
//
// FIX-T8 will tighten these to the closed set; for FIX-T2 we keep them
// distinct strings so log triage can tell edge-fail apart from chain
// reject.
const (
	// RejectReasonAuthFailed indicates an HMAC mismatch on the
	// HumanCaller token, an unknown member_actor_id, a deregistered
	// actor, or a replay-window violation.
	RejectReasonAuthFailed = "auth_failed"

	// RejectReasonChannelUnbound indicates the channel is not currently
	// owned by this daemon — the WriteMessageRouter returned ok=false.
	// Server should re-resolve placement before retrying.
	RejectReasonChannelUnbound = "channel_unbound"

	// RejectReasonInternal indicates a non-protocol failure: actor
	// registry IO error, canonical-hash failure, store error escaping
	// the harness chain, etc.
	RejectReasonInternal = "internal"

	// RejectReasonReplayWindow indicates |now - human_caller.ts| exceeded
	// the configured replay window. FIX-T8: previously folded into
	// `auth_failed`; split out so log triage can tell clock-skew /
	// stale-credential apart from HMAC mismatch.
	RejectReasonReplayWindow = "replay_window_expired"

	// RejectReasonReplayNonce indicates the (channel, nonce) tuple was
	// seen within the replay window. FIX-T8 — prevents an attacker who
	// captured a valid HumanCaller token from re-submitting it inside
	// the window.
	RejectReasonReplayNonce = "replay_nonce_seen"
)

// HarnessChain is the minimal harness Chain shape the WriteMessage
// handler needs. The runtime/harness package satisfies it; the test
// stubs in control_test.go also implement it without dragging the full
// runtime/harness import cycle into transit_test.
type HarnessChain interface {
	Write(ctx context.Context, env *message.Envelope) (HarnessWriteResult, error)
}

// HarnessWriteResult mirrors kernel/harness.WriteResult. We re-declare
// it here (rather than importing kernel/harness directly) so the
// transit package stays one layer below the harness wiring — runtime/
// daemon adapts the kernel/harness.Chain into a transit.HarnessChain
// adapter at assembly time.
type HarnessWriteResult struct {
	MessageID        message.ID
	Seq              int64
	Deduped          bool
	RejectReason     string
	RejectDetail     string
	PartialMessageID message.ID
}

// Accepted reports whether the result is a durable / dedupe write.
func (r HarnessWriteResult) Accepted() bool { return r.RejectReason == "" }

var _ = assertHarnessWriteResultSubset(khar.WriteResult{})

// assertHarnessWriteResultSubset is a compile-time field subset check
// against kernel/harness.WriteResult. transit intentionally keeps its local
// result type to avoid importing runtime/harness, but field drift in the
// kernel contract should break this package instead of silently changing the
// daemonbus write_message mapping.
func assertHarnessWriteResultSubset(r khar.WriteResult) HarnessWriteResult {
	return HarnessWriteResult{
		MessageID:        r.MessageID,
		Seq:              r.Seq,
		Deduped:          r.Deduped,
		RejectReason:     string(r.RejectReason),
		RejectDetail:     r.RejectDetail,
		PartialMessageID: r.PartialMessageID,
	}
}

// CallerStamper plumbs the CallerContext into the chain ctx. The
// runtime/daemon wiring supplies this so we keep harness.CtxWithCaller
// out of the transit package's import path (avoids importing runtime/
// harness from the transit layer).
type CallerStamper func(ctx context.Context, actorID actor.ActorID, channelID channel.ID) context.Context

// WriteMessageRouter looks up the per-channel harness chain + actor
// registry the handler needs to authenticate the write. Returns
// ok=false when the daemon does not currently own the channel (placement
// out of date / channel still in phase 2).
type WriteMessageRouter func(ctx context.Context, ch channel.ID) (
	chain HarnessChain,
	registry actorreg.Registry,
	stamp CallerStamper,
	ok bool,
)

// WriteMessageHandlerConfig wires a WriteMessageHandler.
type WriteMessageHandlerConfig struct {
	// Secret is the shared HMAC key — matches server.gateway.App.cfg.
	// HumanCallerSecret. Required.
	Secret []byte

	// Router resolves channel_id → harness chain + actor registry +
	// caller stamper. Required.
	Router WriteMessageRouter

	// NowMs returns unix-ms; used for the replay-window check. Defaults
	// to time.Now when nil.
	NowMs func() int64

	// ReplayWindow rejects HumanCaller frames whose `ts` differs from
	// `NowMs()` by more than the window. Must be >0 unless
	// AllowReplayWindowDisabled is explicitly set for a test/dev path.
	// The window is symmetric (past AND future) so the daemon also drops
	// far-future timestamps.
	ReplayWindow time.Duration

	// AllowReplayWindowDisabled is a test/dev-only escape hatch. Production
	// callers must leave this false so ReplayWindow<=0 fails fast.
	AllowReplayWindowDisabled bool
}

// WriteMessageHandler is the daemon-side implementation of
// `control.write_message`. It exposes a single Handle method that the
// Dispatcher invokes once per frame and that returns the
// WriteMessageAckBody the Dispatcher then sends back as
// `control.write_message_ack`.
type WriteMessageHandler struct {
	cfg WriteMessageHandlerConfig

	// nonceCache is the FIX-T8 per-channel replay guard. It is non-nil
	// only when replay protection is enabled; entries expire after one window
	// (older nonces would already be cut by the ts check).
	nonceCache *nonceCache
}

// NewWriteMessageHandler builds a WriteMessageHandler.
func NewWriteMessageHandler(cfg WriteMessageHandlerConfig) (*WriteMessageHandler, error) {
	if len(cfg.Secret) == 0 {
		return nil, errors.New("transit: WriteMessageHandlerConfig.Secret empty")
	}
	if cfg.Router == nil {
		return nil, errors.New("transit: WriteMessageHandlerConfig.Router nil")
	}
	if cfg.NowMs == nil {
		cfg.NowMs = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.ReplayWindow <= 0 {
		if !cfg.AllowReplayWindowDisabled {
			return nil, errors.New("transit: WriteMessageHandlerConfig.ReplayWindow must be > 0")
		}
		cfg.ReplayWindow = 0
	}
	h := &WriteMessageHandler{cfg: cfg}
	if cfg.ReplayWindow > 0 {
		h.nonceCache = newNonceCache(cfg.ReplayWindow)
	}
	return h, nil
}

// Handle authenticates the HumanCaller token, fills in `sender` +
// derives `envelope.id`, runs the chain, and returns the ack body.
//
// Errors are surfaced as ack.Accepted=false rather than returned —
// only protocol-level decode failures (caught at the Dispatch layer
// before Handle is invoked) bubble up as transport errors.
func (h *WriteMessageHandler) Handle(ctx context.Context, body WriteMessageBody) WriteMessageAckBody {
	ack := WriteMessageAckBody{FrameID: body.FrameID}

	if body.ChannelID == "" {
		ack.RejectReason = RejectReasonInternal
		ack.RejectDetail = "channel_id empty"
		return ack
	}

	// 1. HMAC verify — constant-time.
	expect := SignHumanCaller(h.cfg.Secret,
		string(body.ChannelID),
		body.HumanCaller.UserID,
		body.HumanCaller.MemberActorID,
		body.HumanCaller.TS,
		body.HumanCaller.Nonce,
	)
	if !hmac.Equal([]byte(expect), []byte(body.HumanCaller.ServerToken)) {
		ack.RejectReason = RejectReasonAuthFailed
		ack.RejectDetail = "human_caller token mismatch"
		return ack
	}

	// 2. Optional replay-window check. FIX-T8: split clock-skew rejects
	// (`replay_window_expired`) from the nonce-cache reject
	// (`replay_nonce_seen`) so triage can tell stale-credentials apart
	// from a deliberate replay attempt.
	if h.cfg.ReplayWindow > 0 {
		nowMs := h.cfg.NowMs()
		delta := nowMs - body.HumanCaller.TS
		if delta < 0 {
			delta = -delta
		}
		if time.Duration(delta)*time.Millisecond > h.cfg.ReplayWindow {
			ack.RejectReason = RejectReasonReplayWindow
			ack.RejectDetail = "human_caller ts outside replay window"
			return ack
		}
		// Per-channel nonce LRU — reject re-use of the same nonce
		// within one window. The cache is keyed by (channelID, nonce)
		// and TTL-expires entries lazily.
		if !h.nonceCache.observe(string(body.ChannelID), body.HumanCaller.Nonce, nowMs) {
			ack.RejectReason = RejectReasonReplayNonce
			ack.RejectDetail = "human_caller nonce already seen within replay window"
			return ack
		}
	}

	// 3. Resolve per-channel chain + registry.
	chID := body.ChannelID
	chain, registry, stamp, ok := h.cfg.Router(ctx, chID)
	if !ok {
		ack.RejectReason = RejectReasonChannelUnbound
		ack.RejectDetail = fmt.Sprintf("channel %s not owned by this daemon", body.ChannelID)
		return ack
	}

	// 4. Verify the claimed actor exists + is active.
	actorID := body.HumanCaller.MemberActorID
	if actorID == "" {
		ack.RejectReason = RejectReasonAuthFailed
		ack.RejectDetail = "human_caller member_actor_id empty"
		return ack
	}
	rec, exists, err := registry.Lookup(ctx, actorID)
	if err != nil {
		ack.RejectReason = RejectReasonInternal
		ack.RejectDetail = "actor lookup: " + err.Error()
		return ack
	}
	if !exists || !rec.IsActive() {
		ack.RejectReason = RejectReasonAuthFailed
		ack.RejectDetail = "member_actor_id unknown or deregistered"
		return ack
	}

	// 5. Stamp `sender` from the registry record + force channel_id /
	// caller-controlled fields.
	env := body.EnvelopePartial
	env.ChannelID = body.ChannelID
	env.Sender = message.Sender{
		Kind: rec.Kind,
		ID:   rec.ID,
		Name: rec.DisplayName,
	}
	if env.TS == 0 {
		env.TS = body.HumanCaller.TS
	}
	if env.Visibility == "" {
		env.Visibility = message.VisibilityPublic
	}
	if len(env.Audience) == 0 {
		env.Audience = message.Audience{message.AudienceWildcard}
	}
	// Ensure non-null payload (canonical hash refuses empty).
	if len(env.Payload) == 0 {
		env.Payload = []byte("{}")
	}

	// 6. R4-3: envelope.id MUST be supplied by the caller per L0 §1.1
	// + L3 §1.8.1. The daemon does NOT regenerate; an empty id is a
	// gateway-side contract bug (the gateway must binding:"required"
	// the field) and is rejected at the daemon edge to keep the
	// invariant local rather than silently allowing the harness to
	// fail Step 2 with a less specific reason.
	if env.ID == "" {
		ack.RejectReason = RejectReasonAuthFailed
		ack.RejectDetail = "envelope.id empty — caller MUST supply id per L3 §1.8.1"
		return ack
	}

	// 7. Invoke the harness chain — caller_context provides the
	// authenticated principal for step 1 / step 3.
	chainCtx := ctx
	if stamp != nil {
		chainCtx = stamp(ctx, actorID, chID)
	}
	res, err := chain.Write(chainCtx, &env)
	if err != nil {
		ack.RejectReason = RejectReasonInternal
		ack.RejectDetail = "harness write: " + err.Error()
		ack.MessageID = env.ID
		return ack
	}
	ack.MessageID = res.MessageID
	if ack.MessageID == "" {
		ack.MessageID = env.ID
	}
	if !res.Accepted() {
		ack.RejectReason = res.RejectReason
		ack.RejectDetail = res.RejectDetail
		if res.PartialMessageID != "" {
			ack.MessageID = res.PartialMessageID
		}
		return ack
	}
	ack.Accepted = true
	ack.Seq = res.Seq
	ack.Deduped = res.Deduped
	return ack
}

// SignHumanCaller produces the HMAC token over structured
// (channelID, userID, actorID, ts, nonce) input using SHA-256,
// hex-lowercase output. String fields are length-prefixed and ts is
// fixed-width big-endian so embedded delimiters cannot cause field
// confusion. Mirrors server/gateway/handlers.go signHumanCaller exactly so
// the daemon-side verify recomputes the same bytes.
func SignHumanCaller(secret []byte, channelID string, userID daemonbus.UserID, actorID actor.ActorID, ts int64, nonce string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("coagent-human-caller-v2"))
	writeHumanCallerField(mac, channelID)
	writeHumanCallerField(mac, string(userID))
	writeHumanCallerField(mac, string(actorID))
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], uint64(ts))
	mac.Write(tsBuf[:])
	writeHumanCallerField(mac, nonce)
	return hex.EncodeToString(mac.Sum(nil))
}

func writeHumanCallerField(mac hashWriter, value string) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(value)))
	_, _ = mac.Write(lenBuf[:])
	_, _ = mac.Write([]byte(value))
}

type hashWriter interface {
	Write([]byte) (int, error)
}
