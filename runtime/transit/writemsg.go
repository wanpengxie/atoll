package transit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// HumanCaller is the wire object the server attaches to
// `control.write_message` so the daemon can authenticate the human
// origin of a write (L2 §9.1, T1.9 / FIX-T2 spec).
//
// The field layout MUST match server/gateway/handlers.go HumanCaller
// byte-for-byte — the daemon recomputes the HMAC over the same input
// concatenation order (`channelID|userID|actorID|ts|nonce`).
type HumanCaller struct {
	UserID           string `json:"user_id"`
	ActorIDInChannel string `json:"actor_id_in_channel"`
	TS               int64  `json:"ts"`
	Nonce            string `json:"nonce"`
	ServerToken      string `json:"server_token"`
}

// WriteMessageBody is the daemonbus `control.write_message` payload.
// The daemon receives one per HTTP write the gateway accepts.
//
// `EnvelopePartial` carries the caller-shaped envelope (no id / no
// sender). The daemon fills `sender` from `HumanCaller.ActorIDInChannel`
// + the actor_registry record, then derives `id = CanonicalHash(env)`
// before invoking the harness chain (matches T1.9 §"daemon 收到 control.
// write_message" flow).
type WriteMessageBody struct {
	FrameID         string           `json:"frame_id"`
	ChannelID       string           `json:"channel_id"`
	HumanCaller     HumanCaller      `json:"human_caller"`
	EnvelopePartial message.Envelope `json:"envelope_partial"`
}

// WriteMessageAckBody is the daemon → server reply. One ack per
// inbound `control.write_message` frame.
type WriteMessageAckBody struct {
	// FrameID echoes the request frame_id so the gateway can pair it
	// with the HTTP request waiting on SendAndAwait.
	FrameID string `json:"frame_id"`

	// Accepted is true when the harness wrote (or dedupe-matched) the
	// envelope. false on every reject path (HMAC failure, unknown
	// channel, harness reject, internal error).
	Accepted bool `json:"accepted"`

	// MessageID is the envelope.id the daemon allocated (CanonicalHash
	// result). Present on accept AND on the harness-reject path so the
	// caller can correlate; empty on auth_failed / channel_unbound.
	MessageID string `json:"message_id,omitempty"`

	// Seq is the assigned monotonic sequence on the accept path. Zero
	// for dedupe / any reject.
	Seq int64 `json:"seq,omitempty"`

	// Deduped reports the L2 §1.4.10.1 idempotent-retry path. Implies
	// Accepted=true.
	Deduped bool `json:"deduped,omitempty"`

	// RejectReason is the closed-set L1 §10.3.1 reason on a harness
	// reject. Daemon-edge rejects (HMAC failure / unknown channel /
	// internal error) populate this with edge-only sentinels documented
	// in the const block below.
	RejectReason string `json:"reject_reason,omitempty"`

	// RejectDetail is the human-readable detail mirrored from the
	// rejecting step (or daemon edge).
	RejectDetail string `json:"reject_detail,omitempty"`
}

// Daemon-edge reject reasons. These are NOT part of L1 §10.3.1; they
// surface failures that happen BEFORE the harness chain runs.
//
// FIX-T8 will tighten these to the closed set; for FIX-T2 we keep them
// distinct strings so log triage can tell edge-fail apart from chain
// reject.
const (
	// RejectReasonAuthFailed indicates an HMAC mismatch on the
	// HumanCaller token, an unknown actor_id_in_channel, a deregistered
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
	MessageID        string
	Seq              int64
	Deduped          bool
	RejectReason     string
	RejectDetail     string
	PartialMessageID string
}

// Accepted reports whether the result is a durable / dedupe write.
func (r HarnessWriteResult) Accepted() bool { return r.RejectReason == "" }

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
	registry actor.Registry,
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
	// `NowMs()` by more than the window. Zero disables the check (FIX-
	// T8 hardening lifts this). The window is symmetric (past AND
	// future) so the daemon also drops far-future timestamps.
	ReplayWindow time.Duration
}

// WriteMessageHandler is the daemon-side implementation of
// `control.write_message`. It exposes a single Handle method that the
// Dispatcher invokes once per frame and that returns the
// WriteMessageAckBody the Dispatcher then sends back as
// `control.write_message_ack`.
type WriteMessageHandler struct {
	cfg WriteMessageHandlerConfig
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
	return &WriteMessageHandler{cfg: cfg}, nil
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
		body.ChannelID,
		body.HumanCaller.UserID,
		body.HumanCaller.ActorIDInChannel,
		body.HumanCaller.TS,
		body.HumanCaller.Nonce,
	)
	if !hmac.Equal([]byte(expect), []byte(body.HumanCaller.ServerToken)) {
		ack.RejectReason = RejectReasonAuthFailed
		ack.RejectDetail = "human_caller token mismatch"
		return ack
	}

	// 2. Optional replay-window check.
	if h.cfg.ReplayWindow > 0 {
		nowMs := h.cfg.NowMs()
		delta := nowMs - body.HumanCaller.TS
		if delta < 0 {
			delta = -delta
		}
		if time.Duration(delta)*time.Millisecond > h.cfg.ReplayWindow {
			ack.RejectReason = RejectReasonAuthFailed
			ack.RejectDetail = "human_caller ts outside replay window"
			return ack
		}
	}

	// 3. Resolve per-channel chain + registry.
	chID := channel.ID(body.ChannelID)
	chain, registry, stamp, ok := h.cfg.Router(ctx, chID)
	if !ok {
		ack.RejectReason = RejectReasonChannelUnbound
		ack.RejectDetail = fmt.Sprintf("channel %s not owned by this daemon", body.ChannelID)
		return ack
	}

	// 4. Verify the claimed actor exists + is active.
	actorID := actor.ActorID(body.HumanCaller.ActorIDInChannel)
	if actorID == "" {
		ack.RejectReason = RejectReasonAuthFailed
		ack.RejectDetail = "human_caller actor_id_in_channel empty"
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
		ack.RejectDetail = "actor_id_in_channel unknown or deregistered"
		return ack
	}

	// 5. Stamp `sender` from the registry record + force channel_id /
	// caller-controlled fields.
	env := body.EnvelopePartial
	env.ChannelID = body.ChannelID
	env.Sender = message.Sender{
		Kind: rec.Kind,
		ID:   string(rec.ID),
		Name: rec.DisplayName,
	}
	if env.TS == 0 {
		env.TS = body.HumanCaller.TS
	}
	if env.Visibility == "" {
		env.Visibility = message.VisibilityPublic
	}
	if len(env.Audience) == 0 {
		env.Audience = []string{"*"}
	}
	// Ensure non-null payload (canonical hash refuses empty).
	if len(env.Payload) == 0 {
		env.Payload = []byte("{}")
	}

	// 6. Compute canonical-hash envelope.id BEFORE the chain so step
	// 0.5 / step 8 can dedupe by id.
	id, err := message.CanonicalHash(env)
	if err != nil {
		ack.RejectReason = RejectReasonInternal
		ack.RejectDetail = "canonical_hash: " + err.Error()
		return ack
	}
	env.ID = id

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

// SignHumanCaller produces the HMAC token over
// `channelID|userID|actorID|ts|nonce` using SHA-256, hex-lowercase
// output. Mirrors server/gateway/handlers.go signHumanCaller exactly so
// the daemon-side verify recomputes the same bytes.
func SignHumanCaller(secret []byte, channelID, userID, actorID string, ts int64, nonce string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(channelID))
	mac.Write([]byte("|"))
	mac.Write([]byte(userID))
	mac.Write([]byte("|"))
	mac.Write([]byte(actorID))
	mac.Write([]byte("|"))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("|"))
	mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}
