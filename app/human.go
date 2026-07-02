package app

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// errChannelNotLoaded is returned by humanFor when the home vanished between the
// handler's nil-check and admission (a teardown race). The handler maps it to 404.
var errChannelNotLoaded = errors.New("app: channel not loaded")

// NOTE: this file is the sealed-pen HALF-BUILT state. The target shape (gateway
// relay, Receive→gateway, type A/B response, cell≡online-signal) lives in
// .dalek/pm/human-actor-completion.md (v7). The obs-tail stopgap mentioned below
// is SUPERSEDED there: members read messages via the actor path (Receive→gateway),
// not by tailing the log over the obs axis.
//
// human.go is the app-layer HUMAN write front-end — a human adapter whose
// outward face is SubmitIntent. It is the substrate's "people are actors too":
// a person writes truth ONLY through their own human actor's welded pen, never
// through an app-held write gate (sealed-pen: app holds no Minter, no bare
// writer). It lives in app/ (not actors/) because its write arm carries PRODUCT
// routing policy — default_agent / boost-floor failover / group-chat broadcast —
// reading the app DB (default_agent) and Home.View (live roster). That is domain
// policy; actors/ must not import app DB / platform.View (it would invert the
// layering). It is an adapter all the same: inward a cell whose Receive SHOULD
// translate an inbound collaboration message (an agent's call) and push it to the
// person — currently no-op, borrowing the obs axis (ws.go tail) as a stopgap (see
// Receive); outward it holds a pen and writes through it from the HTTP goroutine
// (the same shape as the xhs/kimi device face — a non-cell goroutine writing truth
// through the actor's pen, guarding its outward state with a mutex).
//
// Lifecycle is HOME-SCOPED, not WS-scoped: a write arrives over HTTP POST and may
// happen with NO WebSocket open (curl / API client / the very first message), so
// the cell must exist whenever the home is up and the user is a member — it
// cannot be gated on a WS connection. The push arm (browser tail) stays the
// existing ws.go tail-subscribe, untouched. liveness (cell up) does NOT mean the
// user is online; online-ness is the WS push arm's concern, never membership.

// humanFront is one user's write front-end in one channel. It IS an actorrt.Actor
// (so it is admitted through the same Home.Spawn path as agents/tools and is born
// with a pen welded to its own user id — no PenFor side door). A human IS callable
// on the message axis (an agent's request lands in this mailbox); Receive is no-op
// FOR NOW, with the inbound push half borrowed from the obs axis (see Receive).
// SubmitIntent is its outward face, called directly by the HTTP goroutine (off the
// cell goroutine, bypassing the mailbox), returning the WriteResult synchronously
// so the POST keeps its 201 message_id/seq.
type humanFront struct {
	app  *App
	chID channel.ID
	pen  harness.Pen

	// mu guards the outward face's state crossing the HTTP↔cell goroutine
	// boundary. Today the write path is fire-and-forget (pen.Write is itself
	// concurrency-safe: the chain is a pure function over a serialised sqlite
	// tx), so the only shared state is the closed flag; the mutex is the seam any
	// future outward state (idle bookkeeping, an outbound fan-out queue) hangs on
	// — the same discipline as the xhs/kimi device face.
	mu     sync.Mutex
	closed bool
}

// Receive is the inward mailbox — the COLLABORATION ingress arm. A human IS a
// first-class actor on the message axis: an agent can call it (kind=request,
// audience=[user:X]) and the request lands in THIS mailbox. (That is exactly why
// the human MUST hold a cell — without one it is a message-axis ghost and an
// agent's call resolves receiver_unavailable even while the person is watching.)
//
// TODO(human-collaboration-arm): Receive should TRANSLATE an inbound collaboration
// message (an agent's request, a directed message) and PUSH it to the person
// (live WS push / offline notification) — the outbound-to-human half of this
// adapter, symmetric with SubmitIntent's inbound half. It is no-op for now:
// delivery to this mailbox reports Delivered but is dropped here, and the person
// instead sees the message via the OBS axis (ws.go tail-subscribe reads the
// committed log and pushes it to the browser), then replies via SubmitIntent
// (parent_id). That obs-axis substitution is good enough pre-launch (person sees
// it + can reply), but it is NOT the message axis: obs is passive observation of
// the log, not actor-directed collaboration delivery. The proper fix is to make
// this arm a real outbound translator. The no-op does NOT mean the cell is
// removable — the cell's existence is what keeps the human callable; only the
// push half is borrowed from obs for now.
func (h *humanFront) Receive(_ context.Context, _ *message.Envelope) error {
	return nil
}

// Stop marks the front-end closed so a late SubmitIntent fails cleanly rather than
// writing through a torn-down home. Runs on the cell goroutine after the mailbox
// drains (actorrt.Stopper).
func (h *humanFront) Stop(_ context.Context) error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	return nil
}

// submitInput is the user's RAW intent (the parsed HTTP body) — NOT a ready
// envelope. The front-end resolves routing and builds a compliant envelope from
// it; identity (sender.id / channel_id) is left for the pen to weld.
type submitInput struct {
	ID         string
	Type       string
	Kind       string
	Payload    json.RawMessage
	Audience   []string
	Visibility string
	ParentID   string
}

// routingError carries a per-request routing condition (no reachable brain / a
// down default agent) back to the HTTP layer as a 503 to the SENDING user — it is
// NOT written into the channel as truth. (errors.As-friendly: the handler maps it
// to ServiceUnavailable.)
type routingError struct {
	detail string
}

func (e *routingError) Error() string { return e.detail }

// SubmitIntent is the human adapter's OUTWARD FACE: the HTTP goroutine hands it a
// user's raw intent; it resolves the channel's routing policy, builds a compliant
// envelope (leaving identity for the pen to weld), commits it through the welded
// pen, and returns the WriteResult SYNCHRONOUSLY (preserving the POST's 201
// message_id/seq). It runs OFF the cell goroutine on a request-scoped ctx (not the
// Receive ctx).
//
// A *routingError means a per-request condition (no reachable brain) that the
// handler surfaces as 503; any other error is an internal failure. A non-Accepted
// WriteResult is the substrate rejecting the write (surfaced as 422).
func (h *humanFront) SubmitIntent(ctx context.Context, in submitInput) (harness.WriteResult, error) {
	h.mu.Lock()
	closed := h.closed
	h.mu.Unlock()
	if closed {
		return harness.WriteResult{}, context.Canceled
	}

	audience, kind, rErr := h.resolveRouting(ctx, in)
	if rErr != nil {
		return harness.WriteResult{}, rErr
	}

	env := h.newClientEnvelope(in, kind, audience)
	if in.Visibility != "" {
		env.Visibility = message.Visibility(in.Visibility)
	}
	if in.ParentID != "" {
		env.ParentID = message.ID(in.ParentID)
	}
	// pen.Write welds sender.id + channel_id (left empty above) and drives the
	// chain — the human cannot stamp its own identity, the substrate does.
	return h.pen.Write(ctx, env)
}

// resolveRouting reproduces the channel's no-audience routing policy (the product
// decision moved here verbatim from handleSendMessage): an explicit audience is
// honoured as-is; otherwise default_agent is the INTENT pointer, resolved against
// the live roster with the agent:boost floor as the §7 failover target:
//   - default_agent points at a LIVE agent      → agent-centric: request to it.
//   - else the channel HAS a boost floor:
//     boost live → failover to boost;  boost down → channel CANNOT serve (503).
//   - else (no boost AND no default set)         → group-chat: broadcast to humans.
//   - else (no boost, default set but down)      → no reachable brain (503).
//
// "cannot serve" / "no brain" are per-request conditions for the SENDING user —
// returned as a routingError, NEVER written as a channel envelope. An introduced
// boost floor means the channel is meant to always have a brain, so a dead default
// never silently degrades to group-chat (that is only the boost-less, default-less
// channel's intent).
func (h *humanFront) resolveRouting(ctx context.Context, in submitInput) ([]actor.ActorID, message.Kind, error) {
	audience := make([]actor.ActorID, 0, len(in.Audience))
	for _, a := range in.Audience {
		audience = append(audience, actor.ActorID(a))
	}
	kind := message.Kind(in.Kind)
	if len(audience) > 0 {
		return audience, kind, nil
	}

	home := h.app.getHome(h.chID)
	if home == nil {
		return nil, kind, &routingError{detail: "channel not loaded"}
	}

	var da string
	_ = h.app.db.QueryRowContext(ctx,
		`SELECT COALESCE(default_agent, '') FROM channels WHERE id = ?`, string(h.chID)).Scan(&da)

	actors, lerr := home.View().ListActors(ctx)
	if lerr != nil {
		// fail closed: do not silently downgrade routing on a transient view failure.
		return nil, kind, lerr
	}
	daLive := false
	if da != "" {
		for _, ac := range actors {
			if string(ac.ID) == da && ac.Kind == actor.KindAgent {
				daLive = true
				break
			}
		}
	}
	if daLive {
		return []actor.ActorID{actor.ActorID(da)}, message.KindRequest, nil
	}

	boostID := string(defaultAgentInstanceID)
	boostLive := false
	for _, ac := range actors {
		if string(ac.ID) == boostID && ac.Kind == actor.KindAgent {
			boostLive = true
			break
		}
	}
	hasBoost, berr := h.app.channelHasInstance(ctx, string(h.chID), boostID)
	if berr != nil {
		return nil, kind, berr
	}
	switch {
	case hasBoost && boostLive:
		return []actor.ActorID{defaultAgentInstanceID}, message.KindRequest, nil
	case hasBoost:
		// boost floor introduced but down → channel cannot serve.
		return nil, kind, &routingError{detail: "the channel's default/fallback agent is down"}
	case da == "":
		// no floor + no default → pure group-chat: broadcast to humans.
		for _, ac := range actors {
			if ac.Kind == actor.KindHuman {
				audience = append(audience, ac.ID)
			}
		}
		return audience, message.KindEvent, nil
	default:
		// da was set but its brain is down, and no boost floor exists.
		return nil, kind, &routingError{detail: "the channel's default agent is down and no fallback is configured"}
	}
}

// clientRequestTTLMs is the default TTL for client-sent messages (product
// decision, lives in the app layer).
const clientRequestTTLMs int64 = 30_000

// newClientEnvelope builds a message.Envelope from the user's raw intent, filling
// product defaults (ID, Kind, TTL). Identity is NOT filled: ChannelID and Sender
// are left zero for the pen to weld at write time (sealed-pen: identity is
// substrate-injected, not caller-settable). Kind defaults to request; the sender's
// human KIND rides as the envelope's declared kind but step 4 force-overwrites it
// from the registry, so it is advisory here.
func (h *humanFront) newClientEnvelope(in submitInput, kind message.Kind, audience []actor.ActorID) *message.Envelope {
	now := time.Now().UnixMilli()
	exp := now + clientRequestTTLMs

	envID := message.ID(in.ID)
	if envID == "" {
		envID = message.ID(uuid.NewString())
	}
	if kind == "" {
		kind = message.KindRequest
	}

	aud := make(message.Audience, 0, len(audience))
	aud = append(aud, audience...)

	return &message.Envelope{
		ID:   envID,
		TS:   now,
		Kind: kind,
		Type: in.Type,
		// Sender left zero: identity is substrate-injected (pen welds id, step 4
		// welds kind from registry) — not caller-settable. Filling it here is
		// harmless (fail-fast only guards id/channel) but off-spec, so leave empty.
		Audience:  aud,
		Payload:   in.Payload,
		ExpiresAt: &exp,
	}
}

// ---------------------------------------------------------------------------
// App-side human front-end index + admission
// ---------------------------------------------------------------------------

// humanFor returns the user's live write front-end in the channel, ensuring it is
// admitted (membership + cell + welded pen) first. The cell is HOME-SCOPED: it is
// spawned on demand (the user's first write or any write after a home reload) and
// lives for the home's lifetime — admission re-applies membership idempotently, so
// a user who only had workspace access becomes a channel member at first write.
// The HTTP handler gets back only this domain front-end (SubmitIntent), never a
// pen and never a Minter.
func (a *App) humanFor(ctx context.Context, chID channel.ID, userID string) (*humanFront, error) {
	self := actor.ActorID("user:" + userID)

	// Fast path: a live front-end is already indexed.
	a.mu.RLock()
	if byUser := a.humans[chID]; byUser != nil {
		if hf := byUser[self]; hf != nil {
			a.mu.RUnlock()
			return hf, nil
		}
	}
	a.mu.RUnlock()

	// Slow path: admission is SERIALIZED under the write lock so AT MOST ONE
	// goroutine ever Spawns a given (chID, self). Without this, two concurrent first
	// sends (double-click / retry / two tabs) would each Spawn the same id; the
	// substrate's replace-on-same-id (runtime.Spawn) stops the loser's cell, and a
	// naive post-Spawn re-check could index that now-Stopped (closed) front —
	// permanently wedging the user's send path on context.Canceled. Holding the lock
	// across check + home-lookup + Spawn + index makes the loser observe the winner's
	// indexed (live) front and skip Spawn entirely, and makes admission atomic w.r.t.
	// forgetHumans teardown (no stale-ref TOCTOU: the home read and the index write
	// are under the same lock the teardown takes).
	a.mu.Lock()
	defer a.mu.Unlock()

	// Re-check under the write lock: a concurrent admission may have won.
	if byUser := a.humans[chID]; byUser != nil {
		if hf := byUser[self]; hf != nil {
			return hf, nil
		}
	}

	home := a.homes[chID]
	if home == nil {
		return nil, errChannelNotLoaded
	}

	// Build the front-end inside the Spawn factory so it is born with a pen welded
	// to its own user id (same admission path as agents/tools — no PenFor side
	// door). The factory captures the ref the substrate does not hand back.
	var built *humanFront
	factory := func(caps actorcaps.Caps) actorrt.Actor {
		built = &humanFront{app: a, chID: chID, pen: caps.Pen}
		return built
	}
	if err := home.Spawn(ctx, self, actor.KindHuman, factory); err != nil {
		return nil, err
	}

	byUser := a.humans[chID]
	if byUser == nil {
		byUser = make(map[actor.ActorID]*humanFront)
		a.humans[chID] = byUser
	}
	byUser[self] = built
	return built, nil
}

// forgetHumans drops a channel's human front-end refs (the home is being torn
// down; the cells go with it). The caller holds a.mu.
func (a *App) forgetHumans(chID channel.ID) {
	delete(a.humans, chID)
}
