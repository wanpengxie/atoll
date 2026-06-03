// Package sysactor is the channel's cross-cutting physical-state actor. It is
// the single owner of the channel's ephemeral PRESENCE state (compute lease:
// who is physically online) and answers the channel-wide directory query
// (actor.list) as a composed view (membership ∧ presence). It is ADVISORY —
// never a dispatch gate (P15/P16): reachability authority is send→terminal, and
// the dispatch path never reads this actor's view. It runs as a channel固有
// cell, spawned once per channel at channel creation time.
//
// Two-axis model (runtime/storespec.Record): membership is durable registry
// truth; PRESENCE is volatile and lives HERE, never in the truth log (lease
// reports are delivered straight to this cell's mailbox, bypassing the
// harness). readiness is NOT a third axis — whether an actor can service a
// request is the OUTCOME of send→terminal, not a state the system actor
// projects or composes.
package sysactor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// SystemActor answers channel-wide directory queries (actor.list) and ingests
// presence lease reports to maintain its advisory physical view.
type SystemActor struct {
	channelID channel.ID
	registry  storespec.Registry
	chain     rtharness.Writer
	lookup    behavior.RequestLookup
	clock     func() time.Time

	// presence is ephemeral physical state: actorID → lease-expiry (ms epoch).
	// Presence is a single fact — a fresh lease (k8s node-lease semantics).
	// "Absent" is one representation: no entry (a not-present report deletes it,
	// expiry is read as gone). Plain map — the cell goroutine is the sole owner
	// (no lock).
	presence map[actor.ActorID]int64
}

// Deps bundles the channel services the system actor needs.
type Deps struct {
	ChannelID channel.ID
	Registry  storespec.Registry
	Chain     rtharness.Writer
	Lookup    behavior.RequestLookup
	Clock     func() time.Time
}

// New constructs the channel system actor cell.
func New(deps Deps) *SystemActor {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	return &SystemActor{
		channelID: deps.ChannelID,
		registry:  deps.Registry,
		chain:     deps.Chain,
		lookup:    deps.Lookup,
		clock:     clock,
		presence:  map[actor.ActorID]int64{},
	}
}

// PresenceReport is one compute-lease report about an actor's volatile physical
// presence: whether the actor holds a live lease, and for how long. It is folded
// onto this cell via cells.Deliver(NewPresenceSignal(r)) — an INTERNAL control
// signal that NEVER enters the truth log (presence is volatile, not a channel
// event; the harness is never asked to write it). The json tags are stable
// because the report is marshalled to/from an envelope payload.
type PresenceReport struct {
	Actor      actor.ActorID `json:"actor"`
	Present    bool          `json:"present"`
	LeaseTTLMs int64         `json:"lease_ttl_ms"`
}

// presenceSignalType is the internal control-signal envelope type carrying a
// PresenceReport onto the cell goroutine. Deliberately OUTSIDE the reserved
// actor.*/system.* namespace and never written to truth — it travels only via
// cells.Deliver (the mailbox), folded serially by Receive.
const presenceSignalType = "sysactor.__presence__"

// NewPresenceSignal wraps a PresenceReport into the internal control envelope
// for direct delivery to this cell's mailbox (bypassing the truth log). The
// subject actor rides in the payload (not the sender — the report is delivered
// on the subject's behalf).
func NewPresenceSignal(r PresenceReport) *message.Envelope {
	payload, _ := json.Marshal(r)
	return &message.Envelope{Kind: message.KindEvent, Type: presenceSignalType, Payload: payload}
}

// Receive handles one envelope serially (implements runtime/actorrt.Actor).
func (s *SystemActor) Receive(ctx context.Context, env *message.Envelope) error {
	// Internal presence control signal (direct mailbox delivery, never written
	// to truth). Folded serially on the cell goroutine like any other message.
	if env.Type == presenceSignalType {
		s.applyPresence(env)
		return nil
	}
	if env.Kind == message.KindRequest {
		switch env.Type {
		case introspect.QueryList:
			return s.respondList(ctx, env)
		case introspect.QueryDescribe:
			// The system actor is itself an actor: it self-answers the reserved
			// actor.describe so the reserved surface is complete (no actor times
			// out on a self-query).
			return s.respondDescribe(ctx, env)
		}
	}
	// Anything else (other reserved requests, stray events): the system actor
	// does not synthesize — a request is left for the caller's caller-scoped
	// closure to time out.
	return nil
}

// respondList answers actor.list with a composed channel-wide directory
// (membership from the registry ∧ presence — composed INSIDE the actor so the
// channel only sees the result, never the raw副本). Readiness is deliberately
// absent: it is not a substrate axis — whether an actor can service a request
// is the OUTCOME of send→terminal, never a stored field here.
func (s *SystemActor) respondList(ctx context.Context, env *message.Envelope) error {
	rows, err := s.registry.ListActive(ctx)
	if err != nil {
		return err
	}
	catalog := introspect.Catalog{Actors: make([]introspect.CatalogEntry, 0, len(rows))}
	for _, r := range rows {
		catalog.Actors = append(catalog.Actors, introspect.CatalogEntry{
			ID:      string(r.ID),
			Kind:    string(r.Kind),
			Binding: string(r.Binding),
			Present: s.isPresent(r.ID),
		})
	}
	payload, err := json.Marshal(catalog)
	if err != nil {
		return err
	}
	return s.respondReserved(ctx, env, payload)
}

// respondDescribe self-answers the reserved actor.describe for the system actor
// itself: its identity plus the API it exposes (the channel directory query).
// Like every actor, it must answer the reserved self-query rather than let the
// caller hang.
func (s *SystemActor) respondDescribe(ctx context.Context, env *message.Envelope) error {
	desc := introspect.Describe{
		Name: string(actor.SystemActorID),
		APIs: []introspect.APIDescriptor{{
			Name: introspect.QueryList,
			Desc: "channel-wide actor directory: membership ∧ presence",
		}},
	}
	payload, err := json.Marshal(desc)
	if err != nil {
		return err
	}
	return s.respondReserved(ctx, env, payload)
}

// respondReserved writes a system-authored completed response carrying payload
// for a reserved self-query (actor.list / actor.describe). It stamps the system
// actor's own caller identity so the harness ACL authenticates the write —
// without it the response is rejected as harness_engine_acl_denied and the
// caller never sees the answer.
func (s *SystemActor) respondReserved(ctx context.Context, env *message.Envelope, payload []byte) error {
	resp, err := behavior.BuildResponseEnvelope(ctx, s.lookup, s.clock,
		message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		behavior.CorrelationKey(env.ID),
		behavior.ResponseSpec{Status: "completed", Payload: payload})
	if err != nil {
		return err
	}
	cctx := rtharness.CtxWithCaller(ctx, rtharness.CallerContext{
		ActorID: actor.SystemActorID, ChannelID: s.channelID,
	})
	_, err = s.chain.Write(cctx, resp)
	return err
}

// applyPresence folds a delivered PresenceReport into the ephemeral physical
// view (cell goroutine, no lock). The signal never enters the truth log.
func (s *SystemActor) applyPresence(env *message.Envelope) {
	var r PresenceReport
	if err := json.Unmarshal(env.Payload, &r); err != nil || r.Actor == "" {
		return
	}
	if !r.Present {
		delete(s.presence, r.Actor) // absent = no entry
		return
	}
	s.presence[r.Actor] = s.clock().UnixMilli() + r.LeaseTTLMs
}

// isPresent is the advisory presence read (lease-fresh). NOT a dispatch gate.
func (s *SystemActor) isPresent(id actor.ActorID) bool {
	exp, ok := s.presence[id]
	return ok && s.clock().UnixMilli() < exp
}
