// Package sysactor is the channel's cross-cutting physical-state actor. It is
// the single owner of the channel's ephemeral PRESENCE state (compute lease:
// who is physically online) and answers the channel-wide directory query
// (actor.list) as a composed view (membership ∧ presence). It is ADVISORY —
// never a dispatch gate (P15/P16): reachability authority is send→terminal, and
// the dispatch path never reads this actor's view. It runs as a channel固有
// cell on the channel home (server, v2).
//
// Two-axis model (runtime/storespec.Record): membership is durable server truth
// (the registry); PRESENCE is volatile and lives HERE, never in the truth log
// (fleet Delivers lease reports straight to this cell's mailbox, bypassing the
// harness). readiness is NOT a third axis — an actor's business serviceable
// state is self-managed and answered by that actor's own actor.status; the
// system actor does not project or compose it.
package sysactor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
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

// PresenceReport is one compute lease report about an actor's volatile physical
// presence. The fleet (server-side, tracking compute leases) folds it onto the
// channel system actor's cell via cells.Deliver(NewPresenceSignal(r)) — an
// INTERNAL control signal that NEVER enters the truth log (presence is volatile,
// not a channel event; the harness is never asked to write it). It travels
// compute → server fleet → this cell, so the json tags are wire-stable.
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

// NewPresenceSignal builds the internal control envelope the fleet delivers to
// fold a presence report onto this channel's system actor cell. The subject
// actor rides in the payload (not the sender — the report is delivered on the
// subject's behalf).
func NewPresenceSignal(r PresenceReport) *message.Envelope {
	payload, _ := json.Marshal(r)
	return &message.Envelope{Kind: message.KindEvent, Type: presenceSignalType, Payload: payload}
}

// Receive handles one envelope serially (implements runtime/actorrt.Actor).
func (s *SystemActor) Receive(ctx context.Context, env *message.Envelope) error {
	// Internal presence control signal (fleet-delivered to the mailbox, never
	// truth). Folded serially on the cell goroutine like any other message.
	if env.Type == presenceSignalType {
		s.applyPresence(env)
		return nil
	}
	if env.Kind == message.KindRequest && env.Type == actor.ReservedActorList {
		return s.respondList(ctx, env)
	}
	// Anything else (other reserved requests, stray events): the system actor
	// does not synthesize — a request is left for the caller's caller-scoped
	// closure to time out.
	return nil
}

// respondList answers actor.list with a composed channel-wide directory
// (membership from the registry ∧ presence — composed INSIDE the actor so the
// channel only sees the result, never the raw副本). Readiness is deliberately
// absent: it is not a substrate axis; ask an actor's own actor.status for its
// serviceable state.
func (s *SystemActor) respondList(ctx context.Context, env *message.Envelope) error {
	rows, err := s.registry.ListActive(ctx)
	if err != nil {
		return err
	}
	catalog := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		catalog = append(catalog, map[string]any{
			"id":      string(r.ID),
			"kind":    string(r.Kind),
			"binding": string(r.Binding),
			"present": s.isPresent(r.ID),
		})
	}
	payload, err := json.Marshal(map[string]any{"actors": catalog})
	if err != nil {
		return err
	}
	resp, err := behavior.BuildResponseEnvelope(ctx, s.lookup, s.clock,
		message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		behavior.CorrelationKey(env.ID),
		behavior.ResponseSpec{Status: "completed", Payload: payload})
	if err != nil {
		return err
	}
	// Stamp the system actor's own caller identity so the harness ACL
	// authenticates the write (without it the response is rejected as
	// harness_engine_acl_denied and the caller never sees the catalog).
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
