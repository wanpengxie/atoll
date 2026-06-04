// Package sysactor is the channel's cross-cutting physical-state actor. It is
// the single owner of the channel's ephemeral PRESENCE state (compute lease:
// who is physically online) and answers the channel-wide directory query
// (actor.list) as a composed view (membership ∧ presence). It is ADVISORY —
// never a dispatch gate (P15/P16): reachability authority is send→terminal, and
// the dispatch path never reads this actor's view. It runs as a channel固有
// cell, spawned once per channel at channel creation time.
//
// Two-axis model (runtime/storespec.Record): membership is durable registry
// truth; PRESENCE is volatile and AUTHORITY-OWNED (the component holding the
// compute leases/connections). The system actor does NOT keep a presence copy —
// it READS presence via an injected seam when composing actor.list, exactly as
// it reads membership from the Registry. Presence is never a message and never
// truth — a volatile state read. readiness is NOT a third axis — whether an
// actor can service a request is the OUTCOME of send→terminal, not a stored
// state the system actor projects or composes.
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

// PresenceStat is the injected obs-read seam: the substrate's AUTHORITATIVE
// presence + bind-instant for an actor (present = the bool, uptime = now -
// startedAt). It is no longer a domain abstraction the system actor invents; it
// reads the substrate obs authority. Defined consumer-side (Go idiom) as the
// NARROW shape this actor needs — so the composition root supplies a thin
// adapter over actorrt.Runtime.Stat (which returns a UnitStat bundle):
//
//	func(id) (time.Time, bool) { st, ok := rt.Stat(id); return st.StartedAt, ok }
//
// A nil seam (not yet wired) reports everyone absent — advisory, never a
// dispatch gate.
type PresenceStat interface {
	Stat(id actor.ActorID) (startedAt time.Time, present bool)
}

// SystemActor answers channel-wide directory queries (actor.list) by composing
// durable membership (Registry) with volatile presence (the injected seam).
type SystemActor struct {
	channelID channel.ID
	registry  storespec.Registry
	chain     rtharness.Writer
	lookup    behavior.RequestLookup
	clock     func() time.Time
	stat      PresenceStat
}

// Deps bundles the channel services the system actor needs.
type Deps struct {
	ChannelID channel.ID
	Registry  storespec.Registry
	Chain     rtharness.Writer
	Lookup    behavior.RequestLookup
	Clock     func() time.Time
	// Stat is the obs-read seam backed by Runtime.Stat (substrate-authoritative
	// presence + bind-instant). Nil → actor.list reports everyone absent.
	Stat PresenceStat
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
		stat:      deps.Stat,
	}
}

// Receive handles one envelope serially (implements runtime/actorrt.Actor).
func (s *SystemActor) Receive(ctx context.Context, env *message.Envelope) error {
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
		present, uptimeMs := s.obs(r.ID)
		catalog.Actors = append(catalog.Actors, introspect.CatalogEntry{
			ID:       string(r.ID),
			Kind:     string(r.Kind),
			Binding:  string(r.Binding),
			Present:  present,
			UptimeMs: uptimeMs,
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

// obs reads the substrate's authoritative obs for id (advisory; NOT a dispatch
// gate): present, and uptime derived as now - startedAt. A nil seam (not yet
// wired) reports absent / zero uptime.
func (s *SystemActor) obs(id actor.ActorID) (present bool, uptimeMs int64) {
	if s.stat == nil {
		return false, 0
	}
	startedAt, present := s.stat.Stat(id)
	if !present {
		return false, 0
	}
	if !startedAt.IsZero() {
		uptimeMs = s.clock().Sub(startedAt).Milliseconds()
	}
	return true, uptimeMs
}
