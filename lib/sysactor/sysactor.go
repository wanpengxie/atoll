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
	"fmt"
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
	writer    behavior.ResponseWriter
	lookup    behavior.RequestLookup
	clock     func() time.Time
	stat      PresenceStat
}

// sender is the system actor's own identity — stamped on every serve write it
// authors (P12: kind by identity, not hard-coded). behavior.Respond carries it
// so the answer is kind-neutral at the base.
var sysSender = message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}

// callerWriter wraps the runtime harness write chain into a behavior.ResponseWriter,
// stamping the system actor's caller context so the harness ACL authenticates the
// write (Step 0/1) and mapping harness_terminal_duplicate to the pure
// WriteOutcome.Duplicate bool — the reject vocabulary stays in runtime, behavior
// reads only the bool. This is the runtime-type → pure-seam bridge the system
// actor needs to call behavior.Respond; it lives here because sysactor already
// imports runtime/harness (§4 keeps the same adapter for channelkit's closure).
type callerWriter struct {
	chain     rtharness.Writer
	channelID channel.ID
}

func (w callerWriter) Write(ctx context.Context, env *message.Envelope) (behavior.WriteOutcome, error) {
	cctx := rtharness.CtxWithCaller(ctx, rtharness.CallerContext{
		ActorID: actor.SystemActorID, ChannelID: w.channelID,
	})
	res, err := w.chain.Write(cctx, env)
	if err != nil {
		return behavior.WriteOutcome{}, err
	}
	return behavior.WriteOutcome{
		MessageID:    res.MessageID,
		Duplicate:    res.RejectReason == rtharness.HarnessTerminalDuplicate,
		RejectReason: string(res.RejectReason),
		RejectDetail: res.RejectDetail,
	}, nil
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
		writer:    callerWriter{chain: deps.Chain, channelID: deps.ChannelID},
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

// Describe implements introspect.Describer — the standard self-describe hook.
// The system actor declares its live API surface (the channel directory query)
// through the SAME convention every actor honours, rather than hand-rolling the
// API list at the serve site; a generic host serving describe on behalf of
// arbitrary actors consults this exact hook.
func (s *SystemActor) Describe(ctx context.Context) ([]introspect.APIDescriptor, error) {
	return []introspect.APIDescriptor{{
		Name: introspect.QueryList,
		Desc: "channel-wide actor directory: membership ∧ presence",
	}}, nil
}

// respondDescribe self-answers the reserved actor.describe for the system actor
// itself: identity + API surface, assembled through introspect.BuildDescribe
// (which honours the Describer hook above) so the answer never drifts from the
// convention. Like every actor, it must answer the reserved self-query rather
// than let the caller hang.
func (s *SystemActor) respondDescribe(ctx context.Context, env *message.Envelope) error {
	desc, err := introspect.BuildDescribe(ctx, string(actor.SystemActorID), s)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(desc)
	if err != nil {
		return err
	}
	return s.respondReserved(ctx, env, payload)
}

// respondReserved answers a reserved self-query (actor.list / actor.describe)
// with a system-authored completed response carrying payload. It recovers the
// original request via the injected RequestLookup (the serve-side truth read)
// and delegates the build+stamp+write to behavior.Respond (author#1, ONE
// implementation — no hand-rolled serve write here). sender = the system actor's
// own identity; the injected writer stamps the system caller context so the
// harness ACL authenticates the write.
func (s *SystemActor) respondReserved(ctx context.Context, env *message.Envelope, payload []byte) error {
	request, ok, err := s.lookup.FindByID(ctx, env.ID)
	if err != nil {
		return err
	}
	if !ok || request == nil {
		return fmt.Errorf("sysactor: reserved request %s not found", env.ID)
	}
	_, err = behavior.Respond(ctx, s.writer, s.clock, request, sysSender,
		behavior.ResponseSpec{Status: "completed", Payload: payload})
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
