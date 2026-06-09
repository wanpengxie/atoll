// Package sysactor is the channel's cross-cutting physical-state actor. It is
// the single owner of the channel's ephemeral PRESENCE state (compute lease:
// who is physically online) and answers the channel-wide directory query
// (actor.list) as a composed view (membership ∧ presence, the formula owned by
// introspect.QueryList). It is ADVISORY — never a dispatch gate (P15/P16):
// reachability authority is send→terminal, and the dispatch path never reads
// this actor's view. It runs as a channel-intrinsic cell, spawned once per
// channel at channel creation time.
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

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// PresenceStat is the injected obs-read seam: the consumer-side narrow read of
// the substrate's AUTHORITATIVE presence + bind-instant for an actor (present =
// the bool, uptime = now - startedAt). Defined consumer-side (Go idiom) as the
// NARROW shape this actor needs, so the composition root supplies a thin reader
// over the substrate's pull-stat obs seam. A nil seam (not yet wired) reports
// everyone absent — advisory, never a dispatch gate.
type PresenceStat interface {
	Stat(id actor.ActorID) (startedAt time.Time, present bool)
}

// SystemActor answers channel-wide directory queries (actor.list) by composing
// durable membership (Registry) with volatile presence (the injected seam). It
// is channel-agnostic at the base — the composition root injects channel-scoped
// services (registry, writer, lookup), so this actor holds no channel id of its
// own.
type SystemActor struct {
	registry storespec.Registry
	writer harness.Writer
	lookup storespec.RequestLookup
	clock    func() time.Time
	stat     PresenceStat
}

// sysSender is the system actor's own identity — stamped on every serve write it
// authors (P12: kind by identity, not hard-coded). behavior.Respond carries it
// so the answer is kind-neutral at the base.
var sysSender = message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID}

// Deps bundles the channel services the system actor needs.
type Deps struct {
	Registry storespec.Registry
	// Writer commits the serve response into truth. The composition root injects a
	// harness.Writer already stamped with the system caller context (so the harness
	// ACL authenticates the write).
	Writer harness.Writer
	Lookup storespec.RequestLookup
	Clock  func() time.Time
	// Stat is the obs-read seam over the substrate's authoritative presence +
	// bind-instant. Nil → actor.list reports everyone absent.
	Stat PresenceStat
}

// New constructs the channel system actor cell.
func New(deps Deps) *SystemActor {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	return &SystemActor{
		registry: deps.Registry,
		writer:   deps.Writer,
		lookup:   deps.Lookup,
		clock:    clock,
		stat:     deps.Stat,
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
// (the membership ∧ presence formula owned by introspect.QueryList), composed
// INSIDE the actor so the channel only sees the result, never the raw rows.
// Readiness is deliberately absent: it is not a substrate axis — whether an actor can service a request
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

// systemDescribe is the system actor's self-answer in the introspect contract
// shape: identity + the reserved directory query it serves. Declared through
// the SAME convention every actor honours, rather than hand-rolling the API
// list at the serve site.
func systemDescribe() introspect.Describe {
	return introspect.Describe{
		ActorID:     string(actor.SystemActorID),
		Description: "Channel system actor: answers the reserved directory query actor.list (membership ∧ presence).",
		SkillDoc: "# system\n\nReserved channel directory.\n\n## Tool surface\n\n" +
			"- `actor.list` — channel-wide actor directory: durable membership composed with live presence.\n",
		Types: map[string]introspect.TypeMeta{
			introspect.QueryList: {
				Description:  "channel-wide actor directory: membership ∧ presence",
				AllowedKinds: []string{string(message.KindRequest)},
			},
		},
	}
}

// respondDescribe self-answers the reserved actor.describe for the system actor
// itself through the standard introspect dispatch (full answer or single-type
// selector). Like every actor, it must answer the reserved self-query rather
// than let the caller hang. A malformed or unknown selector is NOT synthesized
// (this actor's stated philosophy): the caller's closure reaps it.
func (s *SystemActor) respondDescribe(ctx context.Context, env *message.Envelope) error {
	req, err := introspect.ParseDescribeRequest(env.Payload)
	if err != nil {
		return nil
	}
	answer, ok := introspect.AnswerDescribe(systemDescribe(), req)
	if !ok {
		return nil
	}
	payload, err := json.Marshal(answer)
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
