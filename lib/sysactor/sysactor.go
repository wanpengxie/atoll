// Package sysactor is the channel's cross-cutting physical-state actor. It is
// the single owner of the channel's ephemeral physical state (presence / lease
// / a composed callable catalog) and its business-readiness projection, exposed
// ONLY through messages (P18). It is ADVISORY — never a dispatch gate (P15/P16):
// reachability authority is send→terminal, and the dispatch path never reads
// this actor's view. It runs as a channel固有 cell on the channel home
// (server, v2). Collapse of system-actor-adapter-management.md mechanism/policy
// separation: the system actor holds policy STATE; the substrate just delivers.
package sysactor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
)

// presenceEntry is the ephemeral physical-layer state for one actor, fed by a
// compute lease report over wire/placement (v2). Lease semantics: a fresh
// report extends leaseExpiresAt; expiry means "presence gone" (k8s node-lease).
type presenceEntry struct {
	present        bool
	leaseExpiresAt int64
}

// SystemActor answers channel-wide directory queries (actor.list) and ingests
// readiness/presence change events to maintain its advisory view.
type SystemActor struct {
	self      actor.ActorID
	channelID channel.ID
	registry  actor.Registry
	chain     harness.Chain
	lookup    message.RequestLookup
	clock     func() time.Time

	// presence is ephemeral physical state (lease-driven). readiness is the
	// business-readiness projection (event-driven). Both are plain maps — the
	// cell goroutine is the sole owner (no lock).
	presence  map[actor.ActorID]presenceEntry
	readiness map[actor.ActorID]actor.Readiness
}

// Deps bundles the channel services the system actor needs.
type Deps struct {
	ChannelID channel.ID
	Registry  actor.Registry
	Chain     harness.Chain
	Lookup    message.RequestLookup
	Clock     func() time.Time
}

// New constructs the channel system actor cell.
func New(deps Deps) *SystemActor {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	return &SystemActor{
		self:      actor.SystemActorID,
		channelID: deps.ChannelID,
		registry:  deps.Registry,
		chain:     deps.Chain,
		lookup:    deps.Lookup,
		clock:     clock,
		presence:  map[actor.ActorID]presenceEntry{},
		readiness: map[actor.ActorID]actor.Readiness{},
	}
}

// Receive handles one envelope serially (implements runtime/actorrt.Actor).
func (s *SystemActor) Receive(ctx context.Context, env *message.Envelope) error {
	switch env.Kind {
	case message.KindRequest:
		if env.Type == "actor.list" {
			return s.respondList(ctx, env)
		}
		// Unknown reserved request: leave for the caller's caller-scoped
		// closure to time out (the system actor does not synthesize).
		return nil
	case message.KindEvent:
		switch env.Type {
		case "actor.readiness.changed":
			s.applyReadiness(env)
		case "actor.presence.changed":
			s.applyPresence(env)
		}
		return nil
	default:
		return nil
	}
}

// respondList answers actor.list with a composed channel-wide catalog
// (membership from the registry ∧ presence ∧ readiness — composed INSIDE the
// actor so the channel only sees the result, never the raw副本).
func (s *SystemActor) respondList(ctx context.Context, env *message.Envelope) error {
	rows, err := s.registry.ListActive(ctx)
	if err != nil {
		return err
	}
	catalog := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		// Readiness comes from the AUTHORITATIVE registry projection (write side:
		// the adapter persists it via ReadinessUpdater). The actor.readiness.changed
		// event is SystemOnly so the adapter can't push it here; the registry row
		// is the truth. Fall back to the ingested event map for actors whose
		// readiness arrives as a system-emitted event (none today).
		rd := r.Readiness
		if rd.State == "" {
			rd = s.readiness[r.ID]
		}
		catalog = append(catalog, map[string]any{
			"id":        string(r.ID),
			"kind":      string(r.Kind),
			"binding":   string(r.Binding),
			"readiness": string(rd.Normalize().State),
			"present":   s.isPresent(r.ID),
		})
	}
	payload, err := json.Marshal(map[string]any{"actors": catalog})
	if err != nil {
		return err
	}
	resp, err := behavior.BuildResponseEnvelope(ctx, s.lookup, s.clock,
		message.Sender{Kind: actor.KindSystem, ID: s.self},
		behavior.CorrelationKey(env.ID),
		behavior.ResponseSpec{Status: "completed", Payload: payload})
	if err != nil {
		return err
	}
	// Stamp the system actor's own caller identity so the harness ACL
	// authenticates the write (without it the response is rejected as
	// harness_engine_acl_denied and the caller never sees the catalog).
	cctx := rtharness.CtxWithCaller(ctx, rtharness.CallerContext{
		ActorID: s.self, ChannelID: s.channelID, AllowProvidedSenderKind: true,
	})
	_, err = s.chain.Write(cctx, resp)
	return err
}

// applyReadiness folds an actor.readiness.changed event into the projection.
func (s *SystemActor) applyReadiness(env *message.Envelope) {
	var body struct {
		State  string `json:"state"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(env.Payload, &body); err != nil {
		return
	}
	s.readiness[env.Sender.ID] = actor.Readiness{
		State:  actor.ReadinessState(body.State),
		Reason: body.Reason,
	}.Normalize()
}

// applyPresence folds a presence lease report (compute → wire/placement → here)
// into the ephemeral physical view.
func (s *SystemActor) applyPresence(env *message.Envelope) {
	var body struct {
		Actor    string `json:"actor"`
		Present  bool   `json:"present"`
		LeaseTTL int64  `json:"lease_ttl_ms"`
	}
	if err := json.Unmarshal(env.Payload, &body); err != nil || body.Actor == "" {
		return
	}
	// SystemOnly event → sender is the system actor; the subject actor is in the
	// payload (keyed there, not by sender).
	s.presence[actor.ActorID(body.Actor)] = presenceEntry{
		present:        body.Present,
		leaseExpiresAt: s.clock().UnixMilli() + body.LeaseTTL,
	}
}

// isPresent is the advisory presence read (lease-fresh). NOT a dispatch gate.
func (s *SystemActor) isPresent(id actor.ActorID) bool {
	e, ok := s.presence[id]
	if !ok {
		return false
	}
	return e.present && s.clock().UnixMilli() < e.leaseExpiresAt
}
