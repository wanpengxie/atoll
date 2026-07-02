package sysactor

import (
	"context"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// LivenessStat is the injected obs-read seam: the consumer-side narrow read of
// the substrate's AUTHORITATIVE liveness + bind-instant for an actor (present =
// the bool, uptime = now - startedAt). Defined consumer-side (Go idiom) as the
// NARROW shape this actor needs, so the composition root supplies a thin reader
// over the substrate's pull-stat obs seam. A nil seam (not yet wired) reports
// everyone absent — advisory, never a dispatch gate.
type LivenessStat interface {
	Stat(id actor.ActorID) (startedAt time.Time, present bool)
}

// DevicePresenceStat is the injected obs-read seam over the home device-presence fold:
// the latest folded L3 device-presence snapshot for an actor (the actor-source
// obs PUSH a device adapter published). known=false = UNKNOWN (not a device
// adapter, no signal, or decayed) — NOT offline. Defined consumer-side (narrow);
// a nil seam means no device column (everyone unknown). Advisory only —
// authoritative reachability is send→terminal.
type DevicePresenceStat interface {
	Device(id actor.ActorID) (snapshot []byte, known bool)
}

// SystemActor answers channel-wide directory queries (actor.list) by composing
// durable membership (Registry) with volatile liveness (the injected seam). It
// is channel-agnostic at the base — the composition root injects channel-scoped
// services (registry, writer, lookup), so this actor holds no channel id of its
// own.
type SystemActor struct {
	registry storespec.Registry
	writer   harness.Pen
	lookup   storespec.RequestLookup
	clock    func() time.Time
	stat     LivenessStat
	device   DevicePresenceStat
}

// Deps bundles the channel services the system actor needs.
type Deps struct {
	Registry storespec.Registry
	// Writer commits the serve response into truth. The composition root injects a
	// system Pen (Mint(SystemActorID, chID)): the identity is welded into the pen,
	// so the system actor never passes a sender — every write it authors is system-
	// authored by construction.
	Writer harness.Pen
	Lookup storespec.RequestLookup
	Clock  func() time.Time
	// Stat is the obs-read seam over the substrate's authoritative liveness +
	// bind-instant. Nil → actor.list reports everyone absent.
	Stat LivenessStat
	// Device is the obs-read seam over the home device-presence fold (L3 device presence).
	// Nil → actor.list omits the device column (everyone unknown).
	Device DevicePresenceStat
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
		device:   deps.Device,
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
// (the membership ∧ liveness formula owned by introspect.QueryList), composed
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
			Device:   s.deviceObs(r.ID),
		})
	}
	return s.respondReserved(ctx, env, catalog)
}

// systemDescribe is the system actor's self-answer in the introspect contract
// shape: identity + the reserved directory query it serves. Declared through
// the SAME convention every actor honours, rather than hand-rolling the API
// list at the serve site.
func systemDescribe() introspect.Describe {
	return introspect.Describe{
		ActorID:     string(actor.SystemActorID),
		Description: "Channel system actor: answers the reserved directory query actor.list (membership ∧ liveness).",
		SkillDoc: "# system\n\nReserved channel directory.\n\n## Tool surface\n\n" +
			"- `actor.list` — channel-wide actor directory: durable membership composed with liveness.\n",
		Types: map[string]introspect.TypeMeta{
			introspect.QueryList: {
				Description:  "channel-wide actor directory: membership ∧ liveness",
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
	return s.respondReserved(ctx, env, answer)
}

// respondReserved answers a reserved self-query (actor.list / actor.describe)
// with a system-authored completed response carrying result. It recovers the
// original request via the injected RequestLookup (the serve-side truth read)
// and delegates marshal+build+stamp+write to behavior.RespondJSON (the ONE
// marshal+respond home — no hand-rolled json.Marshal at the serve site). The
// injected pen is welded to the system identity (Mint(SystemActorID)), so the
// response is system-authored by construction — no sender passed.
func (s *SystemActor) respondReserved(ctx context.Context, env *message.Envelope, result any) error {
	request, ok, err := s.lookup.FindByID(ctx, env.ID)
	if err != nil {
		return err
	}
	if !ok || request == nil {
		return fmt.Errorf("sysactor: reserved request %s not found", env.ID)
	}
	_, err = behavior.RespondJSON(ctx, s.writer, s.clock, request, result)
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

// deviceObs reads the actor's L3 device presence from the injected fold seam
// (advisory; NOT a dispatch gate). nil = UNKNOWN (no seam, never reported, or
// decayed) — the actor.list omits the device column rather than asserting offline.
func (s *SystemActor) deviceObs(id actor.ActorID) *introspect.DevicePresence {
	if s.device == nil {
		return nil
	}
	raw, known := s.device.Device(id)
	if !known {
		return nil
	}
	p, ok := introspect.ParseDevicePresence(raw)
	if !ok {
		return nil
	}
	return &p
}
