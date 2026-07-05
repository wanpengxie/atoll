package sysactor

import (
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
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

// SystemActor holds one incarnation's process state: it answers channel-wide
// directory queries (actor.list) by composing durable membership (Registry)
// with volatile liveness (the injected seam). It is channel-agnostic at the
// base — the composition root injects channel-scoped services (registry,
// liveness/device seams), so this actor holds no channel id of its own.
//
// It is an actorbase Proc (lib/actorbase, actorbase-spec-v1 §3's out-generation
// matrix: sysactor is a ring0 special Proc — Caps hand-built raw by platform,
// not welded through the live membrane — but it enters through the SAME
// actorbase.New seam every other actor does). Def mints a fresh SystemActor per
// incarnation; run(sys) is the process body.
type SystemActor struct {
	registry storespec.Registry
	clock    func() time.Time
	stat     LivenessStat
	device   DevicePresenceStat
	operate  OperateExecutor
}

// Deps bundles the channel services the system actor needs.
type Deps struct {
	Registry storespec.Registry
	Clock    func() time.Time
	// Stat is the obs-read seam over the substrate's authoritative liveness +
	// bind-instant. Nil → actor.list reports everyone absent.
	Stat LivenessStat
	// Device is the obs-read seam over the home device-presence fold (L3 device presence).
	// Nil → actor.list omits the device column (everyone unknown).
	Device DevicePresenceStat
	// Operate is the injected channel-operate executor (the in-gate control plane's
	// implementation half; the gate here does permission + routing). Nil → the four
	// control types are inert (no synthesis) — the injection point is unfilled.
	Operate OperateExecutor
}

// New constructs the channel system actor's process state (exported so a
// composition root's own tests can drive it directly against a fake Sys; Def
// wraps this in the actorbase registration entry for production spawn).
func New(deps Deps) *SystemActor {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	return &SystemActor{
		registry: deps.Registry,
		clock:    clock,
		stat:     deps.Stat,
		device:   deps.Device,
		operate:  deps.Operate,
	}
}

// Def is sysactor's actorbase registration entry (spec §1.6): New mints a
// fresh SystemActor + run per incarnation, closing over deps.
func Def(deps Deps) actorbase.Def {
	return actorbase.Def{
		Doc: systemDescribe().Description,
		New: func() (actorbase.Proc, error) {
			return New(deps).run, nil
		},
	}
}

// run is the Proc body (spec §1.6): loop sys.Recv() until the cell dies or
// Stop is requested.
func (s *SystemActor) run(sys actorbase.Sys) error {
	for {
		msg, err := sys.Recv()
		if err != nil {
			return err
		}
		s.handle(sys, msg)
	}
}

// handle dispatches one delivered Msg (mirrors the former Receive).
func (s *SystemActor) handle(sys actorbase.Sys, msg actorbase.Msg) {
	if msg.Kind == message.KindRequest {
		switch msg.Type {
		case introspect.QueryList:
			s.respondList(sys, msg)
			return
		case introspect.QueryDescribe:
			// The system actor is itself an actor: it self-answers the reserved
			// actor.describe so the reserved surface is complete (no actor times
			// out on a self-query).
			s.respondDescribe(sys, msg)
			return
		case TypeIntroduceActor, TypeRemoveActor, TypeRestartActor, TypeSetDefaultAgent:
			// Channel operate face (NP-1=c): in-gate control plane. Permission +
			// routing here; the injected executor does the intent write + Home call.
			s.handleOperate(sys, msg)
			return
		}
	}
	// Anything else (other reserved requests, stray events): the system actor
	// does not synthesize — a request is left for the caller's caller-scoped
	// closure to time out.
}

// respondList answers actor.list with a composed channel-wide directory
// (the membership ∧ liveness formula owned by introspect.QueryList), composed
// INSIDE the actor so the channel only sees the result, never the raw rows. A
// registry read failure writes nothing (the same "does not synthesize"
// posture as an unrouted type — the caller's closure reaps it) rather than
// answering with a bogus empty directory.
//
// Readiness is deliberately absent: it is not a substrate axis — whether an actor can service a request
// is the OUTCOME of send→terminal, never a stored field here.
func (s *SystemActor) respondList(sys actorbase.Sys, msg actorbase.Msg) {
	rows, err := s.registry.ListActive(msg.Ctx())
	if err != nil {
		return
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
	_, _ = sys.Reply(msg, catalog)
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
func (s *SystemActor) respondDescribe(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := introspect.ParseDescribeRequest(msg.Payload)
	if err != nil {
		return
	}
	answer, ok := introspect.AnswerDescribe(systemDescribe(), req)
	if !ok {
		return
	}
	_, _ = sys.Reply(msg, answer)
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
