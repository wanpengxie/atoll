package sysactor

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func resolveErrorCode(err error) string {
	var targetErr *actorbase.TargetResolveError
	if errors.As(err, &targetErr) {
		return targetErr.Code
	}
	return "internal_error"
}

type PresenceStat interface {
	Snapshot(ctx context.Context, id actor.ActorID) (presence.Snapshot, error)
}

// Directory is the narrow actor-truth surface the system actor consults: the
// membership roster it composes the member directory from, and the one membership boolean
// its operate gate asks. It never receives an actor record — the directory row
// carries identity and liveness only.
type Directory interface {
	storespec.IdentityRoster
	storespec.IdentityPresence
}

// SystemActor holds one incarnation's process state: it answers channel-wide
// directory queries by composing the unified active-identity
// authority with volatile liveness (the injected seam). It is channel-agnostic
// at the base — the composition root injects channel-scoped services (authority,
// liveness/device seams), so this actor holds no channel id of its own.
//
// It is an actorbase Proc (lib/actorbase, actorbase-spec-v1 §3's out-generation
// matrix: sysactor is a ring0 special Proc — Caps hand-built raw by platform,
// not welded through the live membrane — but it enters through the SAME
// actorbase.New seam every other actor does). Def mints a fresh SystemActor per
// incarnation; run(sys) is the process body.
type SystemActor struct {
	authority Directory
	decl      func(context.Context, []string) (map[string]channelspec.DeclarationFacts, error)
	clock     func() time.Time
	presence  PresenceStat
	operate   OperateExecutor
	peer      Peer
	resolve   func(string) (actor.ActorID, error)
	logbook   interface {
		MaxSeq(context.Context) (int64, error)
		ReadAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, error)
	}
	logger *slog.Logger
}

// Deps bundles the channel services the system actor needs.
type Deps struct {
	Authority   Directory
	Declaration func(context.Context, []string) (map[string]channelspec.DeclarationFacts, error)
	Clock       func() time.Time
	Presence    PresenceStat
	Logger      *slog.Logger
	// Operate is the injected channel-operate executor (the in-gate control plane's
	// implementation half; the gate here does permission + routing). Nil → the four
	// control types are inert (no synthesis) — the injection point is unfilled.
	Operate OperateExecutor
	// Peer is the membrane's only external port: a space-level system request
	// from a non-c0 channel is framed and sent to c0 through it.
	Peer          Peer
	ResolveTarget func(string) (actor.ActorID, error)
	// Logbook is the channel-scoped read face. It intentionally exposes no
	// append capability to the system actor.
	Logbook interface {
		MaxSeq(context.Context) (int64, error)
		ReadAfterSeq(context.Context, int64, int) ([]storespec.StoredRow, error)
	}
}

// New constructs the channel system actor's process state (exported so a
// composition root's own tests can drive it directly against a fake Sys; Def
// wraps this in the actorbase registration entry for production spawn).
func New(deps Deps) *SystemActor {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &SystemActor{
		authority: deps.Authority,
		decl:      deps.Declaration,
		clock:     clock,
		presence:  deps.Presence,
		operate:   deps.Operate,
		peer:      deps.Peer,
		resolve:   deps.ResolveTarget,
		logbook:   deps.Logbook,
		logger:    logger,
	}
}

// Def is sysactor's actorbase registration entry (spec §1.6): New mints a
// fresh SystemActor + run per incarnation, closing over deps.
func Def(deps Deps) actorbase.Def {
	return actorbase.Def{
		Manifest: systemManifest(),
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
		if message.IsSpaceWord(msg.Type) {
			s.routeSpace(sys, msg)
			return
		}
		switch msg.Type {
		case message.TypeSystemMemberList:
			s.respondList(sys, msg)
			return
		case message.TypeSystemMemberGet:
			s.respondStatus(sys, msg)
			return
		case message.TypeSystemLogRecent:
			s.respondLogbookRecent(sys, msg)
			return
		case TypeMemberCreate, TypeMemberAdmit, TypeMemberDelete, TypeMemberRestart:
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

// respondList answers the member-list word with a composed channel-wide directory
// (membership ∧ liveness), composed
// INSIDE the actor so the channel only sees the result, never the raw rows. A
// registry read failure writes nothing (the same "does not synthesize"
// posture as an unrouted type — the caller's closure reaps it) rather than
// answering with a bogus empty directory.
//
// Readiness is deliberately absent: it is not a substrate axis — whether an actor can service a request
// is the OUTCOME of send→terminal, never a stored field here.
func (s *SystemActor) respondList(sys actorbase.Sys, msg actorbase.Msg) {
	identities, err := s.authority.ActiveIdentities()
	if err != nil {
		return
	}
	catalog := introspect.Catalog{Actors: make([]introspect.CatalogEntry, 0, len(identities))}
	// The roster already carries each member's source declaration, so the
	// declarations are fetched in ONE call. Asking per member would re-read what
	// this roster just handed over.
	declIDs := make([]string, 0, len(identities))
	if s.decl != nil {
		seen := make(map[string]bool, len(identities))
		for _, identity := range identities {
			if identity.SourceDeclID == "" || seen[identity.SourceDeclID] {
				continue
			}
			seen[identity.SourceDeclID] = true
			declIDs = append(declIDs, identity.SourceDeclID)
		}
	}
	declarations := map[string]channelspec.DeclarationFacts{}
	if len(declIDs) > 0 {
		var declErr error
		declarations, declErr = s.decl(msg.Ctx(), declIDs)
		if declErr != nil {
			s.logger.Warn("sysactor.declaration_catalog_failed", "error", declErr)
			declarations = map[string]channelspec.DeclarationFacts{}
		}
	}
	for _, identity := range identities {
		snapshot, err := s.snapshot(msg.Ctx(), identity.ID)
		if err != nil {
			s.logger.Warn("sysactor.presence_snapshot_failed", "actor", string(identity.ID), "error", err)
		}
		present, uptimeMs := s.liveness(snapshot)
		entry := introspect.CatalogEntry{
			ID: string(identity.ID), Kind: string(identity.Kind),
			Present:  present,
			UptimeMs: uptimeMs,
			Device:   deviceTestimony(snapshot),
		}
		if identity.SourceDeclID != "" {
			if declaration, ok := declarations[identity.SourceDeclID]; ok {
				entry.Name = declaration.Name
				entry.Description = declaration.Description
			}
		}
		catalog.Actors = append(catalog.Actors, entry)
	}
	// The kernel is a constant, not a member: it has no record to list. The
	// directory entry is SYNTHESIZED here from the identity constant, never read
	// from any row.
	catalog.Actors = append(catalog.Actors, s.kernelEntry(msg))
	slices.SortFunc(catalog.Actors, func(l, r introspect.CatalogEntry) int {
		switch {
		case l.ID < r.ID:
			return -1
		case l.ID > r.ID:
			return 1
		default:
			return 0
		}
	})
	_, _ = sys.Reply(msg, catalog)
}

func (s *SystemActor) kernelEntry(msg actorbase.Msg) introspect.CatalogEntry {
	snapshot, err := s.snapshot(msg.Ctx(), actor.SystemActorID)
	if err != nil {
		s.logger.Warn("sysactor.presence_snapshot_failed",
			"actor", string(actor.SystemActorID), "error", err)
	}
	present, uptimeMs := s.liveness(snapshot)
	return introspect.CatalogEntry{
		ID: string(actor.SystemActorID), Kind: string(actor.KindSystem),
		Present: present, UptimeMs: uptimeMs,
	}
}

// systemDescribe is the system actor's self-answer in the introspect contract
// shape: identity + the reserved directory query it serves. Declared through
// the SAME convention every actor honours, rather than hand-rolling the API
// list at the serve site.
func systemManifest() introspect.Manifest {
	words := map[string]introspect.WordSpec{}
	for _, entry := range message.SystemEntries() {
		if entry.Kind == message.KindRequest {
			words[entry.Name] = introspect.WordSpec{Description: "reserved system request"}
		}
	}
	return introspect.Manifest{Class: "membrane", Interfaces: []string{"actor"}, Words: words}
}

func (s *SystemActor) snapshot(ctx context.Context, id actor.ActorID) (presence.Snapshot, error) {
	if s.presence == nil {
		return presence.Snapshot{}, nil
	}
	return s.presence.Snapshot(ctx, id)
}

func (s *SystemActor) liveness(snapshot presence.Snapshot) (bool, int64) {
	if !snapshot.L1Present {
		return false, 0
	}
	uptime := int64(0)
	if !snapshot.L1StartedAt.IsZero() {
		uptime = s.clock().Sub(snapshot.L1StartedAt).Milliseconds()
	}
	return true, uptime
}

func deviceTestimony(snapshot presence.Snapshot) *introspect.DevicePresence {
	row, known := snapshot.L3[actorrt.ObsKind(introspect.ObsDevicePresence)]
	if !known {
		return nil
	}
	p, ok := introspect.ParseDevicePresence(row.Val)
	if !ok {
		return nil
	}
	return &p
}

func (s *SystemActor) respondStatus(sys actorbase.Sys, msg actorbase.Msg) {
	req, err := introspect.ParseStatusRequest(msg.Payload)
	if err != nil {
		s.logger.Warn("sysactor.member_get.bad_request", "error", err)
		return
	}
	target := actor.ActorID(req.Member)
	if s.resolve != nil {
		target, err = s.resolve(req.Member)
		if err != nil {
			_, _ = sys.Fail(msg, resolveErrorCode(err), err.Error())
			return
		}
	}
	snapshot, err := s.snapshot(msg.Ctx(), target)
	if err != nil {
		s.logger.Warn("sysactor.member_get.snapshot_failed", "actor", req.Member, "error", err)
		return
	}
	present, uptime := s.liveness(snapshot)
	answer := introspect.Status{ActorID: string(target), Member: snapshot.Member, Present: present, UptimeMs: uptime}
	if len(snapshot.L3) > 0 {
		answer.L3 = make(map[string]introspect.StatusTestimony, len(snapshot.L3))
	}
	for kind, row := range snapshot.L3 {
		out := introspect.StatusTestimony{ReceivedAt: row.ReceivedAt, StaleFromPriorLife: row.StaleFromPriorLife}
		if string(kind) == introspect.ObsDevicePresence {
			if value, ok := introspect.ParseDevicePresence(row.Val); ok {
				out.Device = &value
			} else {
				out.ValueBase64 = base64.StdEncoding.EncodeToString(row.Val)
			}
		} else {
			out.ValueBase64 = base64.StdEncoding.EncodeToString(row.Val)
		}
		answer.L3[string(kind)] = out
	}
	_, _ = sys.Reply(msg, answer)
}
