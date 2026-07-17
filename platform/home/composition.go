package home

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// CompositionResolver is the world-half injection seam. Home owns and reads
// channel-local composition; the app resolves its DeclID against the world
// declaration catalog and returns the buildable definition.
type CompositionResolver interface {
	BuildClass(channelpkg.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool)
}

type sourceConfigResolver interface {
	ResolveSourceConfig(context.Context, string, json.RawMessage) (json.RawMessage, error)
}

type compositionView struct {
	h        *Home
	resolver CompositionResolver
}

type boundPlanProvider struct {
	channelID channelpkg.ID
	provider  PlanProvider
}

func (v *compositionView) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	row, ok, err := v.h.controlIndex.LookupActive(context.Background(), id)
	if err != nil || !ok {
		return platform.ActorFactory{}, false
	}
	config, err := v.resolveConfig(context.Background(), row.SourceDeclID, row.Config)
	if err != nil {
		return platform.ActorFactory{}, false
	}
	return v.resolver.BuildClass(v.h.channelID, id, row.Class, config)
}

func (p boundPlanProvider) Plan(ctx context.Context, daemonID string) ([]platform.PlanActor, error) {
	if p.provider == nil {
		return nil, errors.New("platform: no plan provider")
	}
	return p.provider.Plan(ctx, p.channelID, daemonID)
}

func (v *compositionView) LookupByClass(id actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	return v.resolver.BuildClass(v.h.channelID, id, class, config)
}

func (v *compositionView) resolveConfig(ctx context.Context, sourceID string, config json.RawMessage) (json.RawMessage, error) {
	if r, ok := v.resolver.(sourceConfigResolver); ok {
		return r.ResolveSourceConfig(ctx, sourceID, config)
	}
	return append(json.RawMessage(nil), config...), nil
}

func (h *Home) DefaultAgent(ctx context.Context) (actor.ActorID, bool, error) {
	if h.closed.Load() {
		return "", false, ErrClosed
	}
	return h.cs.Routing.DefaultAgent(ctx)
}

func (h *Home) SetDefaultAgent(ctx context.Context, id actor.ActorID) error {
	if h.closed.Load() {
		return ErrClosed
	}
	unlock := h.actorGates.lock(id)
	defer unlock()
	if h.closed.Load() {
		return ErrClosed
	}
	return h.cs.Routing.SetDefaultAgent(ctx, id)
}

// RemoveInstance is composition-level termination: desired deletion and
// registry deregistration/cascades share one transaction, bracketed by quiet
// body removal to close both in-flight build windows.
func (h *Home) RemoveInstance(ctx context.Context, id actor.ActorID) error {
	if h.closed.Load() {
		return ErrClosed
	}
	if id == actor.SystemActorID {
		return ErrRemoveAnchor
	}
	if err := h.systemEndHandle().End(ctx, id, "removed"); err != nil {
		return err
	}
	h.pokeReconcile()
	return nil
}

// RestartInstanceDirect retires a present carrier without changing declaration
// truth. A dormant identity is deliberately a no-op: the next real request will
// activate it from the current declaration. A running daemon carrier is cut by
// the same Despawn primitive as End; the fresh EnsureTicket in the next plan is
// what distinguishes the replacement attempt.
func (h *Home) RestartInstanceDirect(ctx context.Context, id actor.ActorID) (int64, error) {
	if h.closed.Load() {
		return 0, ErrClosed
	}
	if id == actor.SystemActorID {
		return 0, ErrRestartAnchor
	}
	unlock := h.actorGates.lock(id)
	defer unlock()
	if h.closed.Load() {
		return 0, ErrClosed
	}
	row, active, err := h.controlIndex.LookupActive(ctx, id)
	if err != nil {
		return 0, err
	}
	if !active {
		return 0, storespec.ErrActorNotFound
	}
	_, verdict := h.liveness.Retire(id, true)
	if verdict != transitionApplied {
		return 0, errors.New("platform: invalid restart transition")
	}
	h.channel.Cells().DespawnIDReason(id, "restart")
	h.pokeReconcile()
	return row.CurrentDeclVersion, nil
}

func (h *Home) ApplyRestartTarget(ctx context.Context, jobID int64, id actor.ActorID) (int64, bool, error) {
	if h.closed.Load() {
		return 0, false, ErrClosed
	}
	if id == actor.SystemActorID {
		return 0, false, ErrRestartAnchor
	}
	unlock := h.actorGates.lock(id)
	defer unlock()
	if h.closed.Load() {
		return 0, false, ErrClosed
	}
	row, active, err := h.controlIndex.LookupActive(ctx, id)
	if err != nil {
		return 0, false, err
	}
	if !active {
		return 0, false, storespec.ErrActorNotFound
	}
	intent := h.liveness.AttachmentIntent(id)
	currentTicket := ""
	if intent.Present && intent.Version == row.CurrentDeclVersion {
		currentTicket = string(intent.Ticket)
	}
	expected, alreadyApplied, err := h.cs.RestartJournal.ClaimRestartAttempt(ctx, jobID, id, currentTicket, h.nowMs())
	if err != nil {
		return 0, false, err
	}
	if alreadyApplied {
		return row.CurrentDeclVersion, false, nil
	}
	if _, retired := h.liveness.RetireIfTicketMatches(id, EnsureTicket(expected), true); retired {
		h.channel.Cells().DespawnIDReason(id, "restart")
		h.pokeReconcile()
	}
	marked, err := h.cs.RestartJournal.MarkRestartApplied(ctx, jobID, id, h.nowMs())
	if err != nil {
		return row.CurrentDeclVersion, false, err
	}
	return row.CurrentDeclVersion, marked, nil
}

func (h *Home) RevokeDaemonTarget(ctx context.Context, daemonID string) error {
	if h.closed.Load() {
		return ErrClosed
	}
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.Placement.Kind != storespec.PlacementDaemon || row.Placement.Host != daemonID {
			continue
		}
		if err := h.systemEndHandle().End(ctx, row.ID, "daemon_revoked"); err != nil {
			return err
		}
	}
	h.pokeReconcile()
	return nil
}

// ValidateAttachment is a pure Home-side decision over declared authority and
// liveness intent. It never inserts, removes, or re-homes an identity.
type homeDeclarationCoordinator struct{ h *Home }

func (c homeDeclarationCoordinator) PrepareAttachmentFence(_ context.Context, id actor.ActorID, ticket string, birthVersion int64) (link.AttachmentFence, error) {
	fence, verdict := c.h.liveness.prepareAttachmentFence(id, EnsureTicket(ticket), birthVersion)
	if verdict != transitionApplied {
		return nil, errors.New("platform: stale attachment intent")
	}
	return fence, nil
}

func (c homeDeclarationCoordinator) ValidateAttachment(ctx context.Context, owner link.PortOwner, daemonID string, declared []storespec.ComputeDeclaration) ([]storespec.ComputeDeclaration, error) {
	h := c.h
	if h.closed.Load() {
		return nil, ErrClosed
	}
	declaredIDs := make(map[actor.ActorID]struct{}, len(declared))
	ids := make(map[actor.ActorID]struct{}, len(declared))
	for _, d := range declared {
		declaredIDs[d.ActorID] = struct{}{}
		ids[d.ActorID] = struct{}{}
	}
	indexed := h.indexedPortIDs(owner)
	for _, id := range indexed {
		ids[id] = struct{}{}
	}
	ordered := make([]actor.ActorID, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	unlocks := make([]func(), 0, len(ordered))
	for _, id := range ordered {
		unlocks = append(unlocks, h.actorGates.lock(id))
	}
	defer func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}()
	if h.closed.Load() {
		return nil, ErrClosed
	}

	for _, id := range indexed {
		if _, present := declaredIDs[id]; !present {
			if inc, ok := (homePortIndex{h: h}).Take(owner, id); ok {
				_ = h.liveness.ObserveDown(id, true, false)
				h.channel.Cells().Despawn(inc)
			}
		}
	}
	allowed := make([]storespec.ComputeDeclaration, 0, len(declared))
	for _, d := range declared {
		row, ok, err := h.controlIndex.LookupActive(ctx, d.ActorID)
		if err != nil {
			return nil, err
		}
		intent := h.liveness.AttachmentIntent(d.ActorID)
		if !ok || !intent.Present || row.Kind != d.Kind || row.CurrentDeclVersion != d.Version ||
			row.Placement.Kind != storespec.PlacementDaemon || row.Placement.Host != daemonID ||
			d.Binding != actor.BindingRuntimeInboundViaRelay || string(intent.Ticket) != d.EnsureTicket || intent.Version != d.Version {
			h.logger.Warn("home.compute_declaration_rejected", "daemon", daemonID, "actor", string(d.ActorID))
			continue
		}
		allowed = append(allowed, d)
	}
	return allowed, nil
}
