package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// CompositionResolver is the world-half injection seam. Home owns and reads
// channel-local composition; the app resolves its DeclID against the world
// declaration catalog and returns the buildable definition.
type CompositionResolver interface {
	ResolveComposition(context.Context, channelpkg.ID, storespec.CompositionRecord) (platform.ActorDecl, bool, error)
	BuildClass(channelpkg.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool)
}

type compositionView struct {
	h        *Home
	resolver CompositionResolver
}

type boundPlanProvider struct {
	channelID channelpkg.ID
	provider  PlanProvider
}

func (p boundPlanProvider) Plan(ctx context.Context, daemonID string) ([]platform.PlanActor, error) {
	if p.provider == nil {
		return nil, errors.New("platform: no plan provider")
	}
	return p.provider.Plan(ctx, p.channelID, daemonID)
}

func (v *compositionView) Members(ctx context.Context) ([]actorrt.DesiredMember, error) {
	rows, err := v.h.cs.Composition.ListComposition(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]actorrt.DesiredMember, 0, len(rows))
	for _, row := range rows {
		if row.Placement != storespec.PlacementServer {
			continue
		}
		decl, ok, err := v.resolver.ResolveComposition(ctx, v.h.channelID, row)
		if err != nil {
			return nil, err // whole-tick abort: never cull from a partial scan
		}
		if !ok {
			continue
		}
		out = append(out, actorrt.DesiredMember{ID: row.InstanceID, Kind: decl.Kind, Lifecycle: actorrt.LifecycleAlwaysOn, Epoch: row.Epoch})
	}
	return out, nil
}

func (v *compositionView) Lookup(id actor.ActorID) (platform.ActorFactory, bool) {
	row, ok, err := v.h.cs.Composition.LookupComposition(context.Background(), id)
	if err != nil || !ok || row.Placement != storespec.PlacementServer {
		return platform.ActorFactory{}, false
	}
	decl, ok, err := v.resolver.ResolveComposition(context.Background(), v.h.channelID, row)
	if err != nil || !ok || decl.ID != id {
		return platform.ActorFactory{}, false
	}
	return decl.Factory, true
}

func (v *compositionView) LookupByClass(id actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	return v.resolver.BuildClass(v.h.channelID, id, class, config)
}

// IntroduceComposition is the Home-owned composition mutation. The store
// atomically ensures active membership and the intent row; the reconcile poke
// happens only after that transaction commits.
func (h *Home) IntroduceComposition(ctx context.Context, in storespec.CompositionIntroduce) (storespec.CompositionRecord, bool, bool, error) {
	if h.closed.Load() {
		return storespec.CompositionRecord{}, false, false, ErrClosed
	}
	r, created, changed, err := h.cs.Composition.IntroduceComposition(ctx, in)
	if err == nil {
		h.pokeReconcile()
	}
	return r, created, changed, err
}

func (h *Home) Composition(ctx context.Context) ([]storespec.CompositionRecord, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	return h.cs.Composition.ListComposition(ctx)
}

func (h *Home) CompositionByPrincipal(ctx context.Context, principal string) (storespec.CompositionRecord, bool, error) {
	if h.closed.Load() {
		return storespec.CompositionRecord{}, false, ErrClosed
	}
	return h.cs.Composition.LookupCompositionPrincipal(ctx, principal)
}

func (h *Home) HasComposition(ctx context.Context, id actor.ActorID) (bool, error) {
	if h.closed.Load() {
		return false, ErrClosed
	}
	_, ok, err := h.cs.Composition.LookupComposition(ctx, id)
	return ok, err
}

func (h *Home) DefaultAgent(ctx context.Context) (actor.ActorID, bool, error) {
	if h.closed.Load() {
		return "", false, ErrClosed
	}
	return h.cs.Composition.DefaultComposition(ctx)
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
	return h.cs.Composition.SetDefaultComposition(ctx, id)
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
	unlock := h.actorGates.lock(id)
	defer unlock()
	if h.closed.Load() {
		return ErrClosed
	}
	h.channel.Cells().DespawnID(id)
	removed, err := h.cs.Composition.RemoveComposition(ctx, id, h.nowMs())
	if err != nil {
		return fmt.Errorf("platform: remove composition %s: %w", id, err)
	}
	if !removed {
		return storespec.ErrCompositionNotFound
	}
	h.channel.Cells().DespawnID(id)
	h.RemoveSubjectSlot(id)
	h.presenceFold.Forget(id)
	h.pokeReconcile()
	return nil
}

// RestartInstanceDirect advances durable desired generation before cutting the
// old embodiment. The reconcile rings rebuild only after observing the epoch.
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
	epoch, err := h.cs.Composition.RestartComposition(ctx, id)
	if err != nil {
		return 0, err
	}
	h.channel.Cells().DespawnID(id)
	h.pokeReconcile()
	return epoch, nil
}

func (h *Home) ApplyRestartTarget(ctx context.Context, jobID int64, id actor.ActorID) (int64, bool, error) {
	if h.closed.Load() {
		return 0, false, ErrClosed
	}
	unlock := h.actorGates.lock(id)
	defer unlock()
	if h.closed.Load() {
		return 0, false, ErrClosed
	}
	epoch, applied, err := h.cs.Composition.ApplyRestartComposition(ctx, jobID, id, h.nowMs())
	if err != nil {
		if errors.Is(err, storespec.ErrCompositionNotFound) {
			return 0, false, err
		}
		return 0, false, err
	}
	if applied {
		h.channel.Cells().DespawnID(id)
		h.pokeReconcile()
	}
	return epoch, applied, nil
}

func (h *Home) RevokeDaemonTarget(ctx context.Context, daemonID string) error {
	if h.closed.Load() {
		return ErrClosed
	}
	rows, err := h.cs.Composition.ListComposition(ctx)
	if err != nil {
		return err
	}
	var unlocks []func()
	for _, row := range rows {
		if row.DesiredHost == daemonID {
			unlocks = append(unlocks, h.actorGates.lock(row.InstanceID))
		}
	}
	defer func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}()
	ids, err := h.cs.Composition.RevokeDaemonTarget(ctx, daemonID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		h.channel.Cells().DespawnID(id)
	}
	h.links.KickDaemon(daemonID)
	h.pokeReconcile()
	return nil
}

func (h *Home) MarkCompositionMigrated(ctx context.Context, at int64) error {
	if h.closed.Load() {
		return ErrClosed
	}
	return h.cs.Composition.MarkCompositionMigrated(ctx, at)
}

// ApplyComputeDeclaration is the Home coordinator for S2. Every actor in the
// store's affected-set is gated before the transaction; the store invokes the
// body callback after producing the complete decision set and before any DB
// action. Index locks are leaf locks: each pointer is taken under indexMu and
// only then is Runtime touched.
type homeDeclarationCoordinator struct{ h *Home }

func (c homeDeclarationCoordinator) ApplyComputeDeclaration(ctx context.Context, owner link.PortOwner, daemonID string, declared []storespec.ComputeDeclaration) ([]storespec.ComputeDeclaration, error) {
	h := c.h
	if h.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := h.cs.Composition.ListComposition(ctx)
	if err != nil {
		return nil, err
	}
	active, err := h.cs.Registry.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	ids := make(map[actor.ActorID]struct{}, len(rows)+len(active)+len(declared))
	for _, row := range rows {
		ids[row.InstanceID] = struct{}{}
	}
	for _, rec := range active {
		if rec.Host == daemonID {
			ids[rec.ID] = struct{}{}
		}
	}
	for _, d := range declared {
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

	in := storespec.ComputeDeclarationInput{DaemonID: daemonID, Declared: declared, IndexedIDs: indexed, At: h.nowMs()}
	result, err := h.cs.Composition.ApplyComputeDeclaration(ctx, in, func(decisions []storespec.DeclarationDecision) error {
		for _, d := range decisions {
			var inc actorrt.Incarnation
			var ok bool
			switch d.PortAction {
			case storespec.DeclarationPortNone:
				continue
			case storespec.DeclarationPortTakeLink:
				inc, ok = homePortIndex{h: h}.Take(owner, d.ActorID)
			case storespec.DeclarationPortTakeAny:
				inc, ok = h.takeAnyIndexedPort(d.ActorID)
			case storespec.DeclarationPortTakeCurrent:
				inc, ok = h.channel.Cells().CurrentIncarnation(d.ActorID)
			default:
				return fmt.Errorf("platform: unknown declaration port action %d", d.PortAction)
			}
			if ok {
				h.channel.Cells().Despawn(inc)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	allowed := make([]storespec.ComputeDeclaration, 0, len(declared))
	for _, d := range result.Decisions {
		if d.Rejected {
			h.logger.Warn("home.compute_declaration_rejected", "daemon", daemonID, "actor", string(d.ActorID))
		}
		if d.Allow {
			allowed = append(allowed, storespec.ComputeDeclaration{ActorID: d.ActorID, Kind: d.Kind, Binding: d.Binding, Epoch: d.Epoch})
		}
	}
	return allowed, nil
}
