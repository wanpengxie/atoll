package home

import (
	"context"
	"errors"
	"sort"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrEndNotMember       = errors.New("end_not_member")
	ErrEndVersionStale    = errors.New("end_version_stale")
	ErrEndNotSponsor      = errors.New("end_not_sponsor")
	ErrEndSystemForbidden = errors.New("end_system_forbidden")
)

type EndPlan struct {
	AllIDs     []actor.ActorID
	DurableIDs []actor.ActorID
	RunIDs     []actor.ActorID
	Envelopes  []storespec.CascadeEnvelope
	Principals []string
}

// lifecycleEndHandle welds the authenticated author at its mint point. End
// callers can choose a target and reason, but cannot self-report authority.
type lifecycleEndHandle struct {
	home   *Home
	author storespec.AuthorStamp
}

func (x lifecycleEndHandle) End(ctx context.Context, target actor.ActorID, reason string) error {
	tail, err := x.prepare(ctx, target, reason)
	if err == nil && tail != nil {
		tail()
	}
	return err
}

func (x lifecycleEndHandle) prepare(ctx context.Context, target actor.ActorID, reason string) (func(), error) {
	return x.home.prepareEndIdentity(ctx, x.author, target, reason)
}

func (h *Home) systemEndHandle() lifecycleEndHandle {
	if h.systemEnd.home != nil {
		return h.systemEnd
	}
	return lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: actor.SystemActorID, BirthVersion: 1}}
}

// prepareEndIdentity commits and publishes the identity transition, returning
// only the resource-tail teardown. The port path places its end_ack on the
// ordered egress queue before running this tail; every other caller runs it
// immediately through EndIdentity above.
func (h *Home) prepareEndIdentity(ctx context.Context, author storespec.AuthorStamp, target actor.ActorID, reason string) (func(), error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if target == actor.SystemActorID {
		return nil, ErrEndSystemForbidden
	}
	if reason == "" {
		reason = "ended"
	}

	locked := map[actor.ActorID]bool{}
	releases := []func(){}
	lockOne := func(id actor.ActorID) {
		if id == "" || locked[id] {
			return
		}
		releases = append(releases, h.actorGates.lock(id))
		locked[id] = true
	}
	if author.ID != actor.SystemActorID && author.ID != target {
		lockOne(author.ID) // sponsor ids precede their generated descendants
	}
	lockOne(target)
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()

	if author.ID != actor.SystemActorID {
		switch verdict, err := h.controlIndex.CheckAuthor(ctx, author); {
		case err != nil:
			return nil, err
		case verdict == storespec.AuthorNotMember:
			return nil, ErrEndNotMember
		case verdict == storespec.AuthorVersionStale:
			return nil, ErrEndVersionStale
		}
	}
	targetRow, ok, err := h.controlIndex.LookupActive(ctx, target)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // idempotent end
	}
	if targetRow.Role == storespec.RoleOwner {
		return nil, storespec.ErrChannelOwnerProtected
	}
	if author.ID != actor.SystemActorID && author.ID != target && targetRow.Sponsor != author.ID {
		return nil, ErrEndNotSponsor
	}

	if err := h.lockClosure(ctx, target, locked, lockOne); err != nil {
		return nil, err
	}

	plan, err := h.buildEndPlan(ctx, target, reason, author.ID)
	if err != nil || len(plan.AllIDs) == 0 {
		return nil, err
	}
	if _, err := h.cs.Cascade.EndCascade(ctx, storespec.CascadeBundle{
		IDs: plan.DurableIDs, EndedAt: h.nowMs(), Envelopes: plan.Envelopes,
	}); err != nil {
		return nil, err
	}
	return h.finishEndTeardown(plan), nil
}

// lockClosure incrementally locks the full sponsor closure rooted at target,
// using the caller's lock bookkeeping. Once it returns, no concurrent Fork can
// inject another descendant: Fork locks its parent, and every discovered node
// is already locked here. Shared by the self-end path (prepareEndIdentity) and
// the member-word remove (removeMember) so the one Fork-race discipline is not
// re-implemented.
func (h *Home) lockClosure(ctx context.Context, target actor.ActorID, locked map[actor.ActorID]bool, lockOne func(actor.ActorID)) error {
	for {
		rows, err := h.controlIndex.ListActive(ctx)
		if err != nil {
			return err
		}
		inTree := map[actor.ActorID]bool{target: true}
		changed := true
		for changed {
			changed = false
			for _, row := range rows {
				if !inTree[row.ID] && inTree[row.Sponsor] {
					inTree[row.ID] = true
					changed = true
				}
			}
		}
		var next []actor.ActorID
		for id := range inTree {
			if !locked[id] {
				next = append(next, id)
			}
		}
		if len(next) == 0 {
			break
		}
		sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
		for _, id := range next {
			lockOne(id)
		}
	}
	return nil
}

// finishEndTeardown runs the post-commit session cleanup for an ended closure
// and returns the resource-tail despawn the caller runs after any egress
// ordering. Shared by the self-end path and the member-word remove: the durable
// truth was already committed (EndCascade or RemoveActor), and the run-world
// in-memory teardown (session state / grants that no durable cascade can reach)
// is identical for both.
func (h *Home) finishEndTeardown(plan EndPlan) func() {
	h.controlIndex.DeleteBatch(plan.AllIDs)
	h.stateHandles.EndBatch(plan.RunIDs)
	h.grantOverlay.EndBatch(plan.RunIDs)
	for _, id := range plan.AllIDs {
		_, _ = h.liveness.EndIdentity(id)
		h.removeSubjectSlot(id)
		h.presenceFold.Forget(id)
		h.reviveMu.Lock()
		delete(h.reviveLogAt, id)
		delete(h.reviveBackoff, id)
		h.reviveMu.Unlock()
		_, _ = h.takeAnyIndexedPort(id)
	}
	for _, principal := range plan.Principals {
		if principal != "" && h.onMembershipChange != nil {
			h.onMembershipChange(principal)
		}
	}
	return func() {
		for _, id := range plan.AllIDs {
			h.channel.Cells().DespawnID(id)
		}
	}
}

func (h *Home) buildEndPlan(ctx context.Context, root actor.ActorID, reason string, endedBy actor.ActorID) (EndPlan, error) {
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		return EndPlan{}, err
	}
	byID := make(map[actor.ActorID]storespec.ActorControlRow, len(rows))
	inTree := map[actor.ActorID]bool{root: true}
	for _, row := range rows {
		byID[row.ID] = row
	}
	changed := true
	for changed {
		changed = false
		for _, row := range rows {
			if !inTree[row.ID] && inTree[row.Sponsor] {
				inTree[row.ID] = true
				changed = true
			}
		}
	}
	plan := EndPlan{}
	for id := range inTree {
		row, ok := byID[id]
		if !ok {
			continue
		}
		plan.AllIDs = append(plan.AllIDs, id)
		plan.Envelopes = append(plan.Envelopes, storespec.CascadeEnvelope{Target: id, Reason: reason, EndedBy: endedBy})
		if row.Principal != "" {
			plan.Principals = append(plan.Principals, row.Principal)
		}
		world, ok, err := h.controlIndex.WorldOf(ctx, id)
		if err != nil {
			return EndPlan{}, err
		}
		if !ok {
			return EndPlan{}, ErrEndNotMember
		}
		if world == storespec.WorldDurable {
			plan.DurableIDs = append(plan.DurableIDs, id)
		} else {
			plan.RunIDs = append(plan.RunIDs, id)
		}
	}
	sort.Slice(plan.AllIDs, func(i, j int) bool { return plan.AllIDs[i] < plan.AllIDs[j] })
	sort.Slice(plan.DurableIDs, func(i, j int) bool { return plan.DurableIDs[i] < plan.DurableIDs[j] })
	sort.Slice(plan.RunIDs, func(i, j int) bool { return plan.RunIDs[i] < plan.RunIDs[j] })
	sort.Slice(plan.Envelopes, func(i, j int) bool { return plan.Envelopes[i].Target < plan.Envelopes[j].Target })
	return plan, nil
}

func (h *Home) buildDaemonDetachPlan(ctx context.Context, roots []actor.ActorID, daemonID string) (EndPlan, error) {
	merged := EndPlan{}
	seenAll := map[actor.ActorID]bool{}
	seenDurable := map[actor.ActorID]bool{}
	seenRun := map[actor.ActorID]bool{}
	seenPrincipal := map[string]bool{}
	for _, root := range roots {
		if seenAll[root] {
			continue
		}
		plan, err := h.buildEndPlan(ctx, root, "daemon_detach:"+daemonID, actor.SystemActorID)
		if err != nil {
			return EndPlan{}, err
		}
		for i, id := range plan.AllIDs {
			if seenAll[id] {
				continue
			}
			seenAll[id] = true
			merged.AllIDs = append(merged.AllIDs, id)
			merged.Envelopes = append(merged.Envelopes, plan.Envelopes[i])
		}
		for _, id := range plan.DurableIDs {
			if !seenDurable[id] {
				seenDurable[id] = true
				merged.DurableIDs = append(merged.DurableIDs, id)
			}
		}
		for _, id := range plan.RunIDs {
			if !seenRun[id] {
				seenRun[id] = true
				merged.RunIDs = append(merged.RunIDs, id)
			}
		}
		for _, principal := range plan.Principals {
			if principal != "" && !seenPrincipal[principal] {
				seenPrincipal[principal] = true
				merged.Principals = append(merged.Principals, principal)
			}
		}
	}
	sort.Slice(merged.AllIDs, func(i, j int) bool { return merged.AllIDs[i] < merged.AllIDs[j] })
	sort.Slice(merged.DurableIDs, func(i, j int) bool { return merged.DurableIDs[i] < merged.DurableIDs[j] })
	sort.Slice(merged.RunIDs, func(i, j int) bool { return merged.RunIDs[i] < merged.RunIDs[j] })
	sort.Strings(merged.Principals)
	sort.Slice(merged.Envelopes, func(i, j int) bool { return merged.Envelopes[i].Target < merged.Envelopes[j].Target })
	return merged, nil
}
