package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type declarationComposition struct {
	placement storespec.Placement
	desired   string
	epoch     int64
}

type declarationRegistry struct {
	found   bool
	active  bool
	kind    actor.Kind
	binding actor.Binding
	host    string
}

func canonicalCompositionBinding(p storespec.Placement) actor.Binding {
	if p == storespec.PlacementDaemon {
		return actor.BindingRuntimeInboundViaRelay
	}
	return actor.BindingEmbedded
}

// ApplyComputeDeclaration implements the fifteen-row declaration table in one
// channel transaction. Decision generation is deliberately separate from the
// writes below: metadata disagreement can only suppress allow and request body
// removal; it can never erase a death/rehome database action.
func (s *compositionStore) ApplyComputeDeclaration(
	ctx context.Context,
	in storespec.ComputeDeclarationInput,
	beforeWrite func([]storespec.DeclarationDecision) error,
) (storespec.ComputeDeclarationResult, error) {
	if in.DaemonID == "" || in.At == 0 {
		return storespec.ComputeDeclarationResult{}, errors.New("store: compute declaration daemon and timestamp required")
	}
	declared := make(map[actor.ActorID]storespec.ComputeDeclaration, len(in.Declared))
	for _, d := range in.Declared {
		if d.ActorID == "" {
			return storespec.ComputeDeclarationResult{}, errors.New("store: compute declaration actor id required")
		}
		if _, exists := declared[d.ActorID]; exists {
			return storespec.ComputeDeclarationResult{}, fmt.Errorf("store: duplicate compute declaration %q", d.ActorID)
		}
		declared[d.ActorID] = d
	}
	indexed := make(map[actor.ActorID]bool, len(in.IndexedIDs))
	for _, id := range in.IndexedIDs {
		indexed[id] = true
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.ComputeDeclarationResult{}, fmt.Errorf("store: compute declaration begin: %w", err)
	}
	defer tx.Rollback()

	affected := make(map[actor.ActorID]struct{}, len(declared)+len(indexed))
	for id := range declared {
		affected[id] = struct{}{}
	}
	for id := range indexed {
		affected[id] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT instance_id FROM channel_composition`)
	if err != nil {
		return storespec.ComputeDeclarationResult{}, err
	}
	for rows.Next() {
		var id actor.ActorID
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return storespec.ComputeDeclarationResult{}, err
		}
		affected[id] = struct{}{}
	}
	if err := rows.Close(); err != nil {
		return storespec.ComputeDeclarationResult{}, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT actor_id FROM actor_registry WHERE deregistered_at IS NULL AND host=?`, in.DaemonID)
	if err != nil {
		return storespec.ComputeDeclarationResult{}, err
	}
	for rows.Next() {
		var id actor.ActorID
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return storespec.ComputeDeclarationResult{}, err
		}
		affected[id] = struct{}{}
	}
	if err := rows.Close(); err != nil {
		return storespec.ComputeDeclarationResult{}, err
	}

	ids := make([]actor.ActorID, 0, len(affected))
	for id := range affected {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	decisions := make([]storespec.DeclarationDecision, 0, len(ids))
	for _, id := range ids {
		comp, compOK, err := loadDeclarationComposition(ctx, tx, id)
		if err != nil {
			return storespec.ComputeDeclarationResult{}, err
		}
		reg, err := loadDeclarationRegistry(ctx, tx, id)
		if err != nil {
			return storespec.ComputeDeclarationResult{}, err
		}
		decl, hasDecl := declared[id]
		decision := decideComputeDeclaration(in.DaemonID, id, comp, compOK, reg, decl, hasDecl, indexed[id])
		decisions = append(decisions, decision)
	}

	if beforeWrite != nil {
		if err := beforeWrite(decisions); err != nil {
			return storespec.ComputeDeclarationResult{}, fmt.Errorf("store: compute declaration port actions: %w", err)
		}
	}

	appended := 0
	for _, d := range decisions {
		comp, compOK, err := loadDeclarationComposition(ctx, tx, d.ActorID)
		if err != nil {
			return storespec.ComputeDeclarationResult{}, err
		}
		reg, err := loadDeclarationRegistry(ctx, tx, d.ActorID)
		if err != nil {
			return storespec.ComputeDeclarationResult{}, err
		}
		decl, hasDecl := declared[d.ActorID]
		switch {
		case !compOK && reg.active && reg.host == in.DaemonID:
			remove := storespec.MemberActorRemove{ID: d.ActorID, ExpectedHost: in.DaemonID, At: in.At}
			changed, err := s.reg.applyMemberRemoveTx(ctx, tx, remove)
			if err != nil {
				return storespec.ComputeDeclarationResult{}, err
			}
			if changed {
				if _, err := appendTx(ctx, tx, actorDeregisteredEnvelope(s.reg.channelID, remove), false); err != nil {
					return storespec.ComputeDeclarationResult{}, err
				}
				appended++
			}
		case compOK && comp.placement == storespec.PlacementDaemon && comp.desired == in.DaemonID && reg.active && reg.host != in.DaemonID && hasDecl:
			canonical := canonicalCompositionBinding(comp.placement)
			if _, err := tx.ExecContext(ctx, `UPDATE actor_registry SET host=?, actor_binding=? WHERE actor_id=? AND deregistered_at IS NULL AND COALESCE(host,'')<>?`, in.DaemonID, nullableBinding(canonical), string(d.ActorID), in.DaemonID); err != nil {
				return storespec.ComputeDeclarationResult{}, err
			}
		case compOK && !(comp.placement == storespec.PlacementDaemon && comp.desired == in.DaemonID) && reg.active && reg.host == in.DaemonID:
			if _, err := tx.ExecContext(ctx, `UPDATE actor_registry SET host='' WHERE actor_id=? AND deregistered_at IS NULL AND host=?`, string(d.ActorID), in.DaemonID); err != nil {
				return storespec.ComputeDeclarationResult{}, err
			}
		}
		_ = decl
	}
	if err := tx.Commit(); err != nil {
		return storespec.ComputeDeclarationResult{}, fmt.Errorf("store: compute declaration commit: %w", err)
	}
	if appended > 0 && s.reg.onCommit != nil {
		s.reg.onCommit()
	}
	return storespec.ComputeDeclarationResult{Decisions: decisions}, nil
}

func loadDeclarationComposition(ctx context.Context, tx *sql.Tx, id actor.ActorID) (declarationComposition, bool, error) {
	var c declarationComposition
	var placement string
	err := tx.QueryRowContext(ctx, `SELECT placement, desired_host, restart_epoch FROM channel_composition WHERE instance_id=?`, string(id)).Scan(&placement, &c.desired, &c.epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return declarationComposition{}, false, nil
	}
	if err != nil {
		return declarationComposition{}, false, err
	}
	c.placement = storespec.Placement(placement)
	return c, true, nil
}

func loadDeclarationRegistry(ctx context.Context, tx *sql.Tx, id actor.ActorID) (declarationRegistry, error) {
	var r declarationRegistry
	var kind, binding string
	var dereg sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT actor_kind, COALESCE(actor_binding,''), COALESCE(host,''), deregistered_at FROM actor_registry WHERE actor_id=?`, string(id)).Scan(&kind, &binding, &r.host, &dereg)
	if errors.Is(err, sql.ErrNoRows) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	r.found = true
	r.active = !dereg.Valid
	r.kind = actor.Kind(kind)
	r.binding = actor.Binding(binding)
	return r, nil
}

func decideComputeDeclaration(daemon string, id actor.ActorID, comp declarationComposition, compOK bool, reg declarationRegistry, decl storespec.ComputeDeclaration, hasDecl, indexed bool) storespec.DeclarationDecision {
	d := storespec.DeclarationDecision{ActorID: id}
	if hasDecl {
		d.Kind, d.Binding, d.Epoch = decl.Kind, decl.Binding, decl.Epoch
	}
	regSelf := reg.active && reg.host == daemon
	regOther := reg.active && reg.host != daemon
	if !compOK {
		switch {
		case regSelf: // row 1: death dominates every metadata verdict.
			d.PortAction = storespec.DeclarationPortTakeAny
			d.Rejected = hasDecl
		case regOther && hasDecl: // row 2
			d.Rejected = true
		case regOther && !hasDecl && indexed: // row 3
			d.PortAction = storespec.DeclarationPortTakeLink
		case !reg.active && hasDecl: // row 4
			d.Rejected = true
		case !reg.active && !hasDecl && indexed: // row 5
			d.PortAction = storespec.DeclarationPortTakeLink
		}
		return d
	}

	self := comp.placement == storespec.PlacementDaemon && comp.desired == daemon
	if self {
		switch {
		case regSelf && hasDecl: // row 6
			canonical := canonicalCompositionBinding(comp.placement)
			d.Allow = decl.Kind == reg.kind && decl.Binding == canonical && reg.binding == canonical && decl.Epoch == comp.epoch
			d.Rejected = !d.Allow
			if d.Rejected {
				d.PortAction = storespec.DeclarationPortTakeAny
			}
		case regSelf && !hasDecl: // row 7
			d.PortAction = storespec.DeclarationPortTakeLink
		case regOther && hasDecl: // row 8
			canonical := canonicalCompositionBinding(comp.placement)
			d.Allow = decl.Kind == reg.kind && decl.Binding == canonical && decl.Epoch == comp.epoch
			d.Rejected = !d.Allow
			if reg.host == "" {
				d.PortAction = storespec.DeclarationPortTakeCurrent
			} else {
				d.PortAction = storespec.DeclarationPortTakeAny
			}
		case regOther && !hasDecl: // row 9
		case !reg.active && hasDecl: // row 10
			d.Rejected = true
		case !reg.active && !hasDecl: // row 11
		}
		return d
	}

	// other/server rows 12-15. A pool row (daemon placement with empty desired)
	// is intentionally in this branch and cannot be claimed by declaration.
	switch {
	case regSelf: // row 12
		d.PortAction = storespec.DeclarationPortTakeAny
		d.Rejected = hasDecl
	case regOther && hasDecl: // row 13
		d.Rejected = true
	case regOther && !hasDecl: // row 14
	case !reg.active && hasDecl: // row 15 (declared half)
		d.Rejected = true
	case !reg.active && !hasDecl: // row 15 (missing half)
	}
	return d
}
