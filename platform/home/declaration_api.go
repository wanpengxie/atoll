package home

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type DeclareRequest struct {
	SourceDeclID string
	Kind         actor.Kind
	Class        string
	Config       *json.RawMessage
	Placement    storespec.Placement
	TIdle        int64 // milliseconds; zero means no idle retirement
	MakeDefault  bool
	CreatedAt    int64
}

type DeclareResult struct {
	Row           storespec.ActorControlRow
	Created       bool
	ConfigUpdated bool
}

func (h *Home) declare(ctx context.Context, in DeclareRequest) (DeclareResult, error) {
	if h.closed.Load() {
		return DeclareResult{}, ErrClosed
	}
	if in.SourceDeclID == "" || in.Class == "" || in.CreatedAt <= 0 || in.TIdle < 0 || in.Placement.Validate() != nil {
		return DeclareResult{}, errors.New("platform: invalid declaration request")
	}
	var config json.RawMessage
	if in.Config != nil {
		config = append([]byte(nil), (*in.Config)...)
	}
	binding := actor.Binding("")
	if in.Placement.Kind == storespec.PlacementDaemon {
		binding = actor.BindingRuntimeInboundViaRelay
	}
	admitted, err := h.cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: in.Kind, Binding: binding, Class: in.Class, Config: config,
		Placement: in.Placement, TIdle: durationMillis(in.TIdle), SourceDeclID: in.SourceDeclID,
		CreatedAt: in.CreatedAt,
	})
	if err != nil {
		return DeclareResult{}, err
	}
	row, ok, err := h.cs.Declared.LookupDeclaredActive(ctx, admitted.ID)
	if err != nil || !ok {
		return DeclareResult{}, err
	}
	// Assembly order (装配序): liveness row BEFORE authority publish — same
	// rule as fork admission. Publishing authority first opens a window where
	// a delivery finds no L row, returns invalid, and the pump advances past
	// the request with no wake debt recorded.
	if admitted.Created && h.liveness.AdmitIdentity(admitted.ID) != transitionApplied {
		return DeclareResult{}, errors.New("platform: invalid declared liveness row")
	}
	if !h.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldDurable}}) {
		return DeclareResult{}, errors.New("platform: invalid declared control row")
	}
	updated := false
	if !admitted.Created && in.Config != nil {
		synced, syncErr := h.opEntry.applyResolvedDeclaration(ctx, row.ID, row.SourceDeclID, row.Class, config)
		if syncErr != nil {
			return DeclareResult{}, syncErr
		}
		updated = synced.Status == storespec.DeclarationApplied
		if updated {
			var active bool
			row, active, err = h.controlIndex.LookupActive(ctx, row.ID)
			if err != nil {
				return DeclareResult{}, err
			}
			if !active {
				return DeclareResult{}, errors.New("platform: applied declaration missing from control index")
			}
		}
	}
	if in.MakeDefault {
		if err := h.cs.Routing.SetDefaultAgent(ctx, row.ID); err != nil {
			return DeclareResult{}, err
		}
	}
	h.pokeReconcile()
	return DeclareResult{Row: row, Created: admitted.Created, ConfigUpdated: updated}, nil
}

func durationMillis(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }
