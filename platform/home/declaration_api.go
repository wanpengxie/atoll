package home

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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
	RenderSeq    int64
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
		CreatedAt: in.CreatedAt, RenderSeq: in.RenderSeq,
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
	currentDigest, digestErr := declarationContentDigest(row, row.Config)
	if digestErr != nil {
		return DeclareResult{}, digestErr
	}
	candidateDigest, digestErr := declarationContentDigest(row, config)
	if digestErr != nil {
		return DeclareResult{}, digestErr
	}
	if !admitted.Created && in.Config != nil && currentDigest != candidateDigest {
		edited, editErr := h.editDeclaration(ctx, storespec.DeclEditBundle{
			ActorID: row.ID, Class: row.Class, Config: config, Placement: row.Placement,
			TIdle: row.TIdle, CreatedAt: in.CreatedAt,
			RenderSeq: in.RenderSeq,
		})
		if editErr != nil {
			return DeclareResult{}, editErr
		}
		row, err = h.applyDeclaration(ctx, row.ID, edited.CurrentDeclVersion)
		if err != nil {
			return DeclareResult{}, err
		}
		updated = true
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

func declarationContentDigest(row storespec.ActorControlRow, config json.RawMessage) (string, error) {
	return (channel.RenderedSnapshot{
		Class:  row.Class,
		Config: config,
		Placement: channel.Placement{
			Kind:        channel.PlacementKind(row.Placement.Kind),
			DesiredHost: row.Placement.Host,
		},
		TIdleMS:   row.TIdle.Milliseconds(),
		RenderSeq: max(row.RenderSeq, 1),
	}).ContentDigest()
}

func (h *Home) activeActors(ctx context.Context) ([]storespec.ActorControlRow, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	return h.controlIndex.ListActive(ctx)
}

func (h *Home) activeActor(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	if h.closed.Load() {
		return storespec.ActorControlRow{}, false, ErrClosed
	}
	return h.controlIndex.LookupActive(ctx, id)
}

func (h *Home) declaredBySource(ctx context.Context, source string) ([]storespec.ActorControlRow, error) {
	rows, err := h.controlIndex.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorControlRow, 0)
	for _, row := range rows {
		world, ok, werr := h.controlIndex.WorldOf(ctx, row.ID)
		if werr != nil {
			return nil, werr
		}
		if ok && world == storespec.WorldDurable && row.SourceDeclID == source {
			out = append(out, row)
		}
	}
	return out, nil
}
