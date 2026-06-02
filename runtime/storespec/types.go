package storespec

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TypeRow is the per-row projection of one type_registry entry (L2 §1.4.2),
// the install-facing shape (Lookup/List). Payload is opaque (Level A).
type TypeRow struct {
	Type           string
	HandlerActorID actor.ActorID
	HandlerBinding actor.Binding
	MaxPendingMs   int64
	AllowedKinds   []message.Kind
}

// Validate covers surface-level non-empty + binding-format checks.
func (t TypeRow) Validate() error {
	if t.Type == "" {
		return errors.New("storespec: TypeRow.Type required")
	}
	if t.HandlerActorID == "" {
		return fmt.Errorf("storespec: TypeRow[%s].HandlerActorID required", t.Type)
	}
	if _, ok := actor.ParseBinding(string(t.HandlerBinding)); !ok {
		return fmt.Errorf("storespec: TypeRow[%s].HandlerBinding %q invalid", t.Type, t.HandlerBinding)
	}
	if t.MaxPendingMs <= 0 {
		return fmt.Errorf("storespec: TypeRow[%s].MaxPendingMs must be > 0", t.Type)
	}
	return nil
}

// TypeRegistry is the install-facing type_registry READ contract (Lookup /
// List over TypeRow). It is READ-ONLY by construction: the only legitimate way
// to land an installed row is the BeginInstall → emit system.type.installed →
// MarkInstalled state machine (on TypeStore), so a raw `Upsert(installed row)`
// bypass is deliberately ABSENT from every public contract — a bare installed
// write with no mirror event is the type-side analogue of the actor-registry
// Insert/Deregister bypass (§4.5). Impl in runtime/store/type_registry.go.
type TypeRegistry interface {
	Lookup(ctx context.Context, typeName string) (TypeRow, bool, error)
	List(ctx context.Context) ([]TypeRow, error)
}

// TypeView is the read-only projection of one type_registry row the harness
// needs at write time (the minimal subset for steps 5/7).
type TypeView struct {
	Type           string
	AllowedKinds   []message.Kind
	MaxPendingMs   int64
	HandlerActorID actor.ActorID
}

// TypeViewLookup is the harness read seam — fetch one type_view at write
// time. (Named distinctly from TypeRegistry so harness consumes the minimal
// view contract, not the full install one.) Impl in runtime/store.
type TypeViewLookup interface {
	Lookup(ctx context.Context, typeName string) (TypeView, bool, error)
}

// TypeInstallAttempt is the outcome of BeginInstall — the pending row plus
// whether an installed row for the type already existed.
type TypeInstallAttempt struct {
	Row       TypeRow
	AttemptID string
	Existed   bool
}

// TypeStore is the FULL store-side type_registry contract: the install state
// machine (BeginInstall → MarkInstalled / MarkInstallFailed, RecoverInstalling,
// InstallStatus) plus row reads + view lookup. Forward-derived from the
// type_registry table's role (§4.5) — the install-behaviour consumer
// (lib/adapterhost installer, §1.11) ADAPTS to this contract; runtime does not
// wait on it. The harness keeps the narrow TypeViewLookup (via HarnessView).
//
// NOTE: there is NO Upsert here. The installed row is reachable ONLY through
// the BeginInstall → MarkInstalled state machine (which pairs the row with its
// system.type.installed mirror event); a raw installed write would be an
// unmirrored control-plane bypass and is structurally absent (§4.5).
type TypeStore interface {
	Lookup(ctx context.Context, typeName string) (TypeRow, bool, error)
	List(ctx context.Context) ([]TypeRow, error)
	LookupView(ctx context.Context, typeName string) (TypeView, bool, error)
	HarnessView() TypeViewLookup
	BeginInstall(ctx context.Context, row TypeRow) (TypeInstallAttempt, error)
	MarkInstalled(ctx context.Context, typeName, attemptID string) error
	MarkInstallFailed(ctx context.Context, typeName, attemptID, reason string) error
	RecoverInstalling(ctx context.Context, reason string) (int, error)
	InstallStatus(ctx context.Context, typeName string) (status, reason string, ok bool, err error)
}
