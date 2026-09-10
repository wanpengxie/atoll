// Package effectcap owns incarnation-local effect admission capabilities.
// A Scope is deliberately opaque outside this package: it is a causal
// snapshot and a revocable admission token, not an authorization system.
package effectcap

import (
	"sync"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// Snapshot is the immutable channel causality captured when a Scope is
// minted. The concrete ID representation stays outside the Runtime contract.
type Snapshot struct {
	Cause         message.Cause
	Context       harness.Context
	ParentID      string
	CorrelationID string
}

// Scope can only be minted, resolved, or revoked by its owning Vault.
type Scope struct {
	vault *Vault
	id    uint64
}

// Vault owns every Scope for one actor incarnation.
type Vault struct {
	mu     sync.Mutex
	sealed bool
	next   uint64
	rows   map[uint64]row
}

type row struct {
	open     bool
	snapshot Snapshot
}

func NewVault() *Vault { return &Vault{rows: make(map[uint64]row)} }

// Mint returns a fresh open Scope. Mint after Seal returns the zero Scope.
func (v *Vault) Mint(cause message.Cause, app harness.Context) Scope {
	parentID, correlationID := cause.Resolve("")
	if v == nil {
		return Scope{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.sealed || v.next == ^uint64(0) {
		return Scope{}
	}
	v.next++
	v.rows[v.next] = row{open: true, snapshot: Snapshot{ParentID: string(parentID), CorrelationID: string(correlationID), Cause: cause, Context: app.Clone()}}
	return Scope{vault: v, id: v.next}
}

// ResolveOpen atomically checks both the Vault and Scope cut and returns the
// immutable causal snapshot used by a downstream effect.
func (v *Vault) ResolveOpen(scope Scope) (Snapshot, bool) {
	if v == nil || scope.vault != v || scope.id == 0 {
		return Snapshot{}, false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	row, ok := v.rows[scope.id]
	if v.sealed || !ok || !row.open {
		return Snapshot{}, false
	}
	snapshot := row.snapshot
	snapshot.Context = snapshot.Context.Clone()
	return snapshot, true
}

// Revoke is an admission cut for one Scope. It is idempotent.
func (v *Vault) Revoke(scope Scope) {
	if v == nil || scope.vault != v || scope.id == 0 {
		return
	}
	v.mu.Lock()
	delete(v.rows, scope.id)
	v.mu.Unlock()
}

// Seal permanently prevents every future admission through this Vault.
func (v *Vault) Seal() {
	if v == nil {
		return
	}
	v.mu.Lock()
	v.sealed = true
	v.rows = nil
	v.mu.Unlock()
}

func (v *Vault) Sealed() bool {
	if v == nil {
		return true
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.sealed
}
