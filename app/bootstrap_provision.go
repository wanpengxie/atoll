package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Home provisioning names. Ordinary rows, no special-casing anywhere else:
// the home channel is just the FIRST membrane opened for the owner (day0
// zero-ritual entry — 组织性是生长轴不是入场券, channel-organization-model §1.2),
// and the local daemon is an ordinary device row whose key the app minted
// itself (daemon 仪式内化). localDaemonName is DISPLAY ONLY — recovery goes by
// the claimed row id, never by name (home 模型拍点: 身份=持有凭据).
const (
	homeChannelName = "home"
	localDaemonName = "local-device"
)

// ProvisionSpec is what the assembly root (engineboot) resolved from the node
// home before calling: where the rotated token publishes, and which daemon row
// this installation claims as its local device ("" = no claim, mint fresh).
// The claim comes from the device home's identity file — a possession, not a
// name: the daemons row stays the identity authority, the file only proves
// this installation holds it.
type ProvisionSpec struct {
	TokenPath string
	DaemonID  string
}

// ProvisionResult is what `atoll up` needs to finish assembly: the bearer
// token file for shells, and the local daemon's identity for the in-process
// device carrier (which the assembly root persists back into the device home).
type ProvisionResult struct {
	TokenPath     string
	OwnerID       string
	HomeChannelID string
	DaemonID      string
	DaemonKey     string
}

// ProvisionLocalNode makes a fresh engine immediately usable ("一条命令起全套，
// 零手动链接"): owner principal + rotated bearer token file, the home channel
// (owner enrolled by the ordinary creation path), a local daemon row claimed by
// id or freshly minted, and the daemon bound to home. Every step goes through
// the same verbs the API handlers use — provisioning has no side door — and
// every step is idempotent or convergent, so running it on an existing
// installation converges instead of erroring:
//   - owner/user: reused if present; the token session ROTATES every call
//     (BootstrapOwnerToken: exactly one live session, restart IS the rotation);
//   - home channel: same-owner/same-name re-create returns the present row;
//   - daemon row: claimed by spec.DaemonID when that row is alive; a deleted
//     or unclaimed row means a fresh mint (the row is authoritative — deleting
//     a device really retires it);
//   - binding: the attach verb's predicate makes a bound daemon a no-op.
//
// It must run with the convergence arm started (App.Start) — the assembly
// root provisions between Boot and Serve, before any listener exists.
//
// Error contract: a bind failure (or cancellation mid-bind) returns a PARTIAL
// ProvisionResult alongside the error — the minted device row's id/key — and
// the caller must persist that claim even then, or every retry mints another
// orphan row.
func (a *App) ProvisionLocalNode(ctx context.Context, spec ProvisionSpec) (ProvisionResult, error) {
	if _, err := BootstrapOwnerToken(ctx, a.db, spec.TokenPath); err != nil {
		return ProvisionResult{}, err
	}
	var ownerID string
	if err := a.db.QueryRowContext(ctx,
		`SELECT id FROM users WHERE email = ?`, bootstrapOwnerEmail,
	).Scan(&ownerID); err != nil {
		return ProvisionResult{}, fmt.Errorf("provision: owner lookup: %w", err)
	}

	accepted, _, conflict, _, err := a.createGroupChannel(ctx, ownerID, homeChannelName, nil)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("provision: home channel: %w", err)
	}
	if conflict {
		// A channel named "home" exists but belongs to someone else / another
		// shape — provisioning must not steal or mutate it.
		return ProvisionResult{}, errors.New("provision: channel name 'home' is taken with a different owner or shape")
	}

	daemonID, daemonKey, err := a.claimOrMintDaemon(ctx, ownerID, spec.DaemonID)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("provision: local daemon: %w", err)
	}

	// On reopen the home channel converges asynchronously (createGroupChannel's
	// replay path only pokes), so the bind can race the bundle coming online —
	// the same transient the HTTP caller sees as a retryable "unavailable".
	// The attach verb is idempotent, so converge-by-retry is safe and bounded.
	result := ProvisionResult{
		TokenPath:     spec.TokenPath,
		OwnerID:       ownerID,
		HomeChannelID: string(accepted.ID),
		DaemonID:      daemonID,
		DaemonKey:     daemonKey,
	}
	var bindErr error
	for attempt := 0; attempt < 30; attempt++ {
		if _, bindErr = a.attachDaemonCore(ctx, ownerID, accepted.ID, daemonID); bindErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			// PARTIAL result with the error: the device row above is already
			// minted, and the caller must persist that claim even on failure —
			// dropping it here would orphan a fresh row on every retry.
			return result, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if bindErr != nil {
		// Same partial-result contract as the ctx branch.
		return result, fmt.Errorf("provision: bind daemon to home: %w", bindErr)
	}
	if err := a.convergeBootstrapCodex(ctx, ownerID, accepted.ID); err != nil {
		return result, err
	}
	return result, nil
}

// claimOrMintDaemon resolves the local device row. A non-empty claimedID that
// names a live row owned by this owner is honored (same id, the row's current
// key); anything else — no claim, a deleted row, someone else's row — mints a
// fresh device. Never resolved by name: names are display-only and freely
// duplicated, so a user-created daemon that happens to be called
// "local-device" can never be adopted as this installation's device.
func (a *App) claimOrMintDaemon(ctx context.Context, ownerID, claimedID string) (string, string, error) {
	if claimedID != "" {
		var key string
		err := a.db.QueryRowContext(ctx,
			`SELECT api_key FROM daemons WHERE id=? AND owner_id=? AND deleted_at IS NULL`,
			claimedID, ownerID,
		).Scan(&key)
		if err == nil {
			return claimedID, key, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", "", err
		}
		// Claimed row is gone: the delete was authoritative, mint fresh below.
	}
	return a.createDaemonRow(ctx, ownerID, localDaemonName)
}
