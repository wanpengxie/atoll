package accessdoor

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// QueryReject is the runtime QUERY-layer verdict closed set for Stat/List —
// deliberately DISTINCT from proto access.FailureReason (期11 spec §3.7/§8.5
// red line: the five-value proto closed set never grows; Stat/List are Query
// methods, not Operations — §3.8's "Stat/List 绝不进 Operation 闭集" — so
// their own failure vocabulary lives one layer up, in runtime, never in
// proto). "" (the zero value) means no reject — never round-tripped on the
// wire, only tested via QueryReject("").
type QueryReject string

const (
	// QueryNotFound is Stat's zero-rights verdict: authorize failure
	// deliberately dressed as a resolve failure — existence-is-information
	// (§3.6/design doc B1, GitHub-private-repo-404 discipline, not Unix
	// EACCES) — so a caller with zero grants on an id cannot distinguish "no
	// rights" from "does not exist". Pinned to the SAME wire value proto's
	// access.ResourceNotFound uses ("resource_not_found") ON PURPOSE — one
	// word, one HTTP mapping, two closed sets that happen to agree at this
	// one value.
	QueryNotFound QueryReject = "resource_not_found"

	// QueryBadCursor is List's malformed/stale-cursor verdict: an
	// undecodable token, or one minted against a DIFFERENT prefix than the
	// one this call supplies (§3.7's prefix-fingerprint check). Never a
	// proto FailureReason — the five-value closed set stays untouched; HTTP
	// maps this to 400, same disciplinary class as a malformed request.
	QueryBadCursor QueryReject = "bad_cursor"
)

// OpSet is the caller-facing projection of an effectiveOps computation — the
// resource face's public shape for Stat's echoed ops and List's per-row
// projection (期11 spec §3.6/§3.7). Always built by opSetFromEffective, which
// walks objectOps in its FIXED order, so two callers observing the same
// underlying rights always see the same slice (no map-iteration jitter on
// the wire).
type OpSet []access.Operation

// opSetFromEffective converts effectiveOps' internal map[Operation]bool into
// the caller-facing OpSet, preserving objectOps' fixed enumeration order.
func opSetFromEffective(eff map[access.Operation]bool) OpSet {
	out := make(OpSet, 0, len(objectOps))
	for _, op := range objectOps {
		if eff[op] {
			out = append(out, op)
		}
	}
	return out
}

// StatMeta is Stat's caller-facing projection of a resource's metadata — an
// INDEPENDENT type from resourcespec.ResourceMeta (期11 spec §3.6 red line:
// "StatMeta 是独立投影类型,绝不是 ResourceMeta"), because ResourceMeta carries
// PlacementCoord (the opaque storage handle — half of a file's byte
// capability) and reusing that type here would reopen the exact coordinate
// leak the door's Stat/List boundary exists to close. StatMeta structurally
// CANNOT carry a coord: the field does not exist on this type.
type StatMeta struct {
	Kind              resourcespec.ResourceKind
	PlacementKind     resourcespec.PlacementKind
	PlacementDaemonID string
	CreatedAt         int64
	CreatedBy         actor.ActorID
}

// StatResult is Stat's return shape (期11 spec §3.9'): verdict rides Reject,
// never a Go error — a Go error is reserved for infrastructure/assembly
// failure (resolve broken, membership lookup broken), the same two-channel
// discipline invoke's Outcome/error split already draws.
type StatResult struct {
	Meta   StatMeta
	Ops    OpSet
	Reject QueryReject
}

// ListQuery is List's request shape (期11 spec §3.9').
type ListQuery struct {
	Prefix string
	Limit  int
	Cursor string
}

// ListEntry is one List row's minimal, wire-cheap projection — {id,kind,ops}
// only (期11 spec §3.7: "entry={id,kind,ops}精简投影"), never the resource's
// full StatMeta (a page of N rows would otherwise multiply N× the per-row
// cost the frame-cap bound exists to keep in check — see
// TestListFrameCapBound).
type ListEntry struct {
	ID   resource.ResourceID
	Kind resourcespec.ResourceKind
	Ops  OpSet
}

// ListPage is List's return shape. Next=="" means the underlying scan
// reached the end — NOT that Entries is non-empty: a page whose every raw
// row was invisible to caller (any-grant projection) can legally return zero
// Entries with a non-empty Next (期11 spec §3.7: caller must keep pulling
// until Next is empty, not until Entries is empty).
type ListPage struct {
	Entries []ListEntry
	Next    string
	Reject  QueryReject
}

// defaultListLimit / maxListLimit are List's scan-count bounds (期11 spec
// §3.7): limit bounds rows SCANNED, not rows returned after the door's
// any-grant filter — Entries may legitimately be shorter than limit, even
// empty, while Next stays non-empty.
const (
	defaultListLimit = 50
	maxListLimit     = 500
)

// normalizeListLimit applies the default/ceiling rule: a non-positive Limit
// takes the default; anything above the ceiling is capped, never rejected
// (a generous caller-supplied limit is not a protocol violation).
func normalizeListLimit(limit int) int {
	if limit <= 0 {
		return defaultListLimit
	}
	if limit > maxListLimit {
		return maxListLimit
	}
	return limit
}

// --- ingress: Create's structural gate ------------------------------------

// ingressCreate is Create's structural (pre-resolve) gate — the create-arm
// twin of ingress() for Invoke. Failures wrap ErrMalformed (a protocol
// error, never a verdict), mirroring the discipline ingress.go documents.
func ingressCreate(id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) error {
	if err := checkResourceID(id); err != nil {
		return err
	}
	if !resourcespec.ValidKind(spec.Kind) {
		return fmt.Errorf("%w: create kind %q not in the closed set", ErrMalformed, spec.Kind)
	}
	if spec.Dir && spec.WithContent {
		// A directory carries no content — the combination is a conflicting
		// declaration, not silently resolved either way (期11 spec §1.5:
		// "dir=true && with_content=true = ingress 拒").
		return fmt.Errorf("%w: create: dir=true and with_content=true is a conflicting combination (a directory carries no content)", ErrMalformed)
	}
	if spec.Kind == resourcespec.KindFile && initial != nil {
		// file's bytes NEVER ride this param (期11 spec §1.4: "file 恒空,
		// 字节走§1.5备字节流") — a non-nil initial here is a caller trying to
		// smuggle file content through the wrong channel, the same class of
		// violation §8.1's "file 字节永不进控制面" red line names.
		return fmt.Errorf("%w: create: kind=file must not carry initial bytes (file content never rides Create's initial param)", ErrMalformed)
	}
	return nil
}

// --- door-side implementations ---------------------------------------------

// create runs the create decision tree under caller: kv lands immediately
// (unchanged from the pre-§3 Invoke(OpCreate) branch, now living on its own
// method); file kind uses the fully-wired StorageMounts plus ActorAuthority
// placement chain and fails honestly if no unambiguous target is available.
func (d *door) create(ctx context.Context, caller actor.ActorID, id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) (Outcome, error) {
	d.resourceGate.Lock()
	defer d.resourceGate.Unlock()
	_, exists, err := d.deps.Registry.Resolve(ctx, id)
	if err != nil {
		return Outcome{}, err
	}
	if exists {
		return Outcome{RejectReason: access.AlreadyExists}, nil
	}
	_, member, err := d.deps.Authority.LookupActive(ctx, caller)
	if err != nil {
		return Outcome{}, err
	}
	if !member {
		return Outcome{RejectReason: access.AccessDenied}, nil
	}
	world, found, err := d.deps.Authority.WorldOf(ctx, caller)
	if err != nil {
		return Outcome{}, err
	}
	if !found {
		return Outcome{RejectReason: access.AccessDenied}, nil
	}
	birth := resourcespec.ResourceBirthPlan{Authority: resourcespec.BirthCreatorIdentity}
	if world == storespec.WorldRun {
		birth.Authority = resourcespec.BirthChannelOwned
	} else if world != storespec.WorldDurable {
		return Outcome{}, fmt.Errorf("accessdoor: invalid actor world %d", world)
	}

	switch spec.Kind {
	case resourcespec.KindKV:
		if err := d.deps.Registry.Create(ctx, id, resourcespec.KindKV, caller, "", "", initial, birth); err != nil {
			return createVerdict(ctx, err)
		}
		if err := d.installCreatorOverlay(ctx, resourcespec.LandedResource{ID: id, CreatedBy: caller, Birth: birth}); err != nil {
			return Outcome{}, err
		}
		return Outcome{}, nil

	case resourcespec.KindFile:
		daemonID, perr := d.choosePlacement(ctx, caller)
		if perr != nil {
			// Placement failure (no candidate / ambiguous / assembly gap) is
			// an infra-shaped Go error, never a fabricated verdict — same
			// discipline the earlier §3-era stub already established (a
			// driver_error would falsely imply the resource's OWN driver
			// failed, when in fact no placement was ever chosen to try).
			return Outcome{}, perr
		}
		coord, cerr := resourcespec.GenerateCoord()
		if cerr != nil {
			return Outcome{}, cerr
		}
		reservationID, rerr := d.deps.Registry.ReserveCreate(ctx, id, resourcespec.KindFile, caller, daemonID, coord, spec.Dir, birth)
		if rerr != nil {
			return Outcome{}, rerr
		}
		if spec.WithContent {
			// Content-bearing file create: the reservation is durable (coord +
			// door-authenticated creator are safe against a crash from this
			// instant on, §1.7). The write-path handle that actually carries
			// bytes to daemon staging is the SAME FileRoute redemption
			// OpRead/OpWrite(file) uses (§3.5/§5) — the daemon side's post-
			// rename fsync must additionally fire Committed(reservationID)
			// (§1.7), which reservationID here carries through to the
			// redeemed LocalWriteHandle/Stream. A daemon that never receives
			// this reservation's AllocRequest (in practice: never redeems
			// the route at all) leaves it to the Scrubber's timeout sweep
			// (§1.7's third trigger) — no cleanup needed here.
			// with_content is dir=false by construction (dir && with_content is
			// an ingressCreate-rejected combination) — a content-bearing create's
			// write route is always the single-file staging path, never a dir lease.
			route, rerr := d.resolveFileRoute(ctx, caller, daemonID, coord, access.OpWrite, reservationID, false)
			if rerr != nil {
				return Outcome{}, rerr
			}
			return Outcome{Route: route}, nil
		}
		// Content-less create (dir=true, or an empty regular file): §1.5's
		// synchronous path — AllocRequest's OWN ack IS the landing signal (no
		// bytes ever move, so there is no separate daemon-initiated Committed
		// round trip to wait for). A reservation still exists first (durable
		// coord/creator, §1.5: "无内容...仍走reservation保证coord/授权
		// durable"), so a crash between ReserveCreate and here leaves a
		// clean, Scrubber-reclaimable orphan, never a half-built visible row.
		if d.deps.StorageControl == nil {
			return Outcome{}, fmt.Errorf("accessdoor: file kind alloc routing not wired (Deps.StorageControl is nil)")
		}
		if aerr := d.deps.StorageControl.AllocRequest(ctx, daemonID, StorageAllocSpec{
			ChannelID: d.deps.ChannelID, Coord: coord, Dir: spec.Dir,
		}); aerr != nil {
			// Alloc failed / daemon unreachable / timed out: the reservation
			// is left standing — the Scrubber's §1.7 timeout sweep (server-
			// side, by reserved_at) reclaims it. Nothing to undo here.
			return Outcome{}, aerr
		}
		_, found, cerr := d.commitReservationLocked(ctx, reservationID)
		if cerr != nil {
			if errors.Is(cerr, resourcespec.ErrReservationLost) {
				// This create lost the same-resource_id race (期11 S2,
				// transfer-lifecycle-spec.md §3's #2): some OTHER reservation
				// already landed the resource id first. The store already
				// deleted this reservation row (ErrReservationLost's own
				// doc), so there is nothing left to retry — the caller must
				// see the SAME already_exists verdict a synchronous collision
				// would produce, never a fabricated success (a bare
				// `return Outcome{}, nil` here would be the "并发败者假成功"
				// bug: the caller believes it created the resource when
				// another caller actually owns it).
				//
				// 期11 review §2.5 #B: reclaim this loser's orphaned coord. The
				// AllocRequest above already created an empty live/<coord> on the
				// daemon; the with-content path collects such a loser via
				// CommittedReply.Lost→ReclaimCoord, but a content-less create has
				// no byte stream / Committed round trip, so the door issues the
				// reclaim synchronously here (the mirror signal on the same
				// home→daemon channel). Best-effort — a failed reclaim is
				// WARN-logged (nil-safe Deps.Logger) and otherwise swallowed —
				// the verdict must stay AlreadyExists regardless: a missed
				// reclaim leaves at worst an empty live/<coord> directory, never
				// a correctness fault, and the daemon's ReclaimCoord is
				// idempotent so a later duplicate never double-frees.
				if rerr2 := d.deps.StorageControl.ReclaimRequest(ctx, daemonID, coord); rerr2 != nil && d.deps.Logger != nil {
					d.deps.Logger.Warn("accessdoor.reclaim_failed",
						"channel", string(d.deps.ChannelID), "daemon", daemonID,
						"coord", coord, "err", rerr2)
				}
				return Outcome{RejectReason: access.AlreadyExists}, nil
			}
			return createVerdict(ctx, cerr)
		}
		if !found {
			// 期11 review残余#4: found=false with cerr==nil means THIS exact
			// reservationID was already gone by the time CommitReservation ran
			// (CommitReservation's own doc: "already committed by an earlier
			// replay... or swept by the server's own §1.7 timeout sweep — a
			// clean no-op, not an error") — distinct from ErrReservationLost's
			// same-resource_id race above, which always carries a non-nil
			// error. Nothing was ever landed for this create: reporting
			// success here would be the exact "假成功" bug the Lost branch
			// above already guards against, just reached through the OTHER
			// no-row path. Never fabricate accept on a no-op commit.
			return Outcome{RejectReason: access.DriverError}, nil
		}
		return Outcome{}, nil

	default:
		// Unreachable: ingressCreate already gated spec.Kind against
		// ValidKind. Defensive.
		return Outcome{}, ErrMalformed
	}
}

// stat runs the read-face projection: resolve, then any-grant visibility via
// the SAME effectiveOps union set.go's decay law shares with the set arm
// (期11 spec §2 item 2's three-loci contract — this is the second locus).
// Zero rights masquerades as not_found (§3.6/design doc B1) — a deliberate,
// documented choice, not a bug to "fix" back to access_denied.
func (d *door) stat(ctx context.Context, caller actor.ActorID, id resource.ResourceID) (StatResult, error) {
	d.resourceGate.Lock()
	defer d.resourceGate.Unlock()
	meta, exists, err := d.deps.Registry.Resolve(ctx, id)
	if err != nil {
		return StatResult{}, err
	}
	if !exists {
		return StatResult{Reject: QueryNotFound}, nil
	}
	eff, err := d.effectiveOps(ctx, caller, id)
	if err != nil {
		return StatResult{}, err
	}
	ops := opSetFromEffective(eff)
	if len(ops) == 0 {
		return StatResult{Reject: QueryNotFound}, nil
	}
	return StatResult{
		Meta: StatMeta{
			Kind:              meta.Kind,
			PlacementKind:     meta.PlacementKind,
			PlacementDaemonID: meta.PlacementDaemonID,
			CreatedAt:         meta.CreatedAt,
			CreatedBy:         meta.CreatedBy,
		},
		Ops: ops,
	}, nil
}

// list runs the read-face pagination: decode/validate the door's own
// (prefix-fingerprinted) cursor, delegate the raw range scan to
// Registry.List, then any-grant-project each returned row using the grant
// projection List ALREADY fetched (effectiveOpsFromGrants) — the whole point
// of Registry.List returning full per-row grants is so a page of N
// resources costs ONE membership check total, never N×(ActorAllows+
// MembersAllow) round trips (期11 spec §1.9'⑤/§3.7).
func (d *door) list(ctx context.Context, caller actor.ActorID, q ListQuery) (ListPage, error) {
	d.resourceGate.Lock()
	defer d.resourceGate.Unlock()
	limit := normalizeListLimit(q.Limit)

	registryCursor, ok := decodeQueryCursor(q.Prefix, q.Cursor)
	if !ok {
		return ListPage{Reject: QueryBadCursor}, nil
	}

	rows, nextRegistryCursor, err := d.deps.Registry.List(ctx, q.Prefix, limit, registryCursor)
	if err != nil {
		if errors.Is(err, resourcespec.ErrMalformedCursor) {
			return ListPage{Reject: QueryBadCursor}, nil
		}
		return ListPage{}, err
	}

	_, isMember, err := d.deps.Authority.LookupActive(ctx, caller)
	if err != nil {
		return ListPage{}, err
	}

	entries := make([]ListEntry, 0, len(rows))
	for _, row := range rows {
		eff := effectiveOpsFromGrants(caller, row.Grants, isMember)
		// Overlay half: session grants (forked grantees, forked-creator
		// convenience) live only in the volatile overlay — Invoke/Stat merge
		// them via effectiveOps, so List must project the same union or an
		// overlay-granted caller cannot discover a resource it can access.
		// In-memory map lookups; still one page-wide membership check.
		for _, op := range objectOps {
			if eff[op] {
				continue
			}
			allowed, oerr := d.deps.Overlay.ActorAllows(ctx, caller, row.ID, op)
			if oerr != nil {
				return ListPage{}, oerr
			}
			if allowed {
				eff[op] = true
			}
		}
		ops := opSetFromEffective(eff)
		if len(ops) == 0 {
			continue // any-grant projection: zero rights on this row = invisible
		}
		entries = append(entries, ListEntry{ID: row.ID, Kind: row.Meta.Kind, Ops: ops})
	}

	next := ""
	if nextRegistryCursor != "" {
		next = encodeQueryCursor(q.Prefix, nextRegistryCursor)
	}
	return ListPage{Entries: entries, Next: next}, nil
}

// effectiveOpsFromGrants computes the SAME union formula effectiveOps does
// (ActorAllows(caller) ∪ (MembersAllow ∧ IsMember(caller))) directly over an
// ALREADY-FETCHED grant projection (a ResourceRow.Grants slice), rather than
// re-querying the registry per op per row — List's row-level shortcut. isMember
// is resolved ONCE for the whole page (membership does not change mid-scan),
// mirroring effectiveOps' own single-resolve discipline.
func effectiveOpsFromGrants(caller actor.ActorID, grants []access.Grant, isMember bool) map[access.Operation]bool {
	eff := make(map[access.Operation]bool, len(objectOps))
	for _, g := range grants {
		var applies bool
		switch g.GranteeKind {
		case access.GranteeActor:
			applies = g.Grantee == caller
		case access.GranteeMembers:
			applies = isMember
		}
		if !applies {
			continue
		}
		for _, op := range g.Ops {
			eff[op] = true
		}
	}
	return eff
}

// --- Query-layer cursor: prefix-fingerprint + opaque wrap of Registry.List's
//     own cursor (a DIFFERENT, one-layer-up concept — see resourcespec's
//     Registry.List doc; don't conflate them) ---------------------------------

// prefixFingerprint is a short, non-cryptographic (collision resistance
// unneeded — it only needs to catch an accidental/adversarial prefix swap
// under an old cursor, not resist a targeted preimage) digest of the prefix a
// cursor was minted against — §3.7's "prefix 变→bad_cursor" check.
func prefixFingerprint(prefix string) string {
	sum := sha256.Sum256([]byte(prefix))
	return hex.EncodeToString(sum[:8])
}

// encodeQueryCursor wraps Registry.List's own opaque nextCursor token behind
// the door's prefix-fingerprinted envelope.
func encodeQueryCursor(prefix string, registryCursor string) string {
	raw := prefixFingerprint(prefix) + "\x00" + registryCursor
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// decodeQueryCursor unwraps a caller-supplied cursor, verifying it was
// minted against THIS SAME prefix. An empty cursor is the legal "start from
// the beginning" value (ok=true, registryCursor=""). Any other shape
// failure — bad base64, wrong field count, mismatched fingerprint — is
// ok=false, mapped by the caller to QueryBadCursor.
func decodeQueryCursor(prefix string, cursor string) (registryCursor string, ok bool) {
	if cursor == "" {
		return "", true
	}
	raw, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return "", false
	}
	parts := strings.SplitN(string(raw), "\x00", 2)
	if len(parts) != 2 {
		return "", false
	}
	if parts[0] != prefixFingerprint(prefix) {
		return "", false
	}
	return parts[1], true
}
