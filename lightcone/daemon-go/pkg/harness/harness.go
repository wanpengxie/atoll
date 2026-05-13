// Package harness implements the v4 Message-Write Harness contract
// (L1 §10.2 / §10.2.1). It exposes:
//
//   - The shared `Write(ctx, env, callerCtx)` function executing the
//     normalize pass + 9-step validation chain on a single envelope.
//   - The in_worker_bus binding (this package) returning Result.Err
//     style (no panics).
//   - The dependency interfaces (Store / ActorLookup / TypeLookup /
//     WorkerLockLookup / Dispatcher) so callers can wire sqlite (real)
//     or in-memory (tests) backends.
//
// The daemon_rpc HTTP binding lives in internal/harness — it imports
// the public Write here and maps RejectError → HTTP per L2 §3.6.1.
//
// Authoritative spec text:
//
//   - L1 §10.2     contract
//   - L1 §10.2.1   pseudocode (the implementation reference; behaviour
//     here matches step-for-step)
//   - L1 §10.2.2   canonical_hash contract
//   - L1 §10.3.1   harness reject reasons (closed set)
//   - L2 §3.6.1    daemon_rpc HTTP status mapping
package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/canonical"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// Deps bundles the dependency interfaces the shared Write body needs.
// Bindings construct one of these per channel; tests build one with
// in-memory mocks.
//
// All fields are required except Dispatcher (defaults to NoopDispatcher)
// and Clock (defaults to nowMillis). New / NewWith helpers handle the
// defaults.
type Deps struct {
	Store       Store
	Actors      ActorLookup
	Types       TypeLookup
	WorkerLocks WorkerLockLookup // may be nil when fencing not applicable
	Dispatcher  Dispatcher       // defaults to NoopDispatcher
	Clock       Clock            // defaults to UnixMilli now()
	// ChannelID is the channel this Deps is bound to. Step 7 uses it to
	// reject doc_refs crossing channel boundaries; Step 0 onward never
	// rewrites envelope.channel_id (callers must supply the matching
	// value or Step 2 rejects via missing_required_field).
	ChannelID string
}

// New constructs a Deps with sensible defaults. Dispatcher defaults to
// NoopDispatcher; Clock defaults to UnixMilli wall-clock.
func New(
	store Store,
	actors ActorLookup,
	types TypeLookup,
	workerLocks WorkerLockLookup,
	channelID string,
) Deps {
	return Deps{
		Store:       store,
		Actors:      actors,
		Types:       types,
		WorkerLocks: workerLocks,
		Dispatcher:  NoopDispatcher{},
		Clock:       defaultClock,
		ChannelID:   channelID,
	}
}

// Write executes the L1 §10.2.1 9-step validation chain on env, then
// (if all steps pass) inserts env and dispatches it. On reject it
// returns (nil, *RejectError). On unrecoverable infrastructure failure
// (sql, ctx done, etc.) it returns (nil, error) where error is NOT a
// RejectError — callers should distinguish via errors.As.
//
// `env` is modified in place during the normalize pass — caller MUST
// hand in a value it owns (or a copy if the caller plans to retry with
// fresh defaults).
//
// `callerCtx` carries the auth + trigger context the binding extracted
// from its transport. The harness trusts callerCtx as authoritative.
func Write(
	ctx context.Context,
	deps Deps,
	env *v4types.Envelope,
	callerCtx CallerCtx,
) (*Result, error) {
	if env == nil {
		return nil, errors.New("harness: envelope is nil")
	}
	if err := validateDeps(deps); err != nil {
		return nil, err
	}

	// ----- Step 0a: Normalize ----------------------------------------
	// Done before any reject path so Step 0.5 / Step 8 catch can canonical-
	// compare apples to apples.
	if err := normalize(env, callerCtx); err != nil {
		return nil, err
	}

	// ----- Step 0.5: Universal id-conflict dedupe --------------------
	// Pre-check on envelope.id covering every kind so turn replay /
	// network retry / any same-id resend dedupes idempotently. A second
	// catch on UNIQUE violation lives inside Step 8 (else branch) for
	// the race where two writes pass Step 0.5 then collide at INSERT.
	if r, err := dedupeByID(ctx, deps, env); err != nil {
		return nil, err
	} else if r != nil {
		return r, nil
	}

	// ----- Step 1: Auth ----------------------------------------------
	if !callerCtx.Authenticated {
		return nil, rejectf(v4types.HarnessAuthFailed, "caller is not authenticated")
	}

	// ----- Step 2: Required fields + ADT enum + One Law pairing ------
	if err := checkRequiredFields(env); err != nil {
		return nil, err
	}

	// ----- Step 3: Sender identity + actor_registry + fencing --------
	actor, err := checkSenderIdentity(ctx, deps, env, callerCtx)
	if err != nil {
		return nil, err
	}
	// Force-overwrite sender.kind from registry (caller's value is not
	// trusted past Step 3). The mismatch check above already rejected
	// when caller's declared kind ≠ registry.
	env.Sender.Kind = actor.Kind

	// ----- Step 4: Type whitelist ------------------------------------
	typeInfo, err := checkTypeWhitelist(deps, env)
	if err != nil {
		return nil, err
	}

	// ----- Step 5: kind × type + audience narrow ---------------------
	if err := checkKindTypeAndAudience(ctx, deps, env, typeInfo); err != nil {
		return nil, err
	}

	// ----- Step 6: Payload schema ------------------------------------
	if err := checkPayloadSchema(env, typeInfo); err != nil {
		return nil, err
	}

	// ----- Step 7: doc_refs paths ------------------------------------
	if err := checkDocRefs(env, deps.ChannelID); err != nil {
		return nil, err
	}

	// ----- Step 8: The One Law + INSERT ------------------------------
	result, err := commitMessage(ctx, deps, env, typeInfo)
	if err != nil {
		return nil, err
	}

	// ----- Dispatch (out-of-tx, best-effort) -------------------------
	// Per L1 §10.2.1: dispatch errors do NOT roll back the insert. We
	// surface them only via the implementation's own logging — Write
	// always returns Ok if Step 8 committed.
	_ = deps.Dispatcher.Dispatch(ctx, env)

	return result, nil
}

// validateDeps checks that the binding wired the mandatory fields.
// Returns a non-RejectError programming error — bindings (not callers)
// observe these and panic-equivalent fail.
func validateDeps(d Deps) error {
	if d.Store == nil {
		return errors.New("harness: Deps.Store is nil")
	}
	if d.Actors == nil {
		return errors.New("harness: Deps.Actors is nil")
	}
	if d.Types == nil {
		return errors.New("harness: Deps.Types is nil")
	}
	if d.Dispatcher == nil {
		return errors.New("harness: Deps.Dispatcher is nil")
	}
	if d.Clock == nil {
		return errors.New("harness: Deps.Clock is nil")
	}
	if strings.TrimSpace(d.ChannelID) == "" {
		return errors.New("harness: Deps.ChannelID is empty")
	}
	return nil
}

// ----------------------------------------------------------------------------
// Step 0a — normalize
// ----------------------------------------------------------------------------

// normalize applies the L1 §10.2.1 Step 0a fill-in rules:
//
//   - audience nil/empty → ['*']
//   - visibility empty   → 'public'
//   - kind missing for core type → core default kind
//   - correlation_id empty → 3-tier fallback (trigger / explicit / self)
//
// `kind` for business types is left empty when missing; Step 5 rejects
// with `kind_not_allowed`. Parent_id is never auto-filled.
//
// Payload normalization: an entirely empty (`nil` / `[]byte("")`) payload
// is filled with `{}` so canonical_hash and schema validators have a
// stable shape. L0 §2.2 declares `payload={}` legal and `payload=null`
// not; the wire baseline therefore upgrades absent payloads to `{}`.
func normalize(env *v4types.Envelope, callerCtx CallerCtx) error {
	if len(env.Audience) == 0 {
		env.Audience = []string{"*"}
	}
	if env.Visibility == "" {
		env.Visibility = v4types.VisibilityPublic
	}
	if env.Kind == "" && IsCoreType(env.Type) {
		env.Kind = coreDefaultKind(env.Type)
	}
	if env.CorrelationID == "" {
		switch {
		case callerCtx.Trigger != nil && callerCtx.Trigger.CorrelationID != "":
			env.CorrelationID = callerCtx.Trigger.CorrelationID
		case callerCtx.ExplicitCorrelationID != "":
			env.CorrelationID = callerCtx.ExplicitCorrelationID
		default:
			// Self-rooted: new root chain. envelope.id may itself be empty
			// (caller bug); Step 2 then rejects via missing_required_field.
			env.CorrelationID = env.ID
		}
	}
	if len(env.Payload) == 0 {
		env.Payload = json.RawMessage("{}")
	}
	return nil
}

// ----------------------------------------------------------------------------
// Step 0.5 — universal id dedupe
// ----------------------------------------------------------------------------

// dedupeByID performs the L1 §10.2.1 Step 0.5 pre-check. On a match it
// compares the existing row's canonical_hash against the incoming
// envelope's; equal → idempotent success; different → message_id_conflict.
//
// Returns (nil, nil) when no row exists — caller continues with Step 1.
func dedupeByID(ctx context.Context, deps Deps, env *v4types.Envelope) (*Result, error) {
	if env.ID == "" {
		return nil, nil
	}
	existing, err := deps.Store.FindByID(ctx, env.ID)
	if err != nil {
		return nil, fmt.Errorf("harness: dedupe FindByID: %w", err)
	}
	if existing == nil {
		return nil, nil
	}
	return decideDedupe(env, existing)
}

// decideDedupe is the shared canonical_hash comparison used by Step 0.5
// and Step 8 catch. Equal hash → idempotent Ok wrapping the existing
// row; different → message_id_conflict.
func decideDedupe(incoming, existing *v4types.Envelope) (*Result, error) {
	existingHash, err := canonical.CanonicalHash(*existing)
	if err != nil {
		return nil, fmt.Errorf("harness: hash existing: %w", err)
	}
	incomingHash, err := canonical.CanonicalHash(*incoming)
	if err != nil {
		return nil, fmt.Errorf("harness: hash incoming: %w", err)
	}
	if existingHash == incomingHash {
		return &Result{
			ID:            existing.ID,
			CorrelationID: existing.CorrelationID,
			Kind:          existing.Kind,
			Dedupe:        true,
		}, nil
	}
	return nil, &RejectError{
		Reason: v4types.HarnessMessageIDConflict,
		Detail: fmt.Sprintf("envelope id %q already exists with different content (existing hash %s)", existing.ID, existingHash),
	}
}

// ----------------------------------------------------------------------------
// Step 2 — required fields + ADT enum
// ----------------------------------------------------------------------------

func checkRequiredFields(env *v4types.Envelope) error {
	if env.ID == "" {
		return rejectf(v4types.HarnessMissingRequiredField, "envelope.id is required")
	}
	if env.TS == 0 {
		return rejectf(v4types.HarnessMissingRequiredField, "envelope.ts is required")
	}
	if env.ChannelID == "" {
		return rejectf(v4types.HarnessMissingRequiredField, "envelope.channel_id is required")
	}
	if env.Sender.ID == "" {
		return rejectf(v4types.HarnessMissingRequiredField, "envelope.sender.id is required")
	}
	if env.Type == "" {
		return rejectf(v4types.HarnessMissingRequiredField, "envelope.type is required")
	}
	if len(env.Payload) == 0 {
		return rejectf(v4types.HarnessMissingRequiredField, "envelope.payload is required")
	}
	if env.Kind == "" {
		// Business types reach here when caller omitted kind. The
		// canonical reject reason is `kind_not_allowed` per L1 §10.2
		// step 5 ("business type 不走默认值——caller 必须显式指定 kind"),
		// but Step 2 ADT check fires earlier — kind=='' is not in the
		// enum, so `kind_invalid` is the right reason.
		return rejectf(v4types.HarnessKindInvalid, "envelope.kind is required and must be one of {event, request, response}")
	}
	if !isValidKind(env.Kind) {
		return rejectf(v4types.HarnessKindInvalid, "envelope.kind %q is not one of {event, request, response}", env.Kind)
	}
	if env.Kind == v4types.KindResponse && env.ParentID == "" {
		return rejectf(v4types.HarnessResponseMissingParentID, "kind=response requires non-empty parent_id (The One Law pairing)")
	}
	if !isValidVisibility(env.Visibility) {
		return rejectf(v4types.HarnessMissingRequiredField, "envelope.visibility %q is not one of {public, private, system}", env.Visibility)
	}
	return nil
}

func isValidKind(k v4types.Kind) bool {
	switch k {
	case v4types.KindEvent, v4types.KindRequest, v4types.KindResponse:
		return true
	}
	return false
}

func isValidVisibility(v v4types.Visibility) bool {
	switch v {
	case v4types.VisibilityPublic, v4types.VisibilityPrivate, v4types.VisibilitySystem:
		return true
	}
	return false
}

// ----------------------------------------------------------------------------
// Step 3 — sender identity + actor registry + fencing
// ----------------------------------------------------------------------------

func checkSenderIdentity(
	ctx context.Context,
	deps Deps,
	env *v4types.Envelope,
	callerCtx CallerCtx,
) (*ActorMeta, error) {
	if env.Sender.ID != callerCtx.ActorID {
		return nil, rejectf(v4types.HarnessSenderMismatch,
			"envelope.sender.id %q does not match caller actor_id %q",
			env.Sender.ID, callerCtx.ActorID)
	}
	actor, err := deps.Actors.Get(ctx, env.Sender.ID)
	if err != nil {
		return nil, fmt.Errorf("harness: actor lookup: %w", err)
	}
	if actor == nil {
		// Per L1 §10.2.1: "sender 完全不在 registry → 视为 deregistered".
		return nil, rejectf(v4types.HarnessSenderDeregistered,
			"sender %q is not registered", env.Sender.ID)
	}
	if actor.DeregisteredAt != nil && env.Sender.ID != "system" {
		return nil, &RejectError{
			Reason: v4types.HarnessSenderDeregistered,
			Detail: fmt.Sprintf("sender %q deregistered at %d", env.Sender.ID, *actor.DeregisteredAt),
		}
	}
	if callerCtx.DeclaredSenderKind != "" && callerCtx.DeclaredSenderKind != actor.Kind {
		return nil, &RejectError{
			Reason: v4types.HarnessSenderKindMismatch,
			Detail: fmt.Sprintf("declared sender.kind=%q does not match actor_registry kind=%q",
				callerCtx.DeclaredSenderKind, actor.Kind),
		}
	}
	// Fencing: only meaningful when caller supplied a token AND the
	// deps wired a worker_locks lookup. Missing lookup with non-zero
	// token is treated as a programming error (binding wire mistake)
	// and surfaces as worker_fencing_stale to be safe.
	if callerCtx.FencingToken != 0 {
		if deps.WorkerLocks == nil {
			return nil, rejectf(v4types.HarnessWorkerFencingStale,
				"fencing token supplied but worker_locks lookup not configured")
		}
		active, lerr := deps.WorkerLocks.IsActive(ctx, env.Sender.ID, callerCtx.FencingToken)
		if lerr != nil {
			return nil, fmt.Errorf("harness: worker_locks lookup: %w", lerr)
		}
		if !active {
			return nil, &RejectError{
				Reason: v4types.HarnessWorkerFencingStale,
				Detail: fmt.Sprintf("fencing_token %d is no longer active for actor %q",
					callerCtx.FencingToken, env.Sender.ID),
			}
		}
	}
	return actor, nil
}

// ----------------------------------------------------------------------------
// Step 4 — type whitelist
// ----------------------------------------------------------------------------

func checkTypeWhitelist(deps Deps, env *v4types.Envelope) (*TypeInfo, error) {
	if IsCoreType(env.Type) {
		// core types have no TypeInfo row; signal that to downstream
		// steps via a nil pointer.
		return nil, nil
	}
	info, ok := deps.Types.Get(env.Type)
	if !ok {
		return nil, rejectf(v4types.HarnessUnknownType,
			"type %q is neither a core type nor registered in type_registry", env.Type)
	}
	return info, nil
}

// ----------------------------------------------------------------------------
// Step 5 — kind × type allowed_kinds + audience narrow
// ----------------------------------------------------------------------------

func checkKindTypeAndAudience(
	ctx context.Context,
	deps Deps,
	env *v4types.Envelope,
	typeInfo *TypeInfo,
) error {
	var allowedKinds []v4types.Kind
	if typeInfo == nil {
		allowedKinds = coreAllowedKinds(env.Type)
	} else {
		allowedKinds = typeInfo.AllowedKinds
	}
	if !kindInList(env.Kind, allowedKinds) {
		return rejectf(v4types.HarnessKindNotAllowed,
			"kind=%q not in allowed_kinds %v for type=%q", env.Kind, allowedKinds, env.Type)
	}

	if env.Kind == v4types.KindRequest {
		if len(env.Audience) != 1 || env.Audience[0] == "*" {
			return rejectf(v4types.HarnessRequestAudienceInvalid,
				"kind=request requires exactly one concrete receiver in audience (got %v)", env.Audience)
		}
		target := env.Audience[0]
		actor, err := deps.Actors.Get(ctx, target)
		if err != nil {
			return fmt.Errorf("harness: audience actor lookup: %w", err)
		}
		if actor == nil || actor.DeregisteredAt != nil {
			return rejectf(v4types.HarnessAudienceActorNotRegistered,
				"audience actor %q is not registered or deregistered", target)
		}
		if typeInfo != nil && typeInfo.HandlerActorID != "" && typeInfo.HandlerActorID != target {
			return &RejectError{
				Reason: v4types.HarnessAudienceHandlerMismatch,
				Detail: fmt.Sprintf("type %q expects handler_actor_id %q but audience targets %q",
					env.Type, typeInfo.HandlerActorID, target),
			}
		}
	}
	return nil
}

func kindInList(k v4types.Kind, ks []v4types.Kind) bool {
	for _, x := range ks {
		if x == k {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------------
// Step 6 — payload schema
// ----------------------------------------------------------------------------

func checkPayloadSchema(env *v4types.Envelope, typeInfo *TypeInfo) error {
	if typeInfo == nil {
		// Core type — apply the baseline core check.
		if err := validateCorePayload(env.Type, env.Payload); err != nil {
			return rejectf(v4types.HarnessPayloadSchemaViolation,
				"core type %q payload check failed: %v", env.Type, err)
		}
		return nil
	}
	schema := typeInfo.Schemas[env.Kind]
	if schema == nil {
		// No schema declared for this kind → no constraint. M1.3
		// baseline tolerates the absence; type_registry Install ensures
		// schemas_by_kind keys ⊆ allowed_kinds, so a missing schema for
		// an allowed kind means the type owner intentionally elided
		// validation.
		return nil
	}
	var probe any
	if err := json.Unmarshal(env.Payload, &probe); err != nil {
		return rejectf(v4types.HarnessPayloadSchemaViolation,
			"payload is not valid JSON: %v", err)
	}
	if err := schema.Validate(probe); err != nil {
		return rejectf(v4types.HarnessPayloadSchemaViolation,
			"payload does not satisfy schema for type=%q kind=%q: %v", env.Type, env.Kind, err)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Step 7 — doc_refs path checks
// ----------------------------------------------------------------------------

func checkDocRefs(env *v4types.Envelope, channelID string) error {
	if env.DocRefs == nil {
		return nil
	}
	for _, p := range *env.DocRefs {
		if p == "" {
			return rejectf(v4types.HarnessDocRefsInvalid, "doc_refs contains empty path")
		}
		if filepath.IsAbs(p) {
			return rejectf(v4types.HarnessDocRefsInvalid, "doc_refs path %q is absolute", p)
		}
		// `..` rejection covers both the literal segment and any
		// embedded form (e.g. "a/../b"). filepath.Clean normalizes
		// segments so we re-check after cleaning to catch obscure
		// constructs like "./..//x".
		cleaned := filepath.ToSlash(filepath.Clean(p))
		if cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "/../") {
			return rejectf(v4types.HarnessDocRefsInvalid, "doc_refs path %q escapes channel root", p)
		}
		// Cross-channel detection: if a path starts with a channel-id
		// prefix that does not equal ours, reject. M1.3 baseline does
		// not require deeper inspection — paths are channel-local by
		// convention; explicit `channels/<id>/...` form is the only
		// way to address cross-channel and is forbidden here.
		if strings.HasPrefix(cleaned, "channels/") {
			rest := strings.TrimPrefix(cleaned, "channels/")
			parts := strings.SplitN(rest, "/", 2)
			if len(parts) > 0 && parts[0] != channelID {
				return rejectf(v4types.HarnessDocRefsInvalid,
					"doc_refs path %q targets channel %q (not %q)", p, parts[0], channelID)
			}
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// Step 8 — The One Law + INSERT
// ----------------------------------------------------------------------------

// commitMessage executes the kind-aware insert path described in L1
// §10.2.1 Step 8. kind=response uses the IMMEDIATE tx + parent existence
// check + terminal uniqueness check; other kinds simply INSERT and
// fall back to dedupe on UNIQUE.
//
// `tsReceived` is stamped from deps.Clock here so callers always see
// a deterministic value (tests inject a fixed clock).
func commitMessage(
	ctx context.Context,
	deps Deps,
	env *v4types.Envelope,
	typeInfo *TypeInfo,
) (*Result, error) {
	tsReceived := deps.Clock()

	if env.Kind != v4types.KindResponse {
		// kind=event / kind=request → plain INSERT + UNIQUE catch.
		return commitNonResponse(ctx, deps, env, tsReceived)
	}

	// kind=response: validate parent + compute is_terminal + run
	// inside an IMMEDIATE tx so terminal uniqueness check + INSERT are
	// atomic (L1 §10.2.1 "事务边界：Step 8 是唯一需要 store transaction").
	parent, err := deps.Store.FindParent(ctx, env.ParentID)
	if err != nil {
		return nil, fmt.Errorf("harness: parent lookup: %w", err)
	}
	if parent == nil || parent.Kind != v4types.KindRequest {
		return nil, rejectf(v4types.HarnessResponseParentInvalid,
			"response.parent_id %q does not point at an existing kind=request message", env.ParentID)
	}

	env.IsTerminal = computeIsTerminal(env, typeInfo)
	if !env.IsTerminal {
		// Non-terminal response — not subject to The One Law uniqueness.
		// Falls through to the plain INSERT path.
		return commitNonResponse(ctx, deps, env, tsReceived)
	}

	var result *Result
	terr := deps.Store.WithTerminalTx(ctx, func(tx Store) error {
		existing, ferr := tx.FindTerminalResponse(ctx, env.ParentID)
		if ferr != nil {
			return fmt.Errorf("harness: find terminal response: %w", ferr)
		}
		if existing != nil {
			if existing.ID == env.ID {
				result = &Result{
					ID:            existing.ID,
					CorrelationID: existing.CorrelationID,
					Kind:          existing.Kind,
					Dedupe:        true,
				}
				return nil
			}
			return &RejectError{
				Reason:           v4types.HarnessTerminalDuplicate,
				Detail:           fmt.Sprintf("parent_id %q already has terminal response %q", env.ParentID, existing.ID),
				DedupeResponseID: existing.ID,
			}
		}
		if ierr := tx.InsertMessage(ctx, env, tsReceived); ierr != nil {
			if errors.Is(ierr, ErrUniqueViolation) {
				// Concurrent winner snuck in between our SELECT and INSERT.
				// Re-read and decide same/different id.
				winner, gerr := tx.FindTerminalResponse(ctx, env.ParentID)
				if gerr != nil {
					return fmt.Errorf("harness: resolve race: %w", gerr)
				}
				if winner == nil {
					// Should be impossible — the UNIQUE conflict points
					// at a row that must exist. Treat as transient.
					return fmt.Errorf("harness: race resolution found no winner")
				}
				if winner.ID == env.ID {
					result = &Result{
						ID:            winner.ID,
						CorrelationID: winner.CorrelationID,
						Kind:          winner.Kind,
						Dedupe:        true,
					}
					return nil
				}
				return &RejectError{
					Reason:           v4types.HarnessTerminalDuplicate,
					Detail:           fmt.Sprintf("parent_id %q already has terminal response %q (concurrent winner)", env.ParentID, winner.ID),
					DedupeResponseID: winner.ID,
				}
			}
			return fmt.Errorf("harness: insert terminal response: %w", ierr)
		}
		result = &Result{
			ID:            env.ID,
			CorrelationID: env.CorrelationID,
			Kind:          env.Kind,
		}
		return nil
	})
	if terr != nil {
		return nil, terr
	}
	return result, nil
}

// commitNonResponse handles the L1 §10.2.1 Step 8 else branch (kind=event
// or kind=request, or non-terminal kind=response). A plain INSERT is
// attempted; on UNIQUE violation we re-read by id and apply the Step 0.5
// canonical_hash logic (race lost / true id reuse).
func commitNonResponse(
	ctx context.Context,
	deps Deps,
	env *v4types.Envelope,
	tsReceived int64,
) (*Result, error) {
	if err := deps.Store.InsertMessage(ctx, env, tsReceived); err != nil {
		if errors.Is(err, ErrUniqueViolation) {
			existing, gerr := deps.Store.FindByID(ctx, env.ID)
			if gerr != nil {
				return nil, fmt.Errorf("harness: resolve unique race: %w", gerr)
			}
			if existing == nil {
				return nil, fmt.Errorf("harness: unique race found no row for id %q", env.ID)
			}
			return decideDedupe(env, existing)
		}
		return nil, fmt.Errorf("harness: insert message: %w", err)
	}
	return &Result{
		ID:            env.ID,
		CorrelationID: env.CorrelationID,
		Kind:          env.Kind,
	}, nil
}

// computeIsTerminal evaluates the L1 §10.2.1 terminal predicate:
//
//   - Core type → always terminal (single-response convention).
//   - Business type:
//   - `single-response` → always terminal
//   - `payload_status` (default) → payload.status ∈ {completed, failed}
func computeIsTerminal(env *v4types.Envelope, typeInfo *TypeInfo) bool {
	if typeInfo == nil {
		// Core type response.
		return coreTerminalAlways(env.Type)
	}
	switch typeInfo.TerminalConvention {
	case "single-response":
		return true
	case "", "payload_status":
		var probe struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(env.Payload, &probe); err != nil {
			return false
		}
		return probe.Status == "completed" || probe.Status == "failed"
	default:
		return false
	}
}

// defaultClock returns the current wall-clock time in milliseconds. Kept
// here (not in deps.go) so tests can override via Deps.Clock without
// re-exporting the helper.
func defaultClock() int64 {
	return nowMillis()
}
