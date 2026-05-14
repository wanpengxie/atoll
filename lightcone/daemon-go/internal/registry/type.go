package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// ---------------------------------------------------------------------------
// TypeRow + Install API (L2 §1.4.2)
// ---------------------------------------------------------------------------

// AllowedKind enumerates the closed set of envelope kinds permitted in
// `type_registry.allowed_kinds` (L0 §2.2 ADT). The slice in a TypeRow
// holds string elements; this type only exists for the validator
// membership check.
type AllowedKind string

const (
	KindEvent    AllowedKind = "event"
	KindRequest  AllowedKind = "request"
	KindResponse AllowedKind = "response"
)

// HandlerBinding mirrors the CHECK constraint on
// `type_registry.handler_binding` (and matches `ActorBinding` for
// agent/tool actors).
const (
	HandlerBindingDaemonRPC   = "daemon_rpc"
	HandlerBindingInWorkerBus = "in_worker_bus"
)

// TerminalConvention mirrors the CHECK constraint on
// `type_registry.terminal_convention` (L2 §1.4.1 + §1.4.2). The empty
// string is treated as the default ('payload_status') for caller
// convenience; Install fills it in before INSERT.
const (
	TerminalConventionPayloadStatus  = "payload_status"
	TerminalConventionSingleResponse = "single-response"
)

// validAllowedKinds is the kind whitelist enforced by Install. Anything
// outside this set fails as `type_registry_invalid`.
var validAllowedKinds = map[string]struct{}{
	string(KindEvent):    {},
	string(KindRequest):  {},
	string(KindResponse): {},
}

// validHandlerBindings is the binding whitelist (matches the
// type_registry CHECK and actor_registry CHECK enums).
var validHandlerBindings = map[string]struct{}{
	HandlerBindingDaemonRPC:   {},
	HandlerBindingInWorkerBus: {},
}

// validTerminalConventions covers the type_registry.terminal_convention
// CHECK enum plus the empty string (treated as the default by Install).
var validTerminalConventions = map[string]struct{}{
	"":                               {},
	TerminalConventionPayloadStatus:  {},
	TerminalConventionSingleResponse: {},
}

// fallbackResponseSamples are the platform fallback payloads L2 §1.4.2
// mandates every request-allowed type's response schema accept. Listed
// in spec order so install-time error messages stay stable.
//
// FIX-6 §3 / codex t89: long-pending scheduler Step 2 actually emits
// `human_unanswered_timeout` (v4types.TerminalHumanUnansweredTimeout)
// — a type whose response schema enumerates only the three older
// reasons would install OK but reject the Step 2 fallback at harness
// step 6 runtime. We include it here so install fails fast.
var fallbackResponseSamples = []map[string]any{
	{"status": "failed", "reason": string(v4types.TerminalUnansweredTimeout)},
	{"status": "failed", "reason": string(v4types.TerminalAdapterDefaultTimeout)},
	{"status": "failed", "reason": string(v4types.TerminalReceiverUnavailable)},
	{"status": "failed", "reason": string(v4types.TerminalHumanUnansweredTimeout)},
}

// TypeRow is the canonical in-memory shape of one type_registry row
// (L2 §1.4.2). Caller-friendly types: AllowedKinds as []string for
// JSON wire compat with bootstrap.TypeRegistryRow; SchemasByKind as
// raw JSON because Install hands it to the schema validator.
//
// Field semantics:
//   - Type:               PRIMARY KEY; must be non-empty.
//   - AllowedKinds:       non-empty subset of {event, request, response}.
//   - SchemasByKind:      JSON object whose top-level keys ⊆ AllowedKinds;
//                         each value is a JSON Schema Draft 2020-12 doc.
//   - HandlerBinding:     'daemon_rpc' | 'in_worker_bus'.
//   - TerminalConvention: '' (defaults to 'payload_status') / 'payload_status' /
//                         'single-response'.
//   - MaxPendingMs:       required when AllowedKinds includes 'request' AND
//                         the resolved HandlerActorID is a tool actor. nil for
//                         non-adapter rows.
//   - HandlerActorID:     optional; when set, must point at an active
//                         actor_registry row whose ActorBinding matches
//                         HandlerBinding.
//   - Domain:             informational (e.g. "xhs").
type TypeRow struct {
	Type               string
	AllowedKinds       []string
	SchemasByKind      json.RawMessage
	HandlerBinding     string
	TerminalConvention string
	MaxPendingMs       *int64
	HandlerActorID     string
	Domain             string
}

// InstallError is the structured rejection returned by Install when a
// row fails any of the L2 §1.4.2 install-time checks. Reason is one of
// the five `v4types.Install*` constants (L1 §10.3.2); Detail is a
// human-readable diagnostic. Type identifies the offending TypeRow.
//
// Callers map (Reason, Detail) to L2 §3.6.1 HTTP status / install-time
// table; the saga / HTTP layer never invents new reasons.
type InstallError struct {
	Reason v4types.InstallReason
	Type   string
	Detail string
}

// Error formats as "install reject <reason>: <type>: <detail>" — stable
// enough that grep + log scraping work, while matching the §3.6.1
// "{reason, detail}" body shape.
func (e *InstallError) Error() string {
	if e.Type == "" {
		return fmt.Sprintf("install reject %s: %s", e.Reason, e.Detail)
	}
	return fmt.Sprintf("install reject %s: %s: %s", e.Reason, e.Type, e.Detail)
}

// Is supports `errors.Is(err, ErrInstallTypeRegistryInvalid)` style
// checks. Each sentinel below is itself an *InstallError with Type=""
// (caller compares Reason only).
func (e *InstallError) Is(target error) bool {
	var t *InstallError
	if !errors.As(target, &t) {
		return false
	}
	return t.Reason == e.Reason
}

// Sentinels — one per closed-set InstallReason in L1 §10.3.2 that this
// package raises. Callers use them with `errors.Is` to branch on the
// reason class without parsing the message string.
var (
	ErrInstallTypeRegistryInvalid           = &InstallError{Reason: v4types.InstallTypeRegistryInvalid}
	ErrInstallAdapterTimeoutMissing         = &InstallError{Reason: v4types.InstallAdapterTimeoutMissing}
	ErrInstallHandlerActorNotRegistered     = &InstallError{Reason: v4types.InstallHandlerActorNotRegistered}
	ErrInstallHandlerActorBindingMismatch   = &InstallError{Reason: v4types.InstallHandlerActorBindingMismatch}
	ErrInstallFallbackResponseSchemaInvalid = &InstallError{Reason: v4types.InstallFallbackResponseSchemaInvalid}
)

// Install runs the L2 §1.4.2 install validator over `rows` and, on
// success, INSERTs every row into the channel-local `type_registry`
// table via `exec`. Caller owns the transaction (typically the channel
// bootstrap saga's BEGIN IMMEDIATE conn, L2 §1.4.7) — Install never
// commits or rolls back on its own.
//
// Validation order per row (fail-fast):
//
//  1. Structural (`type_registry_invalid`):
//     - Type non-empty.
//     - AllowedKinds non-empty subset of {event, request, response};
//       no duplicates.
//     - HandlerBinding ∈ {daemon_rpc, in_worker_bus}.
//     - TerminalConvention ∈ {'', 'payload_status', 'single-response'}.
//     - SchemasByKind is a valid JSON object whose top-level keys ⊆
//       AllowedKinds; every value compiles as a JSON Schema 2020-12 doc.
//  2. `adapter_timeout_missing`: if `request` ∈ AllowedKinds and the
//     resolved HandlerActorID is a tool actor, MaxPendingMs MUST be
//     non-nil and > 0.
//  3. `handler_actor_not_registered`: if HandlerActorID is non-empty,
//     `actor_registry` must hold an active row for it.
//  4. `handler_actor_binding_mismatch`: that actor's `actor_binding`
//     MUST equal `row.HandlerBinding`.
//  5. `fallback_response_schema_invalid`: if `request` ∈ AllowedKinds,
//     `schemas_by_kind.response` MUST accept every payload in
//     `fallbackResponseSamples`.
//
// If validation passes for every row, Install INSERTs them in slice
// order using `created_at = now`. Any INSERT error is returned verbatim
// (NOT wrapped in InstallError) — caller's tx rollback handles cleanup
// (so e.g. an already-seeded actor_registry row goes away with the same
// rollback, satisfying acceptance "install 失败时 actor_registry 不被污染").
//
// IMPORTANT: rows must not be empty (returns nil with no work done).
// Pass exec = the *sql.Conn that owns the surrounding BEGIN IMMEDIATE.
func Install(ctx context.Context, exec Executor, rows []TypeRow, now int64) error {
	if exec == nil {
		return errors.New("registry: Install exec is nil")
	}
	if now <= 0 {
		return errors.New("registry: Install now must be positive")
	}
	if len(rows) == 0 {
		return nil
	}

	// Phase A: validate every row before any INSERT. Compile the
	// per-kind schemas once and cache them so Phase B's fallback check
	// and any future schema-driven step (harness step 6) can re-use.
	compiled := make([]map[string]*jsonschema.Schema, len(rows))
	for i := range rows {
		row := &rows[i]
		schemas, ierr := validateRowStructural(row)
		if ierr != nil {
			return ierr
		}
		compiled[i] = schemas

		if ierr := validateHandlerActor(ctx, exec, row); ierr != nil {
			return ierr
		}
		if ierr := validateFallbackBranch(row, schemas); ierr != nil {
			return ierr
		}
	}

	// Phase B: insert. Caller's tx rolls back on the first INSERT
	// failure; we don't try to convert errors here.
	for i := range rows {
		if err := insertTypeRow(ctx, exec, &rows[i], now); err != nil {
			return fmt.Errorf("registry: insert type_registry %q: %w", rows[i].Type, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Structural validators
// ---------------------------------------------------------------------------

// validateRowStructural runs every check in the
// `type_registry_invalid` bucket. On success it returns the compiled
// per-kind schemas (so validateFallbackBranch can re-use them without
// recompilation).
func validateRowStructural(row *TypeRow) (map[string]*jsonschema.Schema, *InstallError) {
	if strings.TrimSpace(row.Type) == "" {
		return nil, newInvalid("", "type must be non-empty")
	}
	if len(row.AllowedKinds) == 0 {
		return nil, newInvalid(row.Type, "allowed_kinds must be a non-empty array")
	}
	seenKinds := make(map[string]struct{}, len(row.AllowedKinds))
	for _, k := range row.AllowedKinds {
		if _, ok := validAllowedKinds[k]; !ok {
			return nil, newInvalid(row.Type, fmt.Sprintf("allowed_kinds contains invalid kind %q (must be subset of {event,request,response})", k))
		}
		if _, dup := seenKinds[k]; dup {
			return nil, newInvalid(row.Type, fmt.Sprintf("allowed_kinds has duplicate kind %q", k))
		}
		seenKinds[k] = struct{}{}
	}
	if _, ok := validHandlerBindings[row.HandlerBinding]; !ok {
		return nil, newInvalid(row.Type, fmt.Sprintf("handler_binding %q is invalid (must be daemon_rpc|in_worker_bus)", row.HandlerBinding))
	}
	if _, ok := validTerminalConventions[row.TerminalConvention]; !ok {
		return nil, newInvalid(row.Type, fmt.Sprintf("terminal_convention %q is invalid (must be payload_status|single-response)", row.TerminalConvention))
	}

	if len(row.SchemasByKind) == 0 {
		return nil, newInvalid(row.Type, "schemas_by_kind must be a non-empty JSON object")
	}
	var schemaMap map[string]json.RawMessage
	if err := json.Unmarshal(row.SchemasByKind, &schemaMap); err != nil {
		return nil, newInvalid(row.Type, fmt.Sprintf("schemas_by_kind is not a valid JSON object: %v", err))
	}
	if len(schemaMap) == 0 {
		return nil, newInvalid(row.Type, "schemas_by_kind must contain at least one kind entry")
	}

	// Compile every per-kind schema; reject keys outside allowed_kinds.
	// Sort keys before compile so error messages and failure ordering
	// are deterministic across runs.
	keys := make([]string, 0, len(schemaMap))
	for k := range schemaMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	compiled := make(map[string]*jsonschema.Schema, len(schemaMap))
	for _, k := range keys {
		if _, ok := seenKinds[k]; !ok {
			return nil, newInvalid(row.Type, fmt.Sprintf("schemas_by_kind key %q is not in allowed_kinds", k))
		}
		schema, err := compileSchema(row.Type, k, schemaMap[k])
		if err != nil {
			return nil, newInvalid(row.Type, fmt.Sprintf("schemas_by_kind.%s does not compile as JSON Schema 2020-12: %v", k, err))
		}
		compiled[k] = schema
	}

	return compiled, nil
}

// compileSchema feeds one schema doc to the jsonschema/v5 compiler.
// The URL is synthesized to make error messages traceable
// ("type://<type>/<kind>"). Draft 2020-12 is the compiler's default
// when `$schema` is absent.
func compileSchema(typeName, kind string, raw json.RawMessage) (*jsonschema.Schema, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty schema")
	}
	// Ensure the bytes parse as JSON before handing to the compiler —
	// otherwise the compiler returns a less precise error.
	var probe any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	url := fmt.Sprintf("type://%s/%s", typeName, kind)
	c := jsonschema.NewCompiler()
	if err := c.AddResource(url, strings.NewReader(string(raw))); err != nil {
		return nil, fmt.Errorf("add resource: %w", err)
	}
	return c.Compile(url)
}

// ---------------------------------------------------------------------------
// Handler-actor cross-checks
// ---------------------------------------------------------------------------

// validateHandlerActor implements the two install-time invariants
// L2 §1.4.2 layers on top of the actor_registry FK relationship:
//
//   - `handler_actor_not_registered`: HandlerActorID set but no active
//     actor row.
//   - `handler_actor_binding_mismatch`: row.HandlerBinding ≠
//     actor.ActorBinding.
//
// It also captures the `adapter_timeout_missing` rule (L2 §1.4.2 +
// §8.6): if the row is a tool-receiver request type, MaxPendingMs must
// be set. Tool kind detection requires the same actor lookup, so we
// fold the check in here.
//
// Order: we run the timeout check FIRST when the handler resolves to a
// tool actor — the spec lists `adapter_timeout_missing` ahead of the
// fallback / binding mismatch checks, and the test suite asserts the
// `adapter_timeout_missing` reason fires before any binding-mismatch
// fallback. For non-tool actors (agent receivers, etc.) the timeout
// rule does not apply and we skip straight to the binding mismatch.
func validateHandlerActor(ctx context.Context, exec Executor, row *TypeRow) *InstallError {
	if row.HandlerActorID == "" {
		// No handler bound — adapter_timeout_missing does not apply
		// (per L2 §1.4.2 the rule only fires when handler resolves to
		// a tool actor). Skip both lookups.
		return nil
	}
	actor, err := Get(ctx, exec, row.HandlerActorID)
	if err != nil || actor == nil {
		// Treat any lookup error (missing row, sql error) as
		// `handler_actor_not_registered` — caller's tx will roll back
		// any partial state.
		return &InstallError{
			Reason: v4types.InstallHandlerActorNotRegistered,
			Type:   row.Type,
			Detail: fmt.Sprintf("handler_actor_id %q is not registered", row.HandlerActorID),
		}
	}
	if actor.DeregisteredAt != nil {
		return &InstallError{
			Reason: v4types.InstallHandlerActorNotRegistered,
			Type:   row.Type,
			Detail: fmt.Sprintf("handler_actor_id %q is deregistered", row.HandlerActorID),
		}
	}

	// adapter_timeout_missing: tool receiver + request in allowed_kinds
	// + no max_pending_ms.
	if actor.Kind == KindTool && containsString(row.AllowedKinds, string(KindRequest)) {
		if row.MaxPendingMs == nil || *row.MaxPendingMs <= 0 {
			return &InstallError{
				Reason: v4types.InstallAdapterTimeoutMissing,
				Type:   row.Type,
				Detail: fmt.Sprintf("tool handler_actor_id %q requires max_pending_ms (>0) when allowed_kinds includes 'request'", row.HandlerActorID),
			}
		}
	}

	// binding mismatch.
	if string(actor.Binding) != row.HandlerBinding {
		return &InstallError{
			Reason: v4types.InstallHandlerActorBindingMismatch,
			Type:   row.Type,
			Detail: fmt.Sprintf("handler_actor_id %q binding %q ≠ row handler_binding %q", row.HandlerActorID, actor.Binding, row.HandlerBinding),
		}
	}
	return nil
}

// validateFallbackBranch enforces L2 §1.4.2: any request-allowed type
// MUST accept the three platform fallback samples on its response
// schema. The check is required so §3.7 fallback emits never trip
// harness step 6 schema validation later.
//
// Implementation note: the spec lists the samples in a specific order
// (unanswered_timeout / adapter_default_timeout / receiver_unavailable).
// We iterate them in that order and return the first rejecting sample
// in the detail string — easier for callers to map back to the spec
// when debugging.
func validateFallbackBranch(row *TypeRow, schemas map[string]*jsonschema.Schema) *InstallError {
	if !containsString(row.AllowedKinds, string(KindRequest)) {
		return nil
	}
	responseSchema, ok := schemas[string(KindResponse)]
	if !ok {
		return &InstallError{
			Reason: v4types.InstallFallbackResponseSchemaInvalid,
			Type:   row.Type,
			Detail: "schemas_by_kind.response is required when allowed_kinds includes 'request'",
		}
	}
	for _, sample := range fallbackResponseSamples {
		if err := responseSchema.Validate(sample); err != nil {
			return &InstallError{
				Reason: v4types.InstallFallbackResponseSchemaInvalid,
				Type:   row.Type,
				Detail: fmt.Sprintf("response schema rejects fallback sample %v: %v", sample, err),
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------

// insertTypeRow writes one type_registry row. The terminal_convention
// column defaults to 'payload_status' (the L2 §1.4.1 / §1.4.2 contract)
// when the caller leaves the field empty.
//
// NOTE: we keep this private + duplicate the bootstrap saga's
// `insertType` shape on purpose. M1.3 saga (T87) still owns its own
// INSERT path; consolidating both call sites is out of scope for T5
// per ticket dependency boundary.
func insertTypeRow(ctx context.Context, exec Executor, row *TypeRow, now int64) error {
	terminal := row.TerminalConvention
	if terminal == "" {
		terminal = TerminalConventionPayloadStatus
	}
	allowed, err := json.Marshal(row.AllowedKinds)
	if err != nil {
		return fmt.Errorf("marshal allowed_kinds: %w", err)
	}
	var maxPending any
	if row.MaxPendingMs != nil {
		maxPending = *row.MaxPendingMs
	}
	var handlerActor any
	if row.HandlerActorID != "" {
		handlerActor = row.HandlerActorID
	}
	var domain any
	if row.Domain != "" {
		domain = row.Domain
	}
	_, err = exec.ExecContext(ctx,
		`INSERT INTO type_registry
		   (type, allowed_kinds, schemas_by_kind, handler_binding,
		    terminal_convention, max_pending_ms, handler_actor_id, domain, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.Type, string(allowed), string(row.SchemasByKind), row.HandlerBinding,
		terminal, maxPending, handlerActor, domain, now,
	)
	return err
}

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

// newInvalid is the helper that builds the `type_registry_invalid`
// flavor of InstallError. Keeps call sites short and the reason
// hard-coded.
func newInvalid(typeName, detail string) *InstallError {
	return &InstallError{
		Reason: v4types.InstallTypeRegistryInvalid,
		Type:   typeName,
		Detail: detail,
	}
}

func containsString(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}
