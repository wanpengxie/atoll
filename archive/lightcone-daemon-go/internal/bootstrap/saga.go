package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
)

// channelDBFilename is the well-known basename of the channel-local
// sqlite file inside the workdir (L2 §1.2).
const channelDBFilename = "messages.sqlite"

// compensateMarkerFilename is the file the saga writes inside a workdir
// immediately after creating it. compensate() refuses to RemoveAll a
// workdir that does not carry this marker — closes the codex t87
// critical (without the marker, a workdir reused from a previous channel
// or a path leaked by a misconfigured server could be wiped by a failed
// bootstrap retry).
const compensateMarkerFilename = ".coagent-bootstrap"

// New constructs a Saga bound to the daemon-level sqlite pool. The
// daemon caller is expected to keep `daemonDB` open for the lifetime
// of the process (it backs bootstrap_registry CAS + reconcile).
//
// channelRoot (configured via WithChannelRoot) is required for any
// ChannelCreate invocation — the saga derives each channel workdir as
// filepath.Join(channelRoot, channel_id) and validates the derived path
// stays inside channelRoot. New itself does NOT panic on an empty
// channelRoot so that test fixtures can construct a Saga before knowing
// their TempDir; ChannelCreate raises ErrParamsInvalid instead.
func New(daemonDB *sql.DB, opts ...Option) *Saga {
	s := &Saga{
		daemonDB:   daemonDB,
		now:        nowUnix,
		openCh:     defaultOpenChannel,
		mkdir:      os.MkdirAll,
		rmAll:      os.RemoveAll,
		stat:       os.Stat,
		fileWriter: os.WriteFile,
	}
	for _, o := range opts {
		o(s)
	}
	if s.channelRoot != "" {
		s.channelRoot = filepath.Clean(s.channelRoot)
	}
	return s
}

// defaultOpenChannel is the production OpenChannel wrapper. We keep
// the indirection because tests inject a failing variant via
// WithOpenChannel.
func defaultOpenChannel(ctx context.Context, path string) (*sql.DB, error) {
	return store.OpenChannel(ctx, path, store.OpenOptions{})
}

// ChannelCreate drives the L2 §1.4.7 9-step bootstrap saga.
//
// Flow:
//
//  1. Validate params (minimal — full type validation belongs to T5).
//  2. Idempotency check on bootstrap_registry: completed → return
//     stored channel_id; in_progress → ErrBootstrapInProgress;
//     rolled_back → ErrBootstrapRolledBack.
//  3. Step 1: INSERT bootstrap_registry row (status=in_progress).
//  4. Step 2: mkdir workdir + open channel sqlite (DDL auto-applied).
//  5. Steps 3-8a: single channel-local IMMEDIATE transaction —
//     actor_registry seeds (system / humans / agent / tool adapters),
//     type_registry rows, channel_created event. Any failure → tx
//     ROLLBACK + os.RemoveAll(workdir) + UPDATE status='rolled_back'.
//  6. Step 8b: CAS UPDATE bootstrap_registry status='completed'. On
//     failure we leave the row in_progress for the next Reconcile
//     pass (the channel_created event is already durable so
//     INSERT OR IGNORE on retry is a no-op).
func (s *Saga) ChannelCreate(ctx context.Context, p CreateParams) (Result, error) {
	if err := s.validateParams(p); err != nil {
		return Result{}, err
	}

	// Derive the workdir from configured channelRoot + channel_id. The
	// containment check inside validateParams has already verified the
	// result stays under channelRoot, so this Join is purely a join.
	workdirPath := s.deriveWorkdir(p.ChannelID)

	// ---- Idempotency check ------------------------------------------------
	existing, err := s.lookupExisting(ctx, p.CreateRequestID)
	if err != nil {
		return Result{}, err
	}
	if existing != nil {
		switch existing.Status {
		case StatusCompleted:
			return Result{ChannelID: existing.ChannelID, Status: StatusCompleted}, nil
		case StatusInProgress:
			return Result{ChannelID: existing.ChannelID, Status: StatusInProgress}, ErrBootstrapInProgress
		case StatusRolledBack:
			return Result{ChannelID: existing.ChannelID, Status: StatusRolledBack}, ErrBootstrapRolledBack
		default:
			return Result{}, fmt.Errorf("bootstrap: unknown stored status %q", existing.Status)
		}
	}

	// ---- Step 1: INSERT bootstrap_registry (status=in_progress) -----------
	startedAt := s.now()
	if err := s.injected(fpStep1Insert); err != nil {
		return Result{}, fmt.Errorf("bootstrap: step1 insert: %w", err)
	}
	if _, err := s.daemonDB.ExecContext(ctx,
		`INSERT INTO bootstrap_registry
		 (create_request_id, channel_id, status, workdir_path, started_at)
		 VALUES (?, ?, ?, ?, ?)`,
		p.CreateRequestID, p.ChannelID, StatusInProgress, workdirPath, startedAt,
	); err != nil {
		return Result{}, fmt.Errorf("bootstrap: step1 insert: %w", err)
	}

	// From here on, any error must run the rollback compensation
	// (delete workdir + UPDATE status='rolled_back'). Defer captures
	// the named return so we can run the cleanup on the way out.
	channelDBPath := filepath.Join(workdirPath, channelDBFilename)
	rollback := func(reason error) {
		s.compensate(ctx, p.CreateRequestID, workdirPath, reason)
	}

	// ---- Step 2: mkdir + open channel sqlite ------------------------------
	if err := s.injected(fpStep2Mkdir); err != nil {
		rollback(err)
		return Result{}, fmt.Errorf("bootstrap: step2 mkdir: %w", err)
	}
	if err := s.mkdir(workdirPath, 0o755); err != nil {
		rollback(err)
		return Result{}, fmt.Errorf("bootstrap: step2 mkdir: %w", err)
	}
	// Write the compensate marker. Failure to write the marker is not
	// fatal to the bootstrap (the channel sqlite is still useful), but
	// compensate() will refuse to RemoveAll the workdir later. Log via
	// the returned error so reconcile retries can re-attempt mkdir.
	if err := s.fileWriter(filepath.Join(workdirPath, compensateMarkerFilename),
		[]byte("coagent-bootstrap\n"), 0o644); err != nil {
		rollback(err)
		return Result{}, fmt.Errorf("bootstrap: step2 marker: %w", err)
	}

	if err := s.injected(fpStep2OpenCh); err != nil {
		rollback(err)
		return Result{}, fmt.Errorf("bootstrap: step2 open channel: %w", err)
	}
	channelDB, err := s.openCh(ctx, channelDBPath)
	if err != nil {
		rollback(err)
		return Result{}, fmt.Errorf("bootstrap: step2 open channel: %w", err)
	}
	defer func() { _ = channelDB.Close() }()

	// ---- Steps 3-8a: single IMMEDIATE transaction on channel sqlite -------
	txErr := store.WithImmediate(ctx, channelDB, func(ctx context.Context, conn *sql.Conn) error {
		return s.seedChannel(ctx, conn, p)
	})
	if txErr != nil {
		rollback(txErr)
		return Result{}, fmt.Errorf("bootstrap: channel seed tx: %w", txErr)
	}

	// ---- Step 8b: CAS UPDATE bootstrap_registry status=completed ---------
	if err := s.injected(fpStep8bComplete); err != nil {
		// 8a is durable in messages table; leave row in_progress for
		// reconcile. Do NOT run the rollback compensation — that would
		// delete an already-committed channel sqlite. Return the error
		// so the HTTP layer can surface it; the next daemon restart
		// (or explicit Reconcile call) finishes the saga.
		return Result{ChannelID: p.ChannelID, Status: StatusInProgress},
			fmt.Errorf("bootstrap: step8b complete: %w", err)
	}
	res, err := s.daemonDB.ExecContext(ctx,
		`UPDATE bootstrap_registry
		   SET status=?, completed_at=?
		 WHERE create_request_id=? AND status=?`,
		StatusCompleted, s.now(), p.CreateRequestID, StatusInProgress,
	)
	if err != nil {
		return Result{ChannelID: p.ChannelID, Status: StatusInProgress},
			fmt.Errorf("bootstrap: step8b complete: %w", err)
	}
	if affected, _ := res.RowsAffected(); affected != 1 {
		return Result{ChannelID: p.ChannelID, Status: StatusInProgress},
			fmt.Errorf("bootstrap: step8b CAS lost (affected=%d)", affected)
	}

	return Result{ChannelID: p.ChannelID, Status: StatusCompleted}, nil
}

// seedChannel runs steps 3-8a inside the channel-local IMMEDIATE tx.
// Order honors L2 §3.5 (actor before type registry) and L2 §1.4.7
// step-by-step ordering.
func (s *Saga) seedChannel(ctx context.Context, conn *sql.Conn, p CreateParams) error {
	now := s.now()

	// --- Step 3: system actor ------------------------------------------
	if err := s.injected(fpStep3System); err != nil {
		return fmt.Errorf("step3 system actor: %w", err)
	}
	if err := registerActor(ctx, conn, p.ChannelID, registry.ActorMeta{
		ActorID:   "system",
		Kind:      registry.KindSystem,
		Binding:   registry.BindingNone,
		CreatedAt: now,
	}); err != nil {
		return fmt.Errorf("step3 system actor: %w", err)
	}

	// --- Step 4: human members -----------------------------------------
	for i, m := range p.HumanMembers {
		if err := s.injected(fpStep4Human); err != nil {
			return fmt.Errorf("step4 human member[%d] %s: %w", i, m.ActorID, err)
		}
		if err := registerActor(ctx, conn, p.ChannelID, registry.ActorMeta{
			ActorID:   m.ActorID,
			Kind:      registry.KindHuman,
			Binding:   registry.BindingNone,
			CreatedAt: now,
		}); err != nil {
			return fmt.Errorf("step4 human member[%d] %s: %w", i, m.ActorID, err)
		}
	}

	// --- Step 5: channel agent -----------------------------------------
	agentID := p.ChannelAgent.ActorID
	if strings.TrimSpace(agentID) == "" {
		agentID = p.ChannelID + ":" + DefaultChannelAgentName
	}
	if err := s.injected(fpStep5Agent); err != nil {
		return fmt.Errorf("step5 channel agent %s: %w", agentID, err)
	}
	if err := registerActor(ctx, conn, p.ChannelID, registry.ActorMeta{
		ActorID:   agentID,
		Kind:      registry.KindAgent,
		Binding:   registry.BindingDaemonRPC,
		CreatedAt: now,
	}); err != nil {
		return fmt.Errorf("step5 channel agent %s: %w", agentID, err)
	}

	// --- Step 6: tool adapters (actor_registry → type_registry) --------
	for i, ad := range p.ToolAdapters {
		if err := s.injected(fpStep6Adapter); err != nil {
			return fmt.Errorf("step6 adapter[%d] %s: %w", i, ad.ActorID, err)
		}
		if err := registerActor(ctx, conn, p.ChannelID, registry.ActorMeta{
			ActorID:   ad.ActorID,
			Kind:      registry.KindTool,
			Binding:   registry.ActorBinding(ad.Binding),
			CreatedAt: now,
		}); err != nil {
			return fmt.Errorf("step6 adapter[%d] %s actor: %w", i, ad.ActorID, err)
		}
		for j, row := range ad.TypeRows {
			if err := insertType(ctx, conn, row, now); err != nil {
				return fmt.Errorf("step6 adapter[%d] %s type[%d] %s: %w",
					i, ad.ActorID, j, row.Type, err)
			}
		}
	}

	// --- Step 7: business type rows ------------------------------------
	for i, row := range p.BusinessTypes {
		if err := s.injected(fpStep7Type); err != nil {
			return fmt.Errorf("step7 business type[%d] %s: %w", i, row.Type, err)
		}
		// Minimal handler_actor_id integrity check — full validation
		// is T5's job. Saga only ensures the FK-like invariant from
		// L2 §1.4.2: handler_actor_id (if set) must resolve to an
		// active row in actor_registry within the same tx.
		if row.HandlerActorID != "" {
			if _, err := registry.GetKind(ctx, conn, row.HandlerActorID); err != nil {
				if errors.Is(err, registry.ErrActorNotFound) {
					return fmt.Errorf("step7 business type[%d] %s: handler_actor_id %q not registered",
						i, row.Type, row.HandlerActorID)
				}
				return fmt.Errorf("step7 business type[%d] %s lookup: %w", i, row.Type, err)
			}
		}
		if err := insertType(ctx, conn, row, now); err != nil {
			return fmt.Errorf("step7 business type[%d] %s: %w", i, row.Type, err)
		}
	}

	// --- Step 8a: emit channel_created event ---------------------------
	if err := s.injected(fpStep8aEmit); err != nil {
		return fmt.Errorf("step8a emit: %w", err)
	}
	if err := emitChannelCreated(ctx, conn, p, now); err != nil {
		return fmt.Errorf("step8a emit: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Idempotency lookup + rollback compensation
// ---------------------------------------------------------------------------

type registryRow struct {
	ChannelID   string
	Status      string
	WorkdirPath string
}

func (s *Saga) lookupExisting(ctx context.Context, createRequestID string) (*registryRow, error) {
	row := s.daemonDB.QueryRowContext(ctx,
		`SELECT channel_id, status, workdir_path
		   FROM bootstrap_registry
		  WHERE create_request_id = ?`,
		createRequestID,
	)
	var r registryRow
	err := row.Scan(&r.ChannelID, &r.Status, &r.WorkdirPath)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("bootstrap: lookup existing: %w", err)
	}
	return &r, nil
}

// compensate runs the rollback path for a failed bootstrap attempt:
//  1. os.RemoveAll(workdir) — only when the path carries the saga's
//     compensate marker file (.coagent-bootstrap). Closes codex t87
//     critical: without this guard, a CreateParams.WorkdirPath could
//     coerce the daemon into removing any directory it has write access
//     to (incl. caller-controlled paths on a reconcile pass).
//  2. UPDATE bootstrap_registry SET status='rolled_back',
//     rollback_reason=<err> WHERE create_request_id=? AND status='in_progress'.
//
// The CAS protects against concurrent reconcile races and double-rollback.
func (s *Saga) compensate(ctx context.Context, createRequestID, workdirPath string, cause error) {
	reason := "unknown"
	if cause != nil {
		reason = cause.Error()
		if len(reason) > 500 {
			reason = reason[:500]
		}
	}

	// Workdir cleanup is gated on the compensate marker. The marker is
	// written by Step 2 immediately after mkdir, so any directory the
	// saga itself created will carry it; a directory we did not create
	// (mkdir failed, or compensate called with a stale registry row
	// after the workdir was already removed) will not.
	//
	// We additionally double-check containment: the resolved workdir
	// must still sit inside the configured channelRoot. Defense-in-depth
	// for the case where a future bug feeds compensate() a path it
	// shouldn't touch.
	if s.shouldRemoveWorkdir(workdirPath) {
		_ = s.rmAll(workdirPath)
	}

	_, _ = s.daemonDB.ExecContext(ctx,
		`UPDATE bootstrap_registry
		   SET status=?, rollback_reason=?
		 WHERE create_request_id=? AND status=?`,
		StatusRolledBack, reason, createRequestID, StatusInProgress,
	)
}

// shouldRemoveWorkdir is the compensate gate. Returns true iff:
//   - channelRoot is configured and the path is contained inside it,
//   - the path exists, and
//   - the compensate marker file is present.
//
// Any failure (channelRoot empty, path not contained, marker missing)
// yields false so compensate falls back to "leave it for ops" rather
// than risking a destructive rm on a path the saga did not create.
func (s *Saga) shouldRemoveWorkdir(workdirPath string) bool {
	if workdirPath == "" {
		return false
	}
	if s.channelRoot == "" {
		return false
	}
	cleaned := filepath.Clean(workdirPath)
	rel, err := filepath.Rel(s.channelRoot, cleaned)
	if err != nil || rel == "." || rel == "" || strings.HasPrefix(rel, "..") ||
		strings.HasPrefix(rel, string(filepath.Separator)) {
		return false
	}
	// Path must exist and carry the marker. statExists() also returns
	// false for permission errors, so a directory we cannot read is
	// treated as "do not touch".
	if !s.statExists(filepath.Join(cleaned, compensateMarkerFilename)) {
		return false
	}
	return true
}

// deriveWorkdir builds the workdir path the saga uses for the given
// channel. Always returns filepath.Clean'd so callers (INSERT,
// compensate, reconcile) compare the same canonical string.
func (s *Saga) deriveWorkdir(channelID string) string {
	return filepath.Clean(filepath.Join(s.channelRoot, channelID))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// validateParams enforces the minimal pre-conditions before the saga
// touches the daemon sqlite. Method receiver so we can read channelRoot.
func (s *Saga) validateParams(p CreateParams) error {
	if strings.TrimSpace(p.CreateRequestID) == "" {
		return fmt.Errorf("%w: create_request_id is required", ErrParamsInvalid)
	}
	channelID := strings.TrimSpace(p.ChannelID)
	if channelID == "" {
		return fmt.Errorf("%w: channel_id is required", ErrParamsInvalid)
	}
	// channel_id is the only segment we join onto channelRoot. Reject
	// any value that could escape the root via path syntax: traversal
	// (".."), absolute paths, or embedded separators. This is the
	// pre-mkdir half of the T102 FIX-2 containment check; the
	// shouldRemoveWorkdir guard catches anything that slips past.
	if strings.ContainsAny(channelID, `/\`) ||
		channelID == ".." || strings.Contains(channelID, "..") ||
		filepath.IsAbs(channelID) {
		return fmt.Errorf("%w: channel_id %q must not contain path separators or '..'",
			ErrParamsInvalid, p.ChannelID)
	}
	if s.channelRoot == "" {
		return fmt.Errorf("%w: saga channel_root not configured (call WithChannelRoot)",
			ErrParamsInvalid)
	}
	if !filepath.IsAbs(s.channelRoot) {
		return fmt.Errorf("%w: saga channel_root %q must be absolute",
			ErrParamsInvalid, s.channelRoot)
	}
	// Post-derive containment: filepath.Rel returns ".." when the
	// derived path escapes the root (e.g. via symlink-resolved channelRoot
	// disagreement). We compute this defensively even though the
	// channel_id check above already covers the common cases.
	derived := s.deriveWorkdir(channelID)
	rel, err := filepath.Rel(s.channelRoot, derived)
	if err != nil || rel == "." || rel == "" || strings.HasPrefix(rel, "..") ||
		strings.HasPrefix(rel, string(filepath.Separator)) {
		return fmt.Errorf("%w: derived workdir %q escapes channel_root %q",
			ErrParamsInvalid, derived, s.channelRoot)
	}
	for i, ad := range p.ToolAdapters {
		if strings.TrimSpace(ad.ActorID) == "" {
			return fmt.Errorf("%w: tool_adapters[%d].actor_id is required",
				ErrParamsInvalid, i)
		}
		if ad.Binding != "daemon_rpc" && ad.Binding != "in_worker_bus" {
			return fmt.Errorf("%w: tool_adapters[%d].binding must be daemon_rpc|in_worker_bus, got %q",
				ErrParamsInvalid, i, ad.Binding)
		}
		for j, row := range ad.TypeRows {
			if err := validateTypeRow(row); err != nil {
				return fmt.Errorf("%w: tool_adapters[%d].type_rows[%d]: %s",
					ErrParamsInvalid, i, j, err.Error())
			}
		}
	}
	for i, row := range p.BusinessTypes {
		if err := validateTypeRow(row); err != nil {
			return fmt.Errorf("%w: business_types[%d]: %s",
				ErrParamsInvalid, i, err.Error())
		}
	}
	return nil
}

func validateTypeRow(row TypeRegistryRow) error {
	if strings.TrimSpace(row.Type) == "" {
		return errors.New("type is required")
	}
	if len(row.AllowedKinds) == 0 {
		return errors.New("allowed_kinds is required")
	}
	if row.HandlerBinding != "daemon_rpc" && row.HandlerBinding != "in_worker_bus" {
		return fmt.Errorf("handler_binding must be daemon_rpc|in_worker_bus, got %q", row.HandlerBinding)
	}
	if len(row.SchemasByKind) == 0 {
		return errors.New("schemas_by_kind is required")
	}
	if !json.Valid(row.SchemasByKind) {
		return errors.New("schemas_by_kind is not valid JSON")
	}
	return nil
}

// ---------------------------------------------------------------------------
// SQL helpers (used by both saga and reconcile)
// ---------------------------------------------------------------------------

// registerActor is the saga's thin adapter over registry.Register. The
// registry package owns the canonical write path (INSERT actor_registry
// + INSERT OR IGNORE actor_cursors + emit system.event payload.kind=
// actor_registered) per L1 §12.3 / L2 §1.4.6; the saga only forwards
// channel_id + the per-step ActorMeta so the writes join its IMMEDIATE
// tx. Surface error wrapping (step3 / step4 / ... prefixes) stays at
// the call site.
func registerActor(ctx context.Context, conn *sql.Conn, channelID string, meta registry.ActorMeta) error {
	return registry.Register(ctx, conn, channelID, meta)
}

func insertType(ctx context.Context, conn *sql.Conn, row TypeRegistryRow, now int64) error {
	terminal := row.TerminalConvention
	if terminal == "" {
		terminal = "payload_status"
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
	_, err = conn.ExecContext(ctx,
		`INSERT INTO type_registry
		   (type, allowed_kinds, schemas_by_kind, handler_binding,
		    terminal_convention, max_pending_ms, handler_actor_id, domain, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.Type, string(allowed), string(row.SchemasByKind), row.HandlerBinding,
		terminal, maxPending, handlerActor, domain, now,
	)
	return err
}

// channelCreatedEventID derives the deterministic envelope id used by
// step 8a. The `bootstrap:` prefix + create_request_id guarantees same
// id across retries; messages.id UNIQUE then dedupes naturally.
func channelCreatedEventID(createRequestID string) string {
	return "bootstrap:" + createRequestID
}

// channelCreatedPayload builds the well-known payload shape for the
// step 8a system.event. The shape is consumed by the server reconcile
// API (`channel_created` discriminator on payload.kind).
func channelCreatedPayload(channelID string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"kind":       "channel_created",
		"channel_id": channelID,
		"severity":   "info",
	})
}

// emitChannelCreated inserts the step 8a `system.event` row. Uses
// INSERT OR IGNORE so that reconcile-driven retries are idempotent
// against messages.id UNIQUE (L2 §1.4.7 step 8a "messages.id UNIQUE
// 自动 dedupe").
func emitChannelCreated(ctx context.Context, conn *sql.Conn, p CreateParams, now int64) error {
	payload, err := channelCreatedPayload(p.ChannelID)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	audience, err := json.Marshal([]string{"*"})
	if err != nil {
		return fmt.Errorf("marshal audience: %w", err)
	}
	_, err = conn.ExecContext(ctx,
		`INSERT OR IGNORE INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    sender_name, kind, type, payload, parent_id, correlation_id,
		    doc_refs, visibility, audience, not_before, expires_at,
		    delivered_at, delivery_failed_at, last_error, attempts, is_terminal)
		 VALUES
		   (?, ?, ?, ?, 'system', 'system',
		    NULL, 'event', 'system.event', ?, NULL, NULL,
		    NULL, 'system', ?, NULL, NULL,
		    NULL, NULL, NULL, 0, 0)`,
		channelCreatedEventID(p.CreateRequestID), now, now, p.ChannelID,
		string(payload), string(audience),
	)
	return err
}

// ---------------------------------------------------------------------------
// Internal helpers shared with reconcile.go
// ---------------------------------------------------------------------------

// injected returns the failpoint error if `key` is in the map. Production
// callers never see a non-nil map (only tests inject one).
func (s *Saga) injected(key string) error {
	if s.failpoints == nil {
		return nil
	}
	return s.failpoints[key]
}

// statExists wraps Saga.stat into a bool helper. Used by reconcile for
// workdir / channel sqlite presence checks.
func (s *Saga) statExists(path string) bool {
	if _, err := s.stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false
		}
		// Other stat errors (permission, IO) — treat as missing so
		// reconcile rolls back rather than retrying indefinitely.
		return false
	}
	return true
}
