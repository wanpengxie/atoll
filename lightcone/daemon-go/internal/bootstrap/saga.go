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

	"github.com/coagent-ai/daemon-go/internal/store"
)

// channelDBFilename is the well-known basename of the channel-local
// sqlite file inside the workdir (L2 §1.2).
const channelDBFilename = "messages.sqlite"

// New constructs a Saga bound to the daemon-level sqlite pool. The
// daemon caller is expected to keep `daemonDB` open for the lifetime
// of the process (it backs bootstrap_registry CAS + reconcile).
func New(daemonDB *sql.DB, opts ...Option) *Saga {
	s := &Saga{
		daemonDB: daemonDB,
		now:      nowUnix,
		openCh:   defaultOpenChannel,
		mkdir:    os.MkdirAll,
		rmAll:    os.RemoveAll,
		stat:     os.Stat,
	}
	for _, o := range opts {
		o(s)
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
	if err := validateParams(p); err != nil {
		return Result{}, err
	}

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
		p.CreateRequestID, p.ChannelID, StatusInProgress, p.WorkdirPath, startedAt,
	); err != nil {
		return Result{}, fmt.Errorf("bootstrap: step1 insert: %w", err)
	}

	// From here on, any error must run the rollback compensation
	// (delete workdir + UPDATE status='rolled_back'). Defer captures
	// the named return so we can run the cleanup on the way out.
	channelDBPath := filepath.Join(p.WorkdirPath, channelDBFilename)
	rollback := func(reason error) {
		s.compensate(ctx, p.CreateRequestID, p.WorkdirPath, reason)
	}

	// ---- Step 2: mkdir + open channel sqlite ------------------------------
	if err := s.injected(fpStep2Mkdir); err != nil {
		rollback(err)
		return Result{}, fmt.Errorf("bootstrap: step2 mkdir: %w", err)
	}
	if err := s.mkdir(p.WorkdirPath, 0o755); err != nil {
		rollback(err)
		return Result{}, fmt.Errorf("bootstrap: step2 mkdir: %w", err)
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
	if err := insertActor(ctx, conn, "system", "system", "", now); err != nil {
		return fmt.Errorf("step3 system actor: %w", err)
	}

	// --- Step 4: human members -----------------------------------------
	for i, m := range p.HumanMembers {
		if err := s.injected(fpStep4Human); err != nil {
			return fmt.Errorf("step4 human member[%d] %s: %w", i, m.ActorID, err)
		}
		if err := insertActor(ctx, conn, m.ActorID, "human", "", now); err != nil {
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
	if err := insertActor(ctx, conn, agentID, "agent", "daemon_rpc", now); err != nil {
		return fmt.Errorf("step5 channel agent %s: %w", agentID, err)
	}

	// --- Step 6: tool adapters (actor_registry → type_registry) --------
	for i, ad := range p.ToolAdapters {
		if err := s.injected(fpStep6Adapter); err != nil {
			return fmt.Errorf("step6 adapter[%d] %s: %w", i, ad.ActorID, err)
		}
		if err := insertActor(ctx, conn, ad.ActorID, "tool", ad.Binding, now); err != nil {
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
		// L2 §1.4.2: handler_actor_id (if set) must resolve to a row
		// already in actor_registry within the same tx.
		if row.HandlerActorID != "" {
			ok, err := actorExists(ctx, conn, row.HandlerActorID)
			if err != nil {
				return fmt.Errorf("step7 business type[%d] %s lookup: %w", i, row.Type, err)
			}
			if !ok {
				return fmt.Errorf("step7 business type[%d] %s: handler_actor_id %q not registered",
					i, row.Type, row.HandlerActorID)
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
//  1. os.RemoveAll(workdir) — best-effort; report (not retried) but
//     never block the status update.
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

	// Best-effort workdir cleanup. We DO NOT propagate errors — the
	// status update is more important; the next Reconcile pass will
	// retry the rm if it fails here.
	_ = s.rmAll(workdirPath)

	_, _ = s.daemonDB.ExecContext(ctx,
		`UPDATE bootstrap_registry
		   SET status=?, rollback_reason=?
		 WHERE create_request_id=? AND status=?`,
		StatusRolledBack, reason, createRequestID, StatusInProgress,
	)
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func validateParams(p CreateParams) error {
	if strings.TrimSpace(p.CreateRequestID) == "" {
		return fmt.Errorf("%w: create_request_id is required", ErrParamsInvalid)
	}
	if strings.TrimSpace(p.ChannelID) == "" {
		return fmt.Errorf("%w: channel_id is required", ErrParamsInvalid)
	}
	if strings.TrimSpace(p.WorkdirPath) == "" {
		return fmt.Errorf("%w: workdir_path is required", ErrParamsInvalid)
	}
	if !filepath.IsAbs(p.WorkdirPath) {
		return fmt.Errorf("%w: workdir_path must be absolute, got %q",
			ErrParamsInvalid, p.WorkdirPath)
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

func insertActor(ctx context.Context, conn *sql.Conn, actorID, kind, binding string, now int64) error {
	var bindingArg any
	if binding == "" {
		bindingArg = nil
	} else {
		bindingArg = binding
	}
	_, err := conn.ExecContext(ctx,
		`INSERT INTO actor_registry
		   (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES (?, ?, ?, ?, NULL)`,
		actorID, kind, bindingArg, now,
	)
	return err
}

func actorExists(ctx context.Context, conn *sql.Conn, actorID string) (bool, error) {
	var got string
	err := conn.QueryRowContext(ctx,
		`SELECT actor_id FROM actor_registry WHERE actor_id = ? AND deregistered_at IS NULL`,
		actorID,
	).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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
