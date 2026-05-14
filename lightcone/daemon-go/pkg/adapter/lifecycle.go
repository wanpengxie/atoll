package adapter

// lifecycle.go ties F1-F5 together into the daemon-side Manager. The
// Manager owns:
//
//   - one CorrelationTracker per adapter (sqlite-backed),
//   - one ErrorPolicy (timerPolicy) per adapter,
//   - a Respond closure bound to that adapter's actor identity,
//   - the routing tables (actor_id → adapter name, type → adapter name)
//     so Dispatch / OnExternalCallback know where to send incoming
//     traffic.
//
// Lifecycle steps (M1.3 baseline per L2 §8.6):
//
//  1. NewManager           — validate config + wire defaults.
//  2. Install              — apply adapter_correlation DDL, validate
//                            every module's Declaration against
//                            actor_registry / type_registry, build the
//                            per-adapter helpers, call Module.Init.
//  3. BootRecoverTimers    — scan pending tool requests in the channel
//                            sqlite + re-arm timers (or immediately fail
//                            terminals already past expires_at).
//  4. Dispatch             — route an inbound kind=request to the right
//                            Module.Handle + auto-register the L2 §8.6
//                            Ad-2 timeout timer (declares.max_pending_ms).
//  5. OnExternalCallback   — route an inbound callback to the named
//                            Module.OnExternalCallback. The L2 §8.5 race
//                            "terminal_duplicate => idempotent success"
//                            handling inside Respond already covers the
//                            spec §8.2 "duplicate callback" case; we
//                            emit `duplicate_callback` events only when
//                            the adapter explicitly returns one through
//                            Respond's Dedupe flag (observability is
//                            done via the system.event the Respond path
//                            already writes).
//  6. RunGC                — periodic ticker that scans every adapter's
//                            correlation table.
//  7. Shutdown             — stop timers + tick + propagate Module.Shutdown.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// ManagerConfig is the boot-time wiring the daemon hands to NewManager.
// Required fields: DB, Deps, Modules.
type ManagerConfig struct {
	// DB is the channel-local sqlite handle. The framework runs the
	// adapter_correlation DDL on Install and uses it for BootRecover
	// Timers' pending-request scan.
	DB *sql.DB

	// Deps is the shared harness dependency bundle (Store / Actors /
	// Types / WorkerLocks / Clock / Dispatcher / ChannelID). The
	// Manager reuses Store / Actors / Types for the install validators
	// and threads the bundle into the default writer.
	Deps pkgharness.Deps

	// Modules is the set of adapter implementations this Manager
	// supervises. Keys MUST match the adapter Declaration.Name (the
	// Manager re-asserts this in Install).
	Modules map[string]Module

	// Writer is the harness write seam used by Respond + correlation
	// GC event emit. Defaults to DefaultHarnessWriter(Deps).
	Writer HarnessWriter

	// Clock returns wall-clock milliseconds. Defaults to
	// time.Now().UnixMilli.
	Clock func() int64

	// Logger receives "adapter.*" events. Defaults to noopLogger.
	Logger Logger

	// GCGraceMs is the post-deadline grace before correlation entries
	// are evicted. Defaults to DefaultGCGraceMs (5 minutes).
	GCGraceMs int64

	// GCPeriod is the RunGC ticker period. Defaults to DefaultGCPeriod
	// (30 s).
	GCPeriod time.Duration

	// SystemActorID is the sender.id used when the framework emits
	// `system.event` rows (correlation_gc / duplicate_callback).
	// Defaults to "system".
	SystemActorID string
}

// validate returns an error for missing required fields.
func (c *ManagerConfig) validate() error {
	if c.DB == nil {
		return errors.New("adapter: ManagerConfig.DB is nil")
	}
	if err := validateDeps(c.Deps); err != nil {
		return fmt.Errorf("adapter: ManagerConfig.Deps: %w", err)
	}
	if len(c.Modules) == 0 {
		return errors.New("adapter: ManagerConfig.Modules must be non-empty")
	}
	return nil
}

func validateDeps(d pkgharness.Deps) error {
	if d.Store == nil {
		return errors.New("Deps.Store is nil")
	}
	if d.Actors == nil {
		return errors.New("Deps.Actors is nil")
	}
	if d.Types == nil {
		return errors.New("Deps.Types is nil")
	}
	if strings.TrimSpace(d.ChannelID) == "" {
		return errors.New("Deps.ChannelID is empty")
	}
	return nil
}

// applyDefaults fills in optional ManagerConfig fields.
func (c *ManagerConfig) applyDefaults() {
	if c.Writer == nil {
		c.Writer = DefaultHarnessWriter(c.Deps)
	}
	if c.Clock == nil {
		c.Clock = func() int64 { return time.Now().UnixMilli() }
	}
	if c.Logger == nil {
		c.Logger = noopLogger{}
	}
	if c.GCGraceMs <= 0 {
		c.GCGraceMs = DefaultGCGraceMs
	}
	if c.GCPeriod <= 0 {
		c.GCPeriod = DefaultGCPeriod
	}
	if strings.TrimSpace(c.SystemActorID) == "" {
		c.SystemActorID = "system"
	}
}

// moduleEntry is the per-adapter runtime state the Manager keeps.
type moduleEntry struct {
	module         Module
	decl           Declaration
	mctx           *ModuleContext
	tracker        *correlationTracker
	policy         *timerPolicy
	typeMaxPending map[string]int64
}

// Manager is the framework's daemon-side runtime. One instance per
// channel sqlite.
type Manager struct {
	cfg           ManagerConfig
	modules       map[string]*moduleEntry
	actorToModule map[string]string
	typeToModule  map[string]string

	mu        sync.Mutex
	installed bool
	closed    bool
}

// NewManager constructs a Manager from cfg. Returns an error on
// missing required fields. The returned Manager is NOT yet installed —
// callers MUST invoke Install (and typically BootRecoverTimers + RunGC)
// before dispatching traffic.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	m := &Manager{
		cfg:           cfg,
		modules:       make(map[string]*moduleEntry, len(cfg.Modules)),
		actorToModule: map[string]string{},
		typeToModule:  map[string]string{},
	}
	return m, nil
}

// Install applies the framework DDL, validates every module against
// the channel's actor_registry + type_registry, then calls Module.Init
// with a fully populated ModuleContext.
//
// Idempotent at the DDL layer (CREATE TABLE IF NOT EXISTS); calling
// Install twice on the same Manager value returns an error because
// the in-memory state would double-up.
func (m *Manager) Install(ctx context.Context) error {
	m.mu.Lock()
	if m.installed {
		m.mu.Unlock()
		return errors.New("adapter: Manager.Install called twice")
	}
	m.installed = true
	m.mu.Unlock()

	if _, err := m.cfg.DB.ExecContext(ctx, CorrelationTrackerDDL); err != nil {
		return fmt.Errorf("adapter: apply correlation DDL: %w", err)
	}

	// Deterministic install order (name asc) so log + reject sequencing
	// stays reproducible across runs.
	names := make([]string, 0, len(m.cfg.Modules))
	for n := range m.cfg.Modules {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		module := m.cfg.Modules[name]
		if module == nil {
			return fmt.Errorf("adapter: module %q is nil", name)
		}
		decl := module.Declares()
		if decl.Name == "" {
			decl.Name = name
		}
		if decl.Name != name {
			return fmt.Errorf("adapter: Modules key %q does not match Declaration.Name %q", name, decl.Name)
		}
		if err := decl.Validate(); err != nil {
			return err
		}

		// Verify the adapter actor exists + binding matches.
		actor, aerr := m.cfg.Deps.Actors.Get(ctx, decl.ActorID)
		if aerr != nil {
			return fmt.Errorf("adapter[%s]: actor lookup: %w", name, aerr)
		}
		if actor == nil || actor.DeregisteredAt != nil {
			return fmt.Errorf("adapter[%s]: actor %q is not registered (or deregistered)", name, decl.ActorID)
		}
		if actor.Kind != v4types.SenderTool {
			return fmt.Errorf("adapter[%s]: actor %q must have actor_kind='tool', got %q", name, decl.ActorID, actor.Kind)
		}
		if actor.Binding != decl.Binding {
			return fmt.Errorf("adapter[%s]: actor %q binding=%q != declaration binding=%q",
				name, decl.ActorID, actor.Binding, decl.Binding)
		}

		// Verify every declared type has a matching type_registry row.
		for _, t := range decl.Types {
			info, ok := m.cfg.Deps.Types.Get(t)
			if !ok {
				return fmt.Errorf("adapter[%s]: type %q not in type_registry", name, t)
			}
			if info.HandlerActorID != "" && info.HandlerActorID != decl.ActorID {
				return fmt.Errorf("adapter[%s]: type %q handler_actor_id=%q != declaration actor=%q",
					name, t, info.HandlerActorID, decl.ActorID)
			}
		}

		// Reserve routing slots; conflicts mean two adapters claim the
		// same actor / type which would be ambiguous at dispatch time.
		if other, dup := m.actorToModule[decl.ActorID]; dup {
			return fmt.Errorf("adapter[%s]: actor %q already claimed by %q", name, decl.ActorID, other)
		}
		for _, t := range decl.Types {
			if other, dup := m.typeToModule[t]; dup {
				return fmt.Errorf("adapter[%s]: type %q already claimed by %q", name, t, other)
			}
		}

		// Build the per-adapter helpers. The Respond closure captures
		// the policy / tracker for cleanup after a successful write —
		// we build them in two passes so the closure can see both.
		tracker := newCorrelationTracker(
			m.cfg.DB,
			decl.Name,
			m.cfg.Deps.ChannelID,
			m.cfg.SystemActorID,
			m.cfg.Writer,
			m.cfg.Clock,
			m.cfg.Logger,
		)
		var policy *timerPolicy
		respondCfg := respondConfig{
			adapterName:  decl.Name,
			adapterActor: decl.ActorID,
			channelID:    m.cfg.Deps.ChannelID,
			binding:      decl.Binding,
			writer:       m.cfg.Writer,
			store:        m.cfg.Deps.Store,
			clock:        m.cfg.Clock,
			logger:       m.cfg.Logger,
			tracker:      tracker,
		}
		respondFn := func(ctx context.Context, requestID string, payload json.RawMessage, opts RespondOptions) (RespondResult, error) {
			cfg := respondCfg
			cfg.policy = policy
			return respond(ctx, cfg, requestID, payload, opts)
		}
		policy = newTimerPolicy(decl.Name, respondFn, m.cfg.Clock, m.cfg.Logger)

		mctx := &ModuleContext{
			Name:        decl.Name,
			ActorID:     decl.ActorID,
			ChannelID:   m.cfg.Deps.ChannelID,
			Correlation: tracker,
			ErrorPolicy: policy,
			Respond:     respondFn,
		}
		typeMaxPending := make(map[string]int64, len(decl.MaxPendingMs))
		for t, v := range decl.MaxPendingMs {
			typeMaxPending[t] = v
		}

		// Call Module.Init last so adapters can store the context.
		if err := module.Init(ctx, mctx); err != nil {
			return fmt.Errorf("adapter[%s]: Init: %w", name, err)
		}

		entry := &moduleEntry{
			module:         module,
			decl:           decl,
			mctx:           mctx,
			tracker:        tracker,
			policy:         policy,
			typeMaxPending: typeMaxPending,
		}
		m.modules[decl.Name] = entry
		m.actorToModule[decl.ActorID] = decl.Name
		for _, t := range decl.Types {
			m.typeToModule[t] = decl.Name
		}
	}
	return nil
}

// BootRecoverTimers scans the channel sqlite for every pending tool
// request (kind=request, audience[0] is an active tool actor, no
// terminal response yet) and either re-emits the timeout terminal
// immediately (expires_at <= now) or registers a fresh F3 timer that
// fires at expires_at.
//
// L2 §8.6 mandates this on every daemon / framework boot — without it
// a daemon crash + restart would leave tool requests hanging forever
// (the long-pending scheduler explicitly skips `actor_kind='tool'`
// per L2 §3.7.1 "tool 走 framework F3 timer 兜底").
func (m *Manager) BootRecoverTimers(ctx context.Context) error {
	if !m.isInstalled() {
		return errors.New("adapter: BootRecoverTimers requires Install first")
	}
	rows, err := m.cfg.DB.QueryContext(ctx,
		`SELECT m.id, m.expires_at, m.type, json_extract(m.audience, '$[0]') AS receiver_id
		   FROM messages m
		   JOIN actor_registry a
		     ON a.actor_id = json_extract(m.audience, '$[0]')
		    AND a.deregistered_at IS NULL
		  WHERE m.kind = 'request'
		    AND a.actor_kind = 'tool'
		    AND NOT EXISTS (
		         SELECT 1 FROM messages r
		          WHERE r.parent_id = m.id
		            AND r.kind = 'response'
		            AND r.is_terminal = 1
		    )
		  ORDER BY m.seq ASC`,
	)
	if err != nil {
		return fmt.Errorf("adapter: BootRecoverTimers query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	now := m.cfg.Clock()
	type pending struct {
		id         string
		expiresAt  sql.NullInt64
		typ        string
		receiverID string
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.expiresAt, &p.typ, &p.receiverID); err != nil {
			return fmt.Errorf("adapter: BootRecoverTimers scan: %w", err)
		}
		batch = append(batch, p)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("adapter: BootRecoverTimers rows: %w", err)
	}

	for _, p := range batch {
		name, ok := m.actorToModule[p.receiverID]
		if !ok {
			// Receiver is a tool actor but not bound to any module the
			// Manager knows about. M1.3 baseline tolerates this — for
			// example built-in `tool:fs.read` actors handled by the
			// in-worker v4tool wrapper, not by an adapter Module. The
			// long-pending scheduler stays unaware (kind=tool), and
			// the v4tool wrapper holds the side-effect contract for
			// these requests within the worker process. Log + skip.
			m.cfg.Logger.Info("adapter.boot_recover.skip_unbound",
				"request_id", p.id,
				"receiver_id", p.receiverID,
			)
			continue
		}
		entry := m.modules[name]

		// Compute deadline relative to now.
		deadlineMs := int64(0)
		if p.expiresAt.Valid {
			deadlineMs = p.expiresAt.Int64
		} else if ms, ok := entry.typeMaxPending[p.typ]; ok {
			// Defensive: type_registry's max_pending_ms is the source
			// of truth, but the request envelope may not have stamped
			// expires_at if the writer skipped that normalization step
			// (older writers). Fall back to "now + declares.max_pending_ms".
			deadlineMs = now + ms
		} else {
			m.cfg.Logger.Warn("adapter.boot_recover.no_deadline",
				"request_id", p.id, "type", p.typ)
			continue
		}

		if deadlineMs <= now {
			// Already expired — fail immediately. Errors are logged but
			// don't stop the boot loop (next tick re-tries via GC scan).
			if _, err := entry.policy.FailTerminal(ctx, p.id,
				string(v4types.TerminalAdapterDefaultTimeout), nil); err != nil {
				m.cfg.Logger.Warn("adapter.boot_recover.fail_terminal.error",
					"adapter", name,
					"request_id", p.id,
					"err", err.Error(),
				)
			}
			continue
		}
		afterMs := deadlineMs - now
		if err := entry.policy.Timeout(p.id, afterMs,
			string(v4types.TerminalAdapterDefaultTimeout)); err != nil {
			m.cfg.Logger.Warn("adapter.boot_recover.timer.error",
				"adapter", name,
				"request_id", p.id,
				"err", err.Error(),
			)
		}
	}
	return nil
}

// Dispatch routes an inbound request envelope to the module whose
// Declaration.ActorID equals env.Audience[0]. It also auto-registers
// the L2 §8.6 timeout timer (declares.max_pending_ms) so the Ad-2
// contract holds even when the adapter forgets to call Timeout itself.
func (m *Manager) Dispatch(ctx context.Context, env *v4types.Envelope) error {
	if env == nil {
		return errors.New("adapter: Dispatch envelope is nil")
	}
	if env.Kind != v4types.KindRequest {
		return fmt.Errorf("adapter: Dispatch only handles kind=request (got %q)", env.Kind)
	}
	if len(env.Audience) != 1 {
		return fmt.Errorf("adapter: Dispatch envelope.audience must have one receiver (got %v)", env.Audience)
	}
	target := env.Audience[0]
	name, ok := m.actorToModule[target]
	if !ok {
		return fmt.Errorf("adapter: Dispatch no adapter bound to actor %q", target)
	}
	entry := m.modules[name]
	if ms, ok := entry.typeMaxPending[env.Type]; ok {
		// Use envelope's expires_at when present so timer fires at the
		// same wall clock the harness would. Fall back to "now + ms"
		// when expires_at is absent.
		var afterMs int64
		now := m.cfg.Clock()
		if env.ExpiresAt != nil && *env.ExpiresAt > now {
			afterMs = *env.ExpiresAt - now
		} else {
			afterMs = ms
		}
		if afterMs > 0 {
			if err := entry.policy.Timeout(env.ID, afterMs,
				string(v4types.TerminalAdapterDefaultTimeout)); err != nil {
				m.cfg.Logger.Warn("adapter.dispatch.timer.error",
					"adapter", name,
					"request_id", env.ID,
					"err", err.Error(),
				)
			}
		}
	}
	return entry.module.Handle(ctx, env)
}

// OnExternalCallback routes a raw external payload to the named
// adapter's Module.OnExternalCallback. Unknown adapter names yield
// an error; the adapter MUST be present in this Manager's Modules.
//
// L2 §8.2 "duplicate callback" handling: the framework relies on
// Respond's terminal_duplicate → idempotent success path to absorb a
// second callback for an already-resolved request. The explicit
// `duplicate_callback` system.event emit is a Should-tier observability
// hook (M1.x).
func (m *Manager) OnExternalCallback(ctx context.Context, adapterName string, payload []byte) error {
	if !m.isInstalled() {
		return errors.New("adapter: OnExternalCallback requires Install first")
	}
	entry, ok := m.modules[adapterName]
	if !ok {
		return fmt.Errorf("adapter: OnExternalCallback unknown adapter %q", adapterName)
	}
	return entry.module.OnExternalCallback(ctx, payload)
}

// RunGC drives the correlation-table garbage collector. Blocks until
// ctx is cancelled. Each tick walks every adapter's adapter_correlation
// rows + evicts entries whose `deadline_ms + GCGraceMs < now`.
func (m *Manager) RunGC(ctx context.Context) {
	if !m.isInstalled() {
		m.cfg.Logger.Warn("adapter.gc.skip_uninstalled")
		return
	}
	ticker := time.NewTicker(m.cfg.GCPeriod)
	defer ticker.Stop()

	// One sweep on entry so daemon boot does not wait `Period` before
	// purging anything left over from a previous run.
	m.runGCOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runGCOnce(ctx)
		}
	}
}

// runGCOnce executes a single GC pass across every module's tracker.
// Exposed (lower-case) so tests can step the sweep deterministically.
func (m *Manager) runGCOnce(ctx context.Context) {
	now := m.cfg.Clock()
	for name, entry := range m.modules {
		stats, err := entry.tracker.gc(ctx, now, m.cfg.GCGraceMs)
		if err != nil {
			m.cfg.Logger.Warn("adapter.gc.error",
				"adapter", name,
				"err", err.Error(),
			)
			continue
		}
		if stats.Evicted > 0 {
			m.cfg.Logger.Info("adapter.gc.swept",
				"adapter", name,
				"scanned", stats.Scanned,
				"evicted", stats.Evicted,
			)
		}
	}
}

// Shutdown stops every adapter's timers + ticker and invokes
// Module.Shutdown. Idempotent. Errors from individual Module.Shutdown
// calls are logged but not propagated — one bad adapter does not
// block the rest from tearing down.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	for name, entry := range m.modules {
		entry.policy.shutdown()
		if err := entry.module.Shutdown(ctx); err != nil {
			m.cfg.Logger.Warn("adapter.shutdown.error",
				"adapter", name,
				"err", err.Error(),
			)
		}
	}
	return nil
}

// isInstalled reports whether Install has run successfully. Tests use
// it via the public Manager surface (BootRecoverTimers etc.) so they
// don't need to peek inside private state.
func (m *Manager) isInstalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.installed
}

// ModuleNames returns the deterministic list of adapter names this
// Manager supervises. Useful for tests + ops introspection.
func (m *Manager) ModuleNames() []string {
	out := make([]string, 0, len(m.modules))
	for n := range m.modules {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
