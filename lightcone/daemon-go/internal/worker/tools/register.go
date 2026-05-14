// Package tools implements the M1.3 T11 "built-in tool actor" surface:
// the channel-local catalogue of v4 tool types (fs.read / fs.write /
// shell.exec / web.search / ...), their actor_registry rows, and the
// helper that hands the worker runtime a slice of go-kimi tools.Tool
// values pre-wrapped by v4tool.V4ize.
//
// Public API:
//
//	EnsureToolActors — idempotent install of actor_registry +
//	                   type_registry rows for every catalogue entry.
//	BuildTools       — wrap each go-kimi tool with v4tool.V4ize using
//	                   the catalogue's type/actor mapping.
//	Catalog          — the canonical list of v4 tool descriptors used
//	                   by both functions (single source of truth).
//
// The list of tools is intentionally narrower than go-kimi's full
// tool surface — only the L2 §3.9.4 "internal tool actor" set lands on
// the channel log (file / shell / web / plan / question / think /
// sqlite). Specialised go-kimi tools (background, dmail, sandbox,
// subagent) are deferred until a later ticket can sort out their
// channel semantics; today they would emit channel rows but the
// receiving end has no UI for them.
package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/internal/worker/v4tool"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"

	kimitools "github.com/wanpengxie/go-kimi/pkg/kimi/tools"
	"github.com/wanpengxie/go-kimi/pkg/kimi/tools/file"
	"github.com/wanpengxie/go-kimi/pkg/kimi/tools/shell"
	"github.com/wanpengxie/go-kimi/pkg/kimi/tools/think"
	"github.com/wanpengxie/go-kimi/pkg/kimi/tools/web"
)

// Descriptor describes one v4 tool entry: the protocol type, the actor
// id, the per-tool L2 §1.4.2 max_pending_ms, and the constructor that
// produces the underlying go-kimi tools.Tool. Tests + Production both
// run off the same Catalog slice so there is no drift between what
// EnsureToolActors writes and what BuildTools wraps.
type Descriptor struct {
	// Type is the v4 envelope.type (e.g. "fs.read"). Also seeds the
	// actor id (`tool:<Type>`) and the wrapper's primary identity.
	Type string

	// MaxPendingMs is the type_registry.max_pending_ms value. Required
	// per L2 §1.4.2 — tool receivers must declare a timeout.
	MaxPendingMs int64

	// Build returns the underlying go-kimi tools.Tool. Pass-through
	// args (workdir, sqlite handle) are extracted from BuildConfig so
	// each constructor only sees what it needs.
	Build func(BuildConfig) kimitools.Tool
}

// ActorID returns the canonical sender.id for this descriptor's tool
// actor. Centralised so callers and tests share the same convention.
func (d Descriptor) ActorID() string { return "tool:" + d.Type }

// Catalog returns the M1.3 baseline list of v4-ized tools. Order is
// stable so the type_registry install + actor seeding always run
// against the same sequence (tests rely on this).
func Catalog() []Descriptor {
	return []Descriptor{
		{Type: "fs.read", MaxPendingMs: 5_000, Build: func(cfg BuildConfig) kimitools.Tool {
			return file.NewReadFile(cfg.WorkDir)
		}},
		{Type: "fs.write", MaxPendingMs: 5_000, Build: func(cfg BuildConfig) kimitools.Tool {
			return file.NewWriteFile(cfg.WorkDir, autoApproveFile)
		}},
		{Type: "fs.list", MaxPendingMs: 5_000, Build: func(cfg BuildConfig) kimitools.Tool {
			return file.NewGlob(cfg.WorkDir)
		}},
		{Type: "shell.exec", MaxPendingMs: 30_000, Build: func(cfg BuildConfig) kimitools.Tool {
			return shell.NewWithBackground(cfg.WorkDir, autoApproveShell, nil, "")
		}},
		{Type: "web.search", MaxPendingMs: 15_000, Build: func(cfg BuildConfig) kimitools.Tool {
			client := cfg.HTTPClient
			if client == nil {
				client = http.DefaultClient
			}
			return web.NewSearchWeb(cfg.SearchServiceURL, client)
		}},
		{Type: "web.fetch", MaxPendingMs: 15_000, Build: func(cfg BuildConfig) kimitools.Tool {
			client := cfg.HTTPClient
			if client == nil {
				client = http.DefaultClient
			}
			return web.NewFetchURL(client)
		}},
		{Type: "think", MaxPendingMs: 2_000, Build: func(_ BuildConfig) kimitools.Tool {
			return think.New()
		}},
		{Type: "sqlite.query", MaxPendingMs: 5_000, Build: func(cfg BuildConfig) kimitools.Tool {
			return NewSQLiteQueryTool(cfg.DB)
		}},
	}
}

// -----------------------------------------------------------------------------
// EnsureToolActors — actor_registry + type_registry idempotent install
// -----------------------------------------------------------------------------

// EnsureConfig wires EnsureToolActors to a channel sqlite. ChannelID +
// DB + Now are required; the rest fall back to safe defaults.
type EnsureConfig struct {
	// DB is the channel sqlite handle.
	DB *sql.DB

	// ChannelID is the channel scope; actor_registry rows + the
	// system.event audit rows are keyed by it.
	ChannelID string

	// Now is the timestamp (unix seconds) stamped into actor_registry
	// and type_registry. Tests inject a deterministic value; production
	// callers pass time.Now().Unix().
	Now int64

	// Logger receives "tools.ensure.*" events. Defaults to a no-op
	// logger when nil.
	Logger Logger
}

// EnsureToolActors installs every Catalog entry into actor_registry +
// type_registry. Both inserts are idempotent — existing rows are left
// untouched (actor PK conflict / messages.id UNIQUE on the audit row).
//
// Order matches the L2 §3.5 "install 顺序契约":
//  1. actor_registry rows first (handler_actor_id in type rows depends
//     on them existing).
//  2. type_registry rows second, inside the same IMMEDIATE tx so a
//     mid-install crash leaves the channel either fully provisioned
//     or fully untouched.
//
// Returns nil when every row is either already present or freshly
// inserted; sql / install errors bubble up untouched.
func EnsureToolActors(ctx context.Context, cfg EnsureConfig) error {
	if cfg.DB == nil {
		return errors.New("tools: EnsureToolActors DB is nil")
	}
	if strings.TrimSpace(cfg.ChannelID) == "" {
		return errors.New("tools: EnsureToolActors ChannelID is empty")
	}
	if cfg.Now <= 0 {
		return errors.New("tools: EnsureToolActors Now must be positive")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = noopLogger{}
	}

	descriptors := Catalog()
	return store.WithImmediate(ctx, cfg.DB, func(ctx context.Context, conn *sql.Conn) error {
		// Phase 1: actor_registry seeding (idempotent).
		for _, d := range descriptors {
			meta := registry.ActorMeta{
				ActorID:   d.ActorID(),
				Kind:      registry.KindTool,
				Binding:   registry.BindingInWorkerBus,
				CreatedAt: cfg.Now,
			}
			if err := registry.Register(ctx, conn, cfg.ChannelID, meta); err != nil {
				if errors.Is(err, registry.ErrActorExists) {
					logger.Info("tools.ensure.actor.exists", "actor_id", meta.ActorID)
					continue
				}
				return fmt.Errorf("register %s: %w", meta.ActorID, err)
			}
			logger.Info("tools.ensure.actor.created", "actor_id", meta.ActorID)
		}

		// Phase 2: type_registry rows. Install validates structurally
		// + checks fallback schema + writes. We skip rows that already
		// exist by probing type_registry first — Install does not have
		// an idempotent mode.
		toInstall := make([]registry.TypeRow, 0, len(descriptors))
		for _, d := range descriptors {
			present, err := typeRowExists(ctx, conn, d.Type)
			if err != nil {
				return fmt.Errorf("probe type %s: %w", d.Type, err)
			}
			if present {
				logger.Info("tools.ensure.type.exists", "type", d.Type)
				continue
			}
			row, err := buildTypeRow(d)
			if err != nil {
				return fmt.Errorf("build type %s: %w", d.Type, err)
			}
			toInstall = append(toInstall, row)
		}
		if len(toInstall) == 0 {
			return nil
		}
		if err := registry.Install(ctx, conn, toInstall, cfg.Now); err != nil {
			return fmt.Errorf("install tool types: %w", err)
		}
		for _, row := range toInstall {
			logger.Info("tools.ensure.type.created", "type", row.Type)
		}
		return nil
	})
}

// typeRowExists reports whether the channel sqlite already has a
// type_registry row for typeName.
func typeRowExists(ctx context.Context, conn *sql.Conn, typeName string) (bool, error) {
	row := conn.QueryRowContext(ctx,
		`SELECT 1 FROM type_registry WHERE type = ?`, typeName)
	var probe int
	if err := row.Scan(&probe); err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// buildTypeRow assembles the type_registry payload for one descriptor.
// All built-in tool types share:
//   - request schema: type=object (loose — the inner tool validates
//     its own params).
//   - response schema: `{status, reason, value?}` with status enum
//     {completed, failed}, reason as free string (required so §3.7
//     fallback emits pass).
//   - handler_binding: in_worker_bus.
//   - handler_actor_id: tool:<type>.
//   - terminal_convention: payload_status (default — `completed` /
//     `failed` are the only terminal statuses).
func buildTypeRow(d Descriptor) (registry.TypeRow, error) {
	maxPending := d.MaxPendingMs
	schemas, err := json.Marshal(map[string]any{
		"request": map[string]any{"type": "object"},
		"response": map[string]any{
			"type":     "object",
			"required": []string{"status"},
			"properties": map[string]any{
				"status": map[string]any{"type": "string", "enum": []string{"completed", "failed"}},
				"reason": map[string]any{"type": "string"},
				// value is intentionally not declared — the wrapper
				// supplies it on success but it remains free-form JSON
				// because the underlying tool catalogue is too diverse
				// to pin in a single schema.
			},
			"additionalProperties": true,
		},
	})
	if err != nil {
		return registry.TypeRow{}, fmt.Errorf("marshal schemas: %w", err)
	}
	return registry.TypeRow{
		Type:           d.Type,
		AllowedKinds:   []string{"request", "response"},
		SchemasByKind:  schemas,
		HandlerBinding: registry.HandlerBindingInWorkerBus,
		MaxPendingMs:   &maxPending,
		HandlerActorID: d.ActorID(),
	}, nil
}

// -----------------------------------------------------------------------------
// BuildTools — produce V4ize-wrapped tool slice for AgentConfig
// -----------------------------------------------------------------------------

// BuildConfig wires BuildTools to the worker's harness + sqlite +
// per-channel workdir. Required fields: DB, ChannelID, AgentID,
// TurnID, WorkDir, Deps. The rest fall back to safe defaults.
type BuildConfig struct {
	// DB is the channel sqlite — feeds the v4 wrapper's ledger
	// executor and the inner sqlite.query tool.
	DB *sql.DB

	// ChannelID is the channel scope (envelope.channel_id).
	ChannelID string

	// AgentID is the worker's agent identity (envelope.sender.id on
	// request emits).
	AgentID string

	// FencingToken is the worker_locks fencing_token. Forwarded to
	// the wrapper so harness Step 3 can verify the lease lifetime.
	FencingToken int64

	// TurnID seeds the ledger key. Spec §3.9.3 derives this from
	// `hash(actor_id, min_seq_in_batch)`; M1.3 baseline uses
	// `turn:<agent_id>:<trigger_msg_id>`.
	TurnID string

	// TriggerCorrelationID propagates the trigger's correlation_id
	// per L1 §2.2.1.
	TriggerCorrelationID string

	// WorkDir is the channel/agent workdir; passed to file + shell
	// constructors so the underlying tools operate inside the channel
	// sandbox.
	WorkDir string

	// Deps is the harness dependency bundle the wrapper writes through.
	Deps pkgharness.Deps

	// HTTPClient is an optional override for the web.fetch / web.search
	// inner tools.
	HTTPClient *http.Client

	// SearchServiceURL is the web.search backend (empty falls back to
	// the constructor's default).
	SearchServiceURL string

	// Clock returns the current wall-clock in milliseconds. Defaults
	// to nil (wrapper falls back to time.Now().UnixMilli).
	Clock func() int64

	// NowSec returns the current wall-clock in seconds for the
	// action_ledger row. Defaults to nil (wrapper uses time.Now().Unix).
	NowSec func() int64

	// Logger receives "v4tool.wrapper.*" events.
	Logger v4tool.Logger
}

// Validate enforces the minimum field set BuildTools needs.
func (c BuildConfig) Validate() error {
	switch {
	case c.DB == nil:
		return errors.New("tools: BuildConfig.DB is nil")
	case strings.TrimSpace(c.ChannelID) == "":
		return errors.New("tools: BuildConfig.ChannelID is empty")
	case strings.TrimSpace(c.AgentID) == "":
		return errors.New("tools: BuildConfig.AgentID is empty")
	case strings.TrimSpace(c.TurnID) == "":
		return errors.New("tools: BuildConfig.TurnID is empty")
	case strings.TrimSpace(c.WorkDir) == "":
		return errors.New("tools: BuildConfig.WorkDir is empty")
	case c.Deps.Store == nil:
		return errors.New("tools: BuildConfig.Deps.Store is nil")
	}
	return nil
}

// BuildTools returns one V4ize-wrapped tools.Tool per Catalog entry,
// ready to drop into kimi.AgentConfig.AdditionalTools. Returns an
// error wrapping the first failure when any descriptor's wrapper
// rejects its config (should only happen on programmer error).
func BuildTools(cfg BuildConfig) ([]kimitools.Tool, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	descriptors := Catalog()
	out := make([]kimitools.Tool, 0, len(descriptors))
	for _, d := range descriptors {
		inner := d.Build(cfg)
		if inner == nil {
			return nil, fmt.Errorf("tools: descriptor %s built nil tool", d.Type)
		}
		wcfg := v4tool.Config{
			TypeName:             d.Type,
			ToolActorID:          d.ActorID(),
			CallerActorID:        cfg.AgentID,
			ChannelID:            cfg.ChannelID,
			FencingToken:         cfg.FencingToken,
			TurnID:               cfg.TurnID,
			TriggerCorrelationID: cfg.TriggerCorrelationID,
			LedgerExec:           cfg.DB,
			Deps:                 cfg.Deps,
			Clock:                cfg.Clock,
			NowSec:               cfg.NowSec,
			Logger:               cfg.Logger,
		}
		wrapped, err := v4tool.V4ize(inner, wcfg)
		if err != nil {
			return nil, fmt.Errorf("tools: wrap %s: %w", d.Type, err)
		}
		out = append(out, wrapped)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------------

// Logger is the minimal log surface the package uses. Mirrors the
// worker package's Logger so callers can reuse a single instance.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// isNoRows mirrors v4tool.isNoRows — we cannot import the wrapper's
// private helper without exporting it. Uses errors.Is(err, sql.ErrNoRows)
// so wrapper errors (`fmt.Errorf("...: %w", sql.ErrNoRows)`) and future
// driver-level wrappers resolve correctly; string-matching the stdlib's
// internal text is fragile across Go releases (claude 96-1 major:
// T103 / FIX C).
func isNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// autoApproveFile + autoApproveShell are the go-kimi Approver callbacks
// (a func type, not an interface) for file/shell tools. M1.3 baseline:
// approval gating lives at the channel level (per L4 domain templates)
// — the worker-local tool wrappers do not re-prompt. A later ticket
// can wire an HITL approver if needed.
//
// Both signatures are `func(ctx, action, desc) (bool, reason)`.
func autoApproveFile(_ context.Context, _, _ string) (bool, string)  { return true, "" }
func autoApproveShell(_ context.Context, _, _ string) (bool, string) { return true, "" }
