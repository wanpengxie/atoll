package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// TemplateView is the bootstrap-side projection of one L4 channel
// template (M1.6-T5 phase-2). The Saga consumes it inside Bootstrap to
// (a) seed adapter tool actor rows so framework.Manager.Install can
// locate them in step 5b and (b) materialise template-declared workdir
// subdirectories in new step 5c.
//
// TemplateView is intentionally a value type defined here (NOT in
// runtime/) so the bootstrap package does NOT take a dependency on the
// runtime package — `runtime` constructs a resolver closure during
// AssembleDaemon that converts its richer ChannelTemplate into this
// minimal view.
type TemplateView struct {
	// AdapterActorSeeds lists the actor_registry rows the saga inserts
	// in addition to system + initial members. Each row supplies enough
	// fields for kernel/adapter.Manager.Install to find the actor with
	// the right binding.
	AdapterActorSeeds []actor.Record

	// WorkdirSubdirs lists relative directory paths the saga MUST
	// mkdir under <ChannelsDir>/<channelID>/ during step 5c (after the
	// channel sqlite is open / actor seeds are inserted). Each entry is
	// a path component (e.g. "published-notes", "drafts", "assets")
	// joined with channelDir before MkdirAll. Empty list = no extra
	// subdirs (legacy / generic channels).
	WorkdirSubdirs []string
}

// Saga is the daemon-side channel bootstrap orchestrator (L2 §3.6
// bootstrap_registry 9-step). For M1.5-T3 we ship a lean variant
// covering the data-plane steps the protocol requires; richer features
// (per-channel adapters / type_registry preload / placement reconciliation)
// land in T4 / T5 / T6.
//
// The saga executes these steps for a fresh channel:
//
//  1. Insert bootstrap_registry row (status='in_progress').
//  2. mkdir <ChannelsDir>/<channelID>/.
//  3. Open / create channels/<id>/channel.sqlite with DDL.
//  4. Insert actor_registry row for 'system' actor.
//  5. Insert actor_registry + member rows from req.InitialMembers.
//     5b. Insert template AdapterActorSeeds (M1.6-T2 — pre-creates tool
//     adapter rows so framework.Manager.Install can locate them). Seeds
//     come from the resolved TemplateView for req.ChannelType when a
//     ResolveTemplate callback is wired; otherwise the saga falls back
//     to the AdapterActorSeeds field for backward compatibility.
//     5c. Mkdir template WorkdirSubdirs (M1.6-T5 phase-2 — e.g.
//     published-notes/, drafts/, assets/ for the xhs-creator template).
//  6. (caller — runtime/lifecycle.Creator / runtime.Daemon) writes
//     channel_lock row.
//  7. Caller invokes Complete to mark bootstrap_registry status='completed'
//     only after channel_lock is durable.
//
// On failure between steps 2 and 7 the row is left status='in_progress'
// so reconcile.go can roll it back on next start.
type Saga struct {
	daemonDB          *sql.DB
	channelsDir       string
	nowFn             func() int64
	adapterActorSeeds []actor.Record
	resolveTemplate   func(channelType string) TemplateView
}

// SagaConfig wires Saga.
type SagaConfig struct {
	DaemonDB    *sql.DB
	ChannelsDir string
	NowFn       func() int64

	// AdapterActorSeeds is the static set of tool actor_registry rows
	// the saga MUST insert in addition to the system + initial member
	// rows (M1.6-T2 ChannelTemplate). Empty list = no extra actors.
	//
	// Deprecated: callers wiring multiple templates (M1.6-T5 phase-2)
	// MUST instead provide ResolveTemplate so the per-channel template
	// is selected by req.ChannelType. The static field stays as a
	// fallback for legacy single-template wiring and the existing tests.
	AdapterActorSeeds []actor.Record

	// ResolveTemplate, when non-nil, is consulted in Bootstrap to obtain
	// the per-channel TemplateView keyed by CreateChannelRequest.ChannelType
	// (M1.6-T5 phase-2). When nil the saga uses the legacy
	// AdapterActorSeeds field for every channel and skips the workdir-
	// subdir step.
	//
	// The callback MUST return a usable TemplateView even for unknown
	// types (return the zero value to mean "no template" — saga seeds
	// only system + initial members).
	ResolveTemplate func(channelType string) TemplateView
}

// NewSaga builds a Saga.
func NewSaga(cfg SagaConfig) (*Saga, error) {
	if cfg.DaemonDB == nil {
		return nil, errors.New("bootstrap: SagaConfig.DaemonDB nil")
	}
	if cfg.ChannelsDir == "" {
		return nil, errors.New("bootstrap: SagaConfig.ChannelsDir empty")
	}
	if cfg.NowFn == nil {
		return nil, errors.New("bootstrap: SagaConfig.NowFn nil")
	}
	seeds := make([]actor.Record, len(cfg.AdapterActorSeeds))
	copy(seeds, cfg.AdapterActorSeeds)
	return &Saga{
		daemonDB:          cfg.DaemonDB,
		channelsDir:       cfg.ChannelsDir,
		nowFn:             cfg.NowFn,
		adapterActorSeeds: seeds,
		resolveTemplate:   cfg.ResolveTemplate,
	}, nil
}

// Bootstrap implements lifecycle.ChannelBootstrapper.
func (s *Saga) Bootstrap(
	ctx context.Context,
	channelID channel.ID,
	req placement.CreateChannelRequest,
) (string, error) {
	createReq := string(req.CreateRequestID)
	if createReq == "" {
		return "", errors.New("bootstrap: empty create_request_id")
	}
	channelDir := filepath.Join(s.channelsDir, string(channelID))
	sqlitePath := filepath.Join(channelDir, "channel.sqlite")

	// Step 1 — bootstrap_registry INSERT (or honor idempotent retry).
	if err := s.insertRegistry(ctx, createReq, channelID, channelDir); err != nil {
		return "", err
	}

	// Step 2 — mkdir.
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		return "", fmt.Errorf("bootstrap: mkdir %s: %w", channelDir, err)
	}

	// Step 3 — open channel sqlite (DDL runs on first open).
	channelDB, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{})
	if err != nil {
		return "", fmt.Errorf("bootstrap: open sqlite %s: %w", sqlitePath, err)
	}
	defer func() { _ = channelDB.Close() }()

	// Step 4 — register system actor.
	reg := store.NewActorRegistry(channelDB)
	if err := s.insertActorIfMissing(ctx, reg, actor.Record{
		ID:        actor.SystemActorID,
		Kind:      actor.KindSystem,
		Binding:   "",
		CreatedAt: s.nowFn(),
	}); err != nil {
		return "", fmt.Errorf("bootstrap: insert system actor: %w", err)
	}

	// Step 5 — initial members.
	for _, m := range req.InitialMembers {
		if m.ActorIDInChannel == "" {
			continue
		}
		kind := actor.KindHuman
		if m.Kind != "" {
			kind = actor.Kind(m.Kind)
		}
		if err := s.insertActorIfMissing(ctx, reg, actor.Record{
			ID:          actor.ActorID(m.ActorIDInChannel),
			Kind:        kind,
			DisplayName: m.DisplayName,
			CreatedAt:   s.nowFn(),
		}); err != nil {
			return "", fmt.Errorf("bootstrap: insert member %s: %w", m.ActorIDInChannel, err)
		}
	}

	// M1.6-T5 phase-2 — resolve the per-channel template ONCE: prefer
	// the ResolveTemplate callback when wired, otherwise fall back to
	// the static AdapterActorSeeds field for legacy single-template
	// configurations and the existing test fixtures.
	tpl := TemplateView{AdapterActorSeeds: s.adapterActorSeeds}
	if s.resolveTemplate != nil {
		tpl = s.resolveTemplate(req.ChannelType)
	}

	// Step 5b — adapter actor seeds (M1.6-T2 ChannelTemplate). The
	// framework.Manager Install path will fail later if a declared
	// adapter actor is missing from actor_registry; seeding here keeps
	// channel bootstrap + adapter install atomic from the operator's
	// point of view.
	for _, seed := range tpl.AdapterActorSeeds {
		if seed.ID == "" {
			continue
		}
		rec := seed
		if rec.CreatedAt == 0 {
			rec.CreatedAt = s.nowFn()
		}
		if err := s.insertActorIfMissing(ctx, reg, rec); err != nil {
			return "", fmt.Errorf("bootstrap: insert adapter seed %s: %w", seed.ID, err)
		}
	}

	// Step 5c — template-declared workdir subdirectories (M1.6-T5
	// phase-2 — e.g. the xhs-creator template ships published-notes/,
	// drafts/, assets/). Each entry is resolved against channelDir so
	// the saga does NOT leak above the channel root. MkdirAll is
	// idempotent so re-running the saga on a partially-created channel
	// is safe.
	for _, sub := range tpl.WorkdirSubdirs {
		if sub == "" {
			continue
		}
		// Defensive: reject anything that would escape channelDir or
		// land at an unexpected absolute root. We accept "a", "a/b",
		// reject "..", "../x", "/abs", and any clean form that
		// collapses to "." or starts with "..".
		cleaned := filepath.Clean(sub)
		if filepath.IsAbs(cleaned) || cleaned == "." || cleaned == ".." ||
			strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("bootstrap: workdir subdir %q escapes channel root", sub)
		}
		target := filepath.Join(channelDir, cleaned)
		if err := os.MkdirAll(target, 0o755); err != nil {
			return "", fmt.Errorf("bootstrap: mkdir workdir subdir %s: %w", target, err)
		}
	}

	return sqlitePath, nil
}

// Complete marks a create request completed after the caller has durably
// inserted channel_lock. Keeping completion after lock insertion eliminates
// the completed-without-lock crash window while preserving Saga's ownership
// of bootstrap_registry.
func (s *Saga) Complete(ctx context.Context, createReq string) error {
	if createReq == "" {
		return errors.New("bootstrap: complete empty create_request_id")
	}
	return s.markCompleted(ctx, createReq)
}

func (s *Saga) insertActorIfMissing(ctx context.Context, reg *store.ActorRegistry, rec actor.Record) error {
	if rec.ID == "" {
		return nil
	}
	existing, ok, err := reg.Lookup(ctx, rec.ID)
	if err != nil {
		return err
	}
	if ok {
		if existing.Kind != rec.Kind || existing.Binding != rec.Binding {
			return fmt.Errorf("actor %s exists with kind=%s binding=%s, want kind=%s binding=%s",
				rec.ID, existing.Kind, existing.Binding, rec.Kind, rec.Binding)
		}
		return nil
	}
	return reg.Insert(ctx, rec)
}

func (s *Saga) insertRegistry(ctx context.Context, createReq string, channelID channel.ID, workdir string) error {
	const ins = `INSERT OR IGNORE INTO bootstrap_registry
	   (create_request_id, channel_id, status, workdir_path, started_at)
	   VALUES (?, ?, 'in_progress', ?, ?)`
	if _, err := s.daemonDB.ExecContext(ctx, ins,
		createReq, string(channelID), workdir, s.nowFn()); err != nil {
		return fmt.Errorf("bootstrap: registry insert: %w", err)
	}
	return nil
}

func (s *Saga) markCompleted(ctx context.Context, createReq string) error {
	const upd = `UPDATE bootstrap_registry
	             SET status='completed', completed_at=?
	             WHERE create_request_id=? AND status IN ('in_progress', 'completed')`
	if _, err := s.daemonDB.ExecContext(ctx, upd, s.nowFn(), createReq); err != nil {
		return fmt.Errorf("bootstrap: registry complete: %w", err)
	}
	return nil
}
