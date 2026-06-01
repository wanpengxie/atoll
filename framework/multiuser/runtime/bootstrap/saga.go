package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
	multistore "github.com/wanpengxie/ActOS/framework/multiuser/runtime/store"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	kfencing "github.com/wanpengxie/ActOS/kernel/fencing"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/store"
)

type Phase string

const (
	PhaseSent            Phase = "sent"
	PhaseAwaitingAck     Phase = "awaiting_ack"
	PhasePartialTakeover Phase = "partial_takeover"
	PhaseCompleted       Phase = "completed"
	PhaseAbandoned       Phase = "abandoned"
)

// TemplateView is the bootstrap-side projection of one L4 channel
// template. The Saga consumes it inside Bootstrap to
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
	AdapterActorSeeds []actorreg.Record

	// WorkdirSubdirs lists relative directory paths the saga MUST
	// mkdir under <ChannelsDir>/<channelID>/ during step 5c (after the
	// channel sqlite is open / actor seeds are inserted). Each entry is
	// a path component (e.g. "published-notes", "drafts", "assets")
	// joined with channelDir before MkdirAll. Empty list = no extra
	// subdirs (legacy / generic channels).
	WorkdirSubdirs []string
}

// Saga is the daemon-side channel bootstrap orchestrator (L2 §3.6
// bootstrap_registry 9-step). For launch-T3 we ship a lean variant
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
//     come from the resolved TemplateView for req.ChannelType.
//     5c. Mkdir template WorkdirSubdirs (M1.6-T5 phase-2 — e.g.
//     published-notes/, drafts/, assets/ for the xhs-creator template).
//  6. Caller writes channel_lock row.
//  7. Caller invokes Complete to mark bootstrap_registry status='completed'
//     only after channel_lock is durable.
//
// On failure between steps 2 and 7 the row is left status='in_progress'
// so reconcile.go can roll it back on next start.
type Saga struct {
	daemonDB        *sql.DB
	channelsDir     string
	nowFn           func() int64
	resolveTemplate func(channelType string) TemplateView
}

// SagaConfig wires Saga.
type SagaConfig struct {
	DaemonDB    *sql.DB
	ChannelsDir string
	NowFn       func() int64

	// ResolveTemplate, when non-nil, is consulted in Bootstrap to obtain
	// the per-channel TemplateView keyed by CreateChannelRequest.ChannelType.
	// When nil the saga seeds only system + initial members.
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
	return &Saga{
		daemonDB:        cfg.DaemonDB,
		channelsDir:     cfg.ChannelsDir,
		nowFn:           cfg.NowFn,
		resolveTemplate: cfg.ResolveTemplate,
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

	// Step 3 — open channel sqlite (DDL runs on first open). Actor rows
	// (system / initial members / adapter seeds) are NOT inserted here:
	// they are registered together with their system.actor.registered fact
	// by SeedActors, which the daemon invokes once the channel_lock fencing
	// tuple exists. Opening the DB here keeps DDL creation in Bootstrap so
	// the lock store the daemon opens next finds the schema in place.
	channelDB, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{})
	if err != nil {
		return "", fmt.Errorf("bootstrap: open sqlite %s: %w", sqlitePath, err)
	}
	_ = channelDB.Close()

	// Resolve the per-channel template once. A nil resolver means generic
	// channels with no extra actors or workdir subdirectories.
	tpl := TemplateView{}
	if s.resolveTemplate != nil {
		tpl = s.resolveTemplate(req.ChannelType)
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

// SeedActors registers the channel's initial actors (system actor, initial
// members, template adapter seeds) AND appends their system.actor.registered
// facts in one fenced, idempotent path (推论5 / §4 事实完整性). It runs after
// the daemon has written channel_lock — the fencing tuple it requires — so the
// fact append and the actor_registry row land in the same transaction via
// ApplyMemberTransitions. There is no separate "write row, then backfill fact"
// step: registration and fact are a single source of truth.
//
// Idempotent on every entry path: ApplyMemberTransitions treats an already-active
// row as a no-op (no second fact), so a create retry / crash-replay that re-runs
// SeedActors converges without duplicating facts.
func (s *Saga) SeedActors(
	ctx context.Context,
	channelID channel.ID,
	req placement.CreateChannelRequest,
	fencing klog.FencingTuple,
) error {
	sqlitePath := filepath.Join(s.channelsDir, string(channelID), "channel.sqlite")
	channelDB, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return fmt.Errorf("bootstrap: seed actors open sqlite %s: %w", sqlitePath, err)
	}
	defer func() { _ = channelDB.Close() }()
	if err := multistore.EnsureChannelTables(ctx, channelDB); err != nil {
		return err
	}
	lock := multistore.NewChannelLock(channelDB)
	outbox := multistore.NewViewSyncOutbox(channelDB, channelID)
	fence := store.WriteFenceFunc(func(ctx context.Context, tx *sql.Tx, token kfencing.FencingToken, epoch kfencing.DaemonEpoch) error {
		return lock.ValidateWriteTx(ctx, tx, token, epoch)
	})
	observer := store.AppendObserverFuncs{
		Wait: func(ctx context.Context) error {
			return outbox.WaitForAdmission(ctx)
		},
		Enqueue: func(ctx context.Context, tx *sql.Tx, env *message.Envelope, seq int64) error {
			return outbox.EnqueueAppendTx(ctx, tx, env, seq)
		},
	}
	reg := store.NewActorRegistryWithObservers(channelDB, fence, observer)

	now := s.nowFn()
	adds := make([]store.MemberActorAdd, 0, len(req.InitialMembers)+4)

	// System actor.
	adds = append(adds, store.MemberActorAdd{
		ID:   actor.SystemActorID,
		Kind: actor.KindSystem,
		At:   now,
	})

	// Initial members.
	for _, m := range req.InitialMembers {
		if m.MemberActorID == "" {
			continue
		}
		kind := actor.KindHuman
		if m.Kind != "" {
			kind = m.Kind
		}
		adds = append(adds, store.MemberActorAdd{
			ID:          m.MemberActorID,
			Kind:        kind,
			DisplayName: m.DisplayName,
			At:          now,
		})
	}

	// Template adapter actor seeds. framework.Manager.Install fails later if
	// a declared adapter actor is missing from actor_registry; registering
	// here keeps channel bootstrap + adapter install atomic.
	tpl := TemplateView{}
	if s.resolveTemplate != nil {
		tpl = s.resolveTemplate(req.ChannelType)
	}
	for _, seed := range tpl.AdapterActorSeeds {
		if seed.ID == "" {
			continue
		}
		adds = append(adds, store.MemberActorAdd{
			ID:          seed.ID,
			Kind:        seed.Kind,
			Binding:     seed.Binding,
			DisplayName: seed.DisplayName,
			At:          now,
		})
	}

	if err := reg.ApplyMemberTransitions(ctx, channelID, adds, nil, fencing); err != nil {
		return fmt.Errorf("bootstrap: seed actors %s: %w", channelID, err)
	}
	return nil
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

func (s *Saga) MarkPhase(ctx context.Context, createReq string, phase Phase) error {
	if createReq == "" {
		return errors.New("bootstrap: phase empty create_request_id")
	}
	if phase == "" {
		return errors.New("bootstrap: phase empty")
	}
	const upd = `UPDATE bootstrap_registry
	             SET phase=?, last_attempt_at=?
	             WHERE create_request_id=? AND status='in_progress'`
	if _, err := s.daemonDB.ExecContext(ctx, upd, string(phase), s.nowFn(), createReq); err != nil {
		return fmt.Errorf("bootstrap: registry phase: %w", err)
	}
	return nil
}

func (s *Saga) insertRegistry(ctx context.Context, createReq string, channelID channel.ID, workdir string) error {
	now := s.nowFn()
	const ins = `INSERT OR IGNORE INTO bootstrap_registry
	   (create_request_id, channel_id, status, phase, workdir_path, sent_at,
	    expected_ack_frame_kind, attempt_count, last_attempt_at, started_at)
	   VALUES (?, ?, 'in_progress', 'sent', ?, ?, 'control.create_channel_ack', 1, ?, ?)`
	if _, err := s.daemonDB.ExecContext(ctx, ins,
		createReq, string(channelID), workdir, now, now, now); err != nil {
		return fmt.Errorf("bootstrap: registry insert: %w", err)
	}
	return nil
}

func (s *Saga) markCompleted(ctx context.Context, createReq string) error {
	const upd = `UPDATE bootstrap_registry
		             SET status='completed',
		                 phase='completed',
		                 terminal_status='accepted',
		                 abandonment_reason='',
		                 completed_at=?
		             WHERE create_request_id=? AND status IN ('in_progress', 'completed')`
	if _, err := s.daemonDB.ExecContext(ctx, upd, s.nowFn(), createReq); err != nil {
		return fmt.Errorf("bootstrap: registry complete: %w", err)
	}
	return nil
}
