package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/store"
)

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
//     5b. Insert AdapterActorSeeds from the daemon-level ChannelTemplate
//     (M1.6-T2 — pre-creates tool adapter rows so framework.Manager.Install
//     can locate them).
//  6. (caller — runtime/lifecycle.Creator) writes channel_lock row.
//  7. Mark bootstrap_registry status='completed'.
//
// On failure between steps 2 and 7 the row is left status='in_progress'
// so reconcile.go can roll it back on next start.
type Saga struct {
	daemonDB          *sql.DB
	channelsDir       string
	nowFn             func() int64
	adapterActorSeeds []actor.Record
}

// SagaConfig wires Saga.
type SagaConfig struct {
	DaemonDB    *sql.DB
	ChannelsDir string
	NowFn       func() int64

	// AdapterActorSeeds is the static set of tool actor_registry rows
	// the saga MUST insert in addition to the system + initial member
	// rows (M1.6-T2 ChannelTemplate). Empty list = no extra actors.
	AdapterActorSeeds []actor.Record
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
	if err := reg.Insert(ctx, actor.Record{
		ID:        actor.SystemActorID,
		Kind:      message.SenderSystem,
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
		kind := message.SenderHuman
		if m.Kind != "" {
			kind = message.SenderKind(m.Kind)
		}
		if err := reg.Insert(ctx, actor.Record{
			ID:          actor.ActorID(m.ActorIDInChannel),
			Kind:        kind,
			DisplayName: m.DisplayName,
			CreatedAt:   s.nowFn(),
		}); err != nil {
			return "", fmt.Errorf("bootstrap: insert member %s: %w", m.ActorIDInChannel, err)
		}
	}

	// Step 5b — adapter actor seeds (M1.6-T2 ChannelTemplate). The
	// framework.Manager Install path will fail later if a declared
	// adapter actor is missing from actor_registry; seeding here keeps
	// channel bootstrap + adapter install atomic from the operator's
	// point of view.
	for _, seed := range s.adapterActorSeeds {
		if seed.ID == "" {
			continue
		}
		rec := seed
		if rec.CreatedAt == 0 {
			rec.CreatedAt = s.nowFn()
		}
		if err := reg.Insert(ctx, rec); err != nil {
			return "", fmt.Errorf("bootstrap: insert adapter seed %s: %w", seed.ID, err)
		}
	}

	// Step 7 — mark completed (step 6 happens in lifecycle.Creator).
	if err := s.markCompleted(ctx, createReq); err != nil {
		return "", err
	}
	return sqlitePath, nil
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
	             WHERE create_request_id=? AND status='in_progress'`
	if _, err := s.daemonDB.ExecContext(ctx, upd, s.nowFn(), createReq); err != nil {
		return fmt.Errorf("bootstrap: registry complete: %w", err)
	}
	return nil
}
