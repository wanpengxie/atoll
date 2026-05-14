package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// Phase enumerates the T1.6 startup-phase barriers.
type Phase int

const (
	PhaseUnstarted    Phase = iota
	PhaseLoadingLocal       // 1: scan channels/ + open _lock rows
	PhaseReclaiming         // 2: connect server + report owned channels
	PhaseRecovering         // 3: spawn outbox push + scheduler + adapters
	PhaseAcceptingNew       // 4: accept new control.create_channel frames
)

// String returns the phase label for telemetry.
func (p Phase) String() string {
	switch p {
	case PhaseUnstarted:
		return "unstarted"
	case PhaseLoadingLocal:
		return "phase1_loading_local"
	case PhaseReclaiming:
		return "phase2_reclaiming"
	case PhaseRecovering:
		return "phase3_recovering"
	case PhaseAcceptingNew:
		return "phase4_accepting_new"
	default:
		return "unknown"
	}
}

// LocalChannel summarizes what phase 1 found for one channel on disk.
type LocalChannel struct {
	ChannelID  channel.ID
	SQLitePath string
	Lock       store.ChannelLockRow
	OwnedByUs  bool // true iff Lock.DaemonID matches current daemon
}

// BootResult is the outcome of a Boot run.
type BootResult struct {
	Local           []LocalChannel
	ReclaimAccepted []channel.ID
	ReclaimRejected []channel.ID
}

// BootConfig wires Boot.
type BootConfig struct {
	DaemonID    placement.DaemonID
	DaemonEpoch placement.DaemonEpoch
	NowFn       func() int64

	ChannelsDir string

	// LockOpener opens the channel_lock store object for a channel sqlite
	// path. Provided by cmd/daemon (bridges path → *sql.DB).
	LockOpener func(ctx context.Context, sqlitePath string) (*store.ChannelLock, error)

	// EmitReclaim sends a control.daemon_reclaim frame to the server in
	// phase 2. Returns the per-channel decisions. cmd/daemon supplies an
	// impl that wraps the transit client. May be nil in tests — the
	// caller will then drive the phase manually.
	EmitReclaim func(ctx context.Context, req placement.ReclaimRequest) ([]placement.ReclaimDecision, error)
}

// Bootstrapper runs the T1.6 phase 1/2/3/4 startup sequencer.
type Bootstrapper struct {
	cfg      BootConfig
	phase    Phase
	channels []LocalChannel
}

// NewBootstrapper builds a Bootstrapper.
func NewBootstrapper(cfg BootConfig) (*Bootstrapper, error) {
	if cfg.DaemonID == "" {
		return nil, errors.New("lifecycle: BootConfig.DaemonID empty")
	}
	if cfg.LockOpener == nil {
		return nil, errors.New("lifecycle: BootConfig.LockOpener nil")
	}
	if cfg.ChannelsDir == "" {
		return nil, errors.New("lifecycle: BootConfig.ChannelsDir empty")
	}
	if cfg.NowFn == nil {
		return nil, errors.New("lifecycle: BootConfig.NowFn nil")
	}
	return &Bootstrapper{cfg: cfg, phase: PhaseUnstarted}, nil
}

// Phase returns the current phase (useful for tests / observability).
func (b *Bootstrapper) Phase() Phase { return b.phase }

// LoadLocal runs T1.6 phase 1: scan ChannelsDir, open every channel
// sqlite, read its channel_lock row. Updates daemon_epoch in each lock
// (RefreshDaemon) so any stale worker IPC fails fence_check.
//
// Returns the list of local channels seen.
func (b *Bootstrapper) LoadLocal(ctx context.Context) ([]LocalChannel, error) {
	b.phase = PhaseLoadingLocal
	entries, err := os.ReadDir(b.cfg.ChannelsDir)
	if err != nil {
		if os.IsNotExist(err) {
			b.channels = nil
			return nil, nil
		}
		return nil, fmt.Errorf("lifecycle: read channels dir: %w", err)
	}

	out := make([]LocalChannel, 0, len(entries))
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		channelID := channel.ID(ent.Name())
		sqlitePath := filepath.Join(b.cfg.ChannelsDir, ent.Name(), "channel.sqlite")
		if _, err := os.Stat(sqlitePath); err != nil {
			continue
		}
		lock, err := b.cfg.LockOpener(ctx, sqlitePath)
		if err != nil {
			return nil, fmt.Errorf("lifecycle: open lock for %s: %w", channelID, err)
		}
		row, ok, err := lock.Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("lifecycle: read lock for %s: %w", channelID, err)
		}
		if !ok {
			// channel sqlite exists but no lock — unbound; skip.
			continue
		}
		owned := row.DaemonID == b.cfg.DaemonID
		// Refresh daemon_epoch regardless: if we owned this channel
		// before, the bump invalidates stale workers; if we didn't, the
		// bump is harmless and gets overwritten by next reclaim.
		if owned {
			if err := lock.RefreshDaemon(ctx, b.cfg.DaemonEpoch, b.cfg.NowFn()); err != nil {
				return nil, fmt.Errorf("lifecycle: refresh daemon_epoch %s: %w", channelID, err)
			}
			row.DaemonEpoch = b.cfg.DaemonEpoch
		}
		out = append(out, LocalChannel{
			ChannelID:  channelID,
			SQLitePath: sqlitePath,
			Lock:       row,
			OwnedByUs:  owned,
		})
	}
	b.channels = out
	return out, nil
}

// ReportReclaim runs phase 2: send control.daemon_reclaim with every
// channel we believe we own, and apply the per-channel decisions.
//
// When BootConfig.EmitReclaim is nil this is a no-op (tests).
func (b *Bootstrapper) ReportReclaim(ctx context.Context) (BootResult, error) {
	b.phase = PhaseReclaiming
	res := BootResult{Local: b.channels}
	if b.cfg.EmitReclaim == nil {
		// All local channels are assumed owned for offline tests.
		for _, c := range b.channels {
			if c.OwnedByUs {
				res.ReclaimAccepted = append(res.ReclaimAccepted, c.ChannelID)
			}
		}
		return res, nil
	}

	req := placement.ReclaimRequest{
		DaemonID:    b.cfg.DaemonID,
		DaemonEpoch: b.cfg.DaemonEpoch,
	}
	for _, c := range b.channels {
		if !c.OwnedByUs {
			continue
		}
		req.Channels = append(req.Channels, placement.ReclaimChannel{
			ChannelID:    c.ChannelID,
			FencingToken: c.Lock.FencingToken,
			OwnerEpoch:   c.Lock.OwnerEpoch,
		})
	}
	if len(req.Channels) == 0 {
		return res, nil
	}
	decisions, err := b.cfg.EmitReclaim(ctx, req)
	if err != nil {
		return res, fmt.Errorf("lifecycle: emit reclaim: %w", err)
	}
	for _, d := range decisions {
		if d.Accepted {
			res.ReclaimAccepted = append(res.ReclaimAccepted, d.ChannelID)
		} else {
			res.ReclaimRejected = append(res.ReclaimRejected, d.ChannelID)
		}
	}
	return res, nil
}

// MarkRecovering moves the phase forward (phase 3). The actual
// recover-side work (spawn outbox push, scheduler, adapter manager) is
// driven by cmd/daemon — this method just records the transition.
func (b *Bootstrapper) MarkRecovering() { b.phase = PhaseRecovering }

// MarkAcceptingNew is phase 4 — daemon now processes new create_channel.
func (b *Bootstrapper) MarkAcceptingNew() { b.phase = PhaseAcceptingNew }
