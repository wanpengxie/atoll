package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/daemonbus"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/runtime/store"
)

// ChannelBootstrapper opens a channel sqlite, runs the DDL, and seeds
// the channel_lock row. Implementations live in cmd/daemon (concrete)
// and in tests (in-memory). The bootstrap saga (T3 phase-6) plugs in
// here.
type ChannelBootstrapper interface {
	// Bootstrap creates the channel workdir + sqlite + initial rows.
	// Returns the channel sqlite path on success so the caller can
	// install lock + register actors.
	Bootstrap(ctx context.Context, channelID channel.ID, req placement.CreateChannelRequest) (string, error)
}

// CreatorConfig wires a Creator.
type CreatorConfig struct {
	DaemonID    placement.DaemonID
	DaemonEpoch placement.DaemonEpoch
	NowFn       func() int64

	// ChannelsDir is the root for per-channel workdirs (e.g.
	// ~/.coagent/channels/). One subdir per channel_id.
	ChannelsDir string

	// Bootstrapper performs the actual workdir + sqlite + member rows.
	Bootstrapper ChannelBootstrapper

	// LockOpener opens or creates the channel_lock store object for the
	// channel's sqlite. cmd/daemon supplies a function that bridges
	// from path → *sql.DB → store.NewChannelLock.
	LockOpener func(ctx context.Context, sqlitePath string) (*store.ChannelLock, error)

	// FrameIDGen produces idempotency-safe frame ids for outbound ACKs.
	FrameIDGen func() string

	// EmitAck sends the ACK back to the server. cmd/daemon plugs the
	// transit Client here.
	EmitAck func(ctx context.Context, ack placement.CreateChannelAck) error
}

// Creator handles control.create_channel frames per T1.4 step 3.
type Creator struct {
	cfg CreatorConfig
}

// NewCreator builds a Creator.
func NewCreator(cfg CreatorConfig) (*Creator, error) {
	if cfg.DaemonID == "" {
		return nil, errors.New("lifecycle: CreatorConfig.DaemonID empty")
	}
	if cfg.Bootstrapper == nil {
		return nil, errors.New("lifecycle: CreatorConfig.Bootstrapper nil")
	}
	if cfg.LockOpener == nil {
		return nil, errors.New("lifecycle: CreatorConfig.LockOpener nil")
	}
	if cfg.FrameIDGen == nil {
		return nil, errors.New("lifecycle: CreatorConfig.FrameIDGen nil")
	}
	if cfg.EmitAck == nil {
		return nil, errors.New("lifecycle: CreatorConfig.EmitAck nil")
	}
	if cfg.NowFn == nil {
		return nil, errors.New("lifecycle: CreatorConfig.NowFn nil")
	}
	if cfg.ChannelsDir == "" {
		return nil, errors.New("lifecycle: CreatorConfig.ChannelsDir empty")
	}
	return &Creator{cfg: cfg}, nil
}

// HandleCreate runs the T1.4 step-3 4-state local decision and emits
// the ACK. Caller passes the wrapper Frame (for frame_id pairing) and
// the decoded request payload.
//
// State branches:
//
//  1. local channel_lock missing → bootstrap; INSERT; ACK bound.
//  2. local row, fencing < request.token → reject (stale local).
//  3. local row, fencing == request.token, ids match → idempotent ACK.
//  4. local row, fencing > request.token → reject (newer epoch active).
func (c *Creator) HandleCreate(
	ctx context.Context,
	frame daemonbus.Frame,
	req placement.CreateChannelRequest,
) error {
	if req.ChannelID == "" {
		return errors.New("lifecycle: create channel id empty")
	}
	sqlitePath := c.channelSqlitePath(req.ChannelID)

	// Try to open existing channel_lock first (no DDL — Bootstrap creates
	// the file). If the lock row exists, decide branches 2/3/4.
	lock, openErr := c.cfg.LockOpener(ctx, sqlitePath)
	if openErr == nil {
		row, ok, err := lock.Get(ctx)
		if err != nil {
			return fmt.Errorf("lifecycle: lock get %s: %w", req.ChannelID, err)
		}
		if ok {
			switch {
			case row.FencingToken < req.FencingToken:
				// Stale local; request brings newer epoch. We honor by
				// UpgradeEpoch + ACK bound.
				okUp, err := lock.UpgradeEpoch(ctx,
					req.FencingToken, req.OwnerEpoch, c.cfg.DaemonID, c.cfg.DaemonEpoch,
					c.cfg.NowFn())
				if err != nil {
					return fmt.Errorf("lifecycle: lock upgrade: %w", err)
				}
				if !okUp {
					// Another concurrent ACK already advanced.
					return c.emit(ctx, frame, req, placement.AckRejected, "upgrade lost CAS")
				}
				return c.emit(ctx, frame, req, placement.AckBound, "")
			case row.FencingToken == req.FencingToken:
				// Idempotent — ACK bound with current ids.
				return c.emit(ctx, frame, req, placement.AckBound, "")
			case row.FencingToken > req.FencingToken:
				// Newer local epoch already exists — server's request is stale.
				return c.emit(ctx, frame, req, placement.AckRejected, "stale_request")
			}
		}
		// Lock file exists but row absent — treat as fresh bootstrap.
	}

	// branch 1: bootstrap
	if _, err := c.cfg.Bootstrapper.Bootstrap(ctx, req.ChannelID, req); err != nil {
		return c.emit(ctx, frame, req, placement.AckRejected, fmt.Sprintf("bootstrap: %v", err))
	}
	// Open lock store (Bootstrap created the sqlite file with DDL).
	lock, err := c.cfg.LockOpener(ctx, sqlitePath)
	if err != nil {
		return c.emit(ctx, frame, req, placement.AckRejected, fmt.Sprintf("lock open: %v", err))
	}
	now := c.cfg.NowFn()
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    req.ChannelID,
		FencingToken: req.FencingToken,
		OwnerEpoch:   req.OwnerEpoch,
		DaemonID:     c.cfg.DaemonID,
		DaemonEpoch:  c.cfg.DaemonEpoch,
		AcquiredAt:   now,
		RefreshedAt:  now,
	}); err != nil {
		return c.emit(ctx, frame, req, placement.AckRejected, fmt.Sprintf("lock insert: %v", err))
	}
	return c.emit(ctx, frame, req, placement.AckBound, "")
}

func (c *Creator) emit(
	ctx context.Context,
	frame daemonbus.Frame,
	req placement.CreateChannelRequest,
	status placement.AckStatus,
	reason string,
) error {
	ack := placement.CreateChannelAck{
		FrameID:         frame.FrameID,
		ChannelID:       req.ChannelID,
		CreateRequestID: req.CreateRequestID,
		OwnerEpoch:      req.OwnerEpoch,
		FencingToken:    req.FencingToken,
		DaemonID:        c.cfg.DaemonID,
		DaemonEpoch:     c.cfg.DaemonEpoch,
		Status:          status,
		Reason:          reason,
	}
	return c.cfg.EmitAck(ctx, ack)
}

// channelSqlitePath maps a channel id to its sqlite file path.
func (c *Creator) channelSqlitePath(id channel.ID) string {
	return filepath.Join(c.cfg.ChannelsDir, string(id), "channel.sqlite")
}
