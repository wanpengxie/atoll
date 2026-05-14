package supervisor

import (
	"context"
	"errors"
	"time"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/placement"
)

// HeartbeatEvery is the worker→daemon liveness cadence (codex review #10
// — distinct from LeaseTTL = 5min which lives in runtime/workerhost).
const HeartbeatEvery = 30 * time.Second

// LeaseInfo is the supervisor's view of one active lease.
type LeaseInfo struct {
	ID            string
	ChannelID     channel.ID
	AgentID       string
	WorkerID      string
	FencingToken  placement.FencingToken
	DaemonEpoch   placement.DaemonEpoch
	AcquiredAt    int64
	LastHeartbeat int64
}

// LeaseManager is a thin facade over the workerhost lease primitives —
// adds a heartbeat tracker so supervisor.Loop can detect a worker that
// stops talking back to its daemon.
//
// The actual sqlite-backed Acquire / Release lives in runtime/workerhost.
// supervisor.LeaseManager keeps in-memory bookkeeping so the daemon
// scheduler can answer "is this agent currently being worked?" without
// hitting sqlite for every event.
type LeaseManager struct {
	store    LeaseStorer
	memTable LeaseTable
	nowFn    func() int64
}

// LeaseStorer abstracts the workerhost lease persistence layer.
type LeaseStorer interface {
	Acquire(ctx context.Context, agentID, workerID string,
		fencing placement.FencingToken, daemonEpoch placement.DaemonEpoch,
		now int64) (info LeaseInfo, ok bool, err error)
	Release(ctx context.Context, agentID string) error
}

// LeaseTable abstracts in-memory lease tracking.
type LeaseTable interface {
	Put(info LeaseInfo)
	Get(id string) (LeaseInfo, bool)
	Delete(id string)
	List() []LeaseInfo
}

// LeaseManagerConfig wires a LeaseManager.
type LeaseManagerConfig struct {
	Store LeaseStorer
	Table LeaseTable
	NowFn func() int64
}

// NewLeaseManager builds a LeaseManager.
func NewLeaseManager(cfg LeaseManagerConfig) (*LeaseManager, error) {
	if cfg.Store == nil || cfg.Table == nil || cfg.NowFn == nil {
		return nil, errors.New("supervisor: LeaseManagerConfig incomplete")
	}
	return &LeaseManager{store: cfg.Store, memTable: cfg.Table, nowFn: cfg.NowFn}, nil
}

// Acquire wraps the storer + memtable. Returns (info, true) on success.
func (m *LeaseManager) Acquire(
	ctx context.Context,
	channelID channel.ID,
	agentID, workerID string,
	fencing placement.FencingToken,
	daemonEpoch placement.DaemonEpoch,
) (LeaseInfo, bool, error) {
	info, ok, err := m.store.Acquire(ctx, agentID, workerID, fencing, daemonEpoch, m.nowFn())
	if err != nil || !ok {
		return LeaseInfo{}, false, err
	}
	info.ID = agentID
	info.ChannelID = channelID
	info.AgentID = agentID
	info.LastHeartbeat = m.nowFn()
	m.memTable.Put(info)
	return info, true, nil
}

// Release frees the lease.
func (m *LeaseManager) Release(ctx context.Context, agentID string) error {
	m.memTable.Delete(agentID)
	return m.store.Release(ctx, agentID)
}

// Heartbeat updates LastHeartbeat for the lease.
func (m *LeaseManager) Heartbeat(agentID string, atMs int64) {
	if info, ok := m.memTable.Get(agentID); ok {
		info.LastHeartbeat = atMs
		m.memTable.Put(info)
	}
}

// Snapshot returns a copy of all currently active leases.
func (m *LeaseManager) Snapshot() []LeaseInfo { return m.memTable.List() }
