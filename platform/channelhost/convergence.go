package channelhost

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	defaultFullScan  = 30 * time.Second
	defaultEdgeDelay = 250 * time.Millisecond
)

// RegistryReader is the desired-state face ChannelHost reconciles against.
// It deliberately exposes values only; the host cannot mutate registry truth.
type RegistryReader interface {
	ListChannels(context.Context) ([]lagoon.ChannelRow, error)
	GetChannelDesired(context.Context, channel.ID) (lagoon.ChannelRow, bool, error)
}

type convergenceState struct {
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	wake      chan channel.ID
	fullScan  time.Duration
	edgeDelay time.Duration

	mu      sync.Mutex
	started bool
	stopped bool
	stop    sync.Once

	retryMu sync.Mutex
	retries map[channel.ID]retryState
}

type retryState struct {
	fingerprint string
	failures    int
	next        time.Time
	permanent   bool
}

func newConvergenceState() *convergenceState {
	ctx, cancel := context.WithCancel(context.Background())
	return &convergenceState{
		ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan channel.ID, 64),
		fullScan: defaultFullScan, edgeDelay: defaultEdgeDelay, retries: make(map[channel.ID]retryState),
	}
}

// StartConvergence starts ChannelHost's own fast-edge and authoritative slow
// scan. It performs the boot scan before returning.
func (h *ChannelHost) StartConvergence() error {
	c := h.convergence
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return errors.New("channelhost: convergence stopped")
	}
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true
	c.mu.Unlock()
	go h.convergenceLoop()
	return h.reconcileAll(c.ctx)
}

func (h *ChannelHost) stopConvergence() {
	c := h.convergence
	if c == nil {
		return
	}
	c.stop.Do(func() {
		c.mu.Lock()
		c.stopped = true
		started := c.started
		c.mu.Unlock()
		c.cancel()
		if started {
			<-c.done
		} else {
			close(c.done)
		}
	})
}

// RegistryChanged is the storage module's post-commit edge. The value carries
// no command or row contents. Every edge is queued for the 250ms fast sweep,
// while the 30s scan remains authoritative.
func (h *ChannelHost) RegistryChanged(change lagoon.Change) {
	c := h.convergence
	if c == nil {
		return
	}
	id := change.ChannelID
	if change.AllChannels {
		id = ""
	}
	select {
	case c.wake <- id:
	default:
	}
}

func (h *ChannelHost) convergenceLoop() {
	c := h.convergence
	defer close(c.done)
	fullTicker := time.NewTicker(c.fullScan)
	retryTicker := time.NewTicker(c.edgeDelay)
	defer fullTicker.Stop()
	defer retryTicker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-fullTicker.C:
			if err := h.reconcileAll(c.ctx); err != nil {
				h.logger.Warn("channelhost full reconcile failed", "err", err)
			}
		case <-retryTicker.C:
			h.retryDue()
		case id := <-c.wake:
			timer := time.NewTimer(c.edgeDelay)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if id == "" {
				if err := h.reconcileAll(c.ctx); err != nil {
					h.logger.Warn("channelhost edge full reconcile failed", "err", err)
				}
			} else if err := h.reconcileID(c.ctx, id); err != nil {
				h.logger.Warn("channelhost edge reconcile failed", "channel", id, "err", err)
			}
		}
	}
}

func (h *ChannelHost) reconcileAll(ctx context.Context) error {
	rows, err := h.registry.ListChannels(ctx)
	if err != nil {
		return err
	}
	known := make(map[channel.ID]lagoon.ChannelRow, len(rows))
	for _, row := range rows {
		known[row.ID] = row
		if err := h.reconcileTracked(ctx, row); err != nil {
			h.logger.Warn("channelhost reconcile failed", "channel", row.ID, "err", err)
		}
	}
	physical, err := h.Census(ctx)
	if err != nil {
		return err
	}
	for _, entry := range physical {
		if entry.ChannelID == protocol.C0ChannelID {
			continue
		}
		if _, ok := known[entry.ChannelID]; !ok {
			if err := h.Destroy(ctx, entry.ChannelID); err != nil {
				h.logger.Warn("channelhost orphan destroy failed", "channel", entry.ChannelID, "err", err)
			}
		}
	}
	return nil
}

func (h *ChannelHost) reconcileID(ctx context.Context, id channel.ID) error {
	row, ok, err := h.registry.GetChannelDesired(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		if id == protocol.C0ChannelID {
			return nil
		}
		return h.Destroy(ctx, id)
	}
	return h.reconcileTracked(ctx, row)
}

func (h *ChannelHost) reconcileTracked(ctx context.Context, row lagoon.ChannelRow) error {
	c := h.convergence
	fingerprint := string(row.Status) + "\x00" + string(row.ParentID) + "\x00" + row.Type + "\x00" + row.OwnerPrincipal + "\x00" + string(row.Spec)
	now := time.Now()
	c.retryMu.Lock()
	state, tracked := c.retries[row.ID]
	if tracked && state.fingerprint != fingerprint {
		delete(c.retries, row.ID)
		tracked = false
	}
	if tracked && (state.permanent || now.Before(state.next)) {
		c.retryMu.Unlock()
		return nil
	}
	c.retryMu.Unlock()

	err := h.reconcileRow(ctx, row)
	c.retryMu.Lock()
	defer c.retryMu.Unlock()
	if err == nil {
		delete(c.retries, row.ID)
		return nil
	}
	state = c.retries[row.ID]
	state.fingerprint = fingerprint
	state.failures++
	state.permanent = permanentConvergenceError(err)
	if !state.permanent {
		exponent := math.Min(float64(state.failures-1), 7)
		state.next = now.Add(time.Duration(math.Pow(2, exponent)) * c.edgeDelay)
	}
	c.retries[row.ID] = state
	return err
}

func (h *ChannelHost) retryDue() {
	c := h.convergence
	now := time.Now()
	c.retryMu.Lock()
	ids := make([]channel.ID, 0, len(c.retries))
	for id, state := range c.retries {
		if !state.permanent && !now.Before(state.next) {
			ids = append(ids, id)
		}
	}
	c.retryMu.Unlock()
	for _, id := range ids {
		if err := h.reconcileID(c.ctx, id); err != nil && c.ctx.Err() == nil {
			h.logger.Warn("channelhost retry reconcile failed", "channel", id, "err", err)
		}
	}
}

func permanentConvergenceError(err error) bool {
	return errors.Is(err, ErrSchemaIncompatible) || errors.Is(err, ErrOwnerInvariant) || errors.Is(err, ErrInvalidChannelID) || errors.Is(err, ErrChannelRetired)
}

func (h *ChannelHost) reconcileRow(ctx context.Context, row lagoon.ChannelRow) error {
	if row.Status == lagoon.ChannelRetired {
		if row.ID == protocol.C0ChannelID {
			return errors.Join(ErrChannelRetired, errors.New("channelhost: c0 cannot be retired"))
		}
		return h.Destroy(ctx, row.ID)
	}
	if _, ok := h.Acquire(row.ID); ok {
		// Registry changes can affect declaration overlays and device binding
		// without changing channel genesis; wake the already-open Home too.
		h.Poke(row.ID)
		return nil
	}
	err := h.Open(ctx, OpenSpec{ChannelID: row.ID, ExpectedType: row.Type})
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrChannelNotFound) {
		return err
	}
	if row.ID == protocol.C0ChannelID {
		return err
	}
	spec, err := provisionFromRow(row)
	if err != nil {
		return err
	}
	if _, err := h.Provision(ctx, spec); err != nil && !errors.Is(err, ErrServing) {
		return err
	}
	return h.Open(ctx, OpenSpec{ChannelID: row.ID, ExpectedType: row.Type})
}

func provisionFromRow(row lagoon.ChannelRow) (ProvisionSpec, error) {
	var desired lagoon.GenesisSpec
	if err := json.Unmarshal(row.Spec, &desired); err != nil {
		return ProvisionSpec{}, errors.Join(ErrSchemaIncompatible, err)
	}
	if desired.ChannelID != row.ID || desired.Type != row.Type || desired.OwnerPrincipal != row.OwnerPrincipal || desired.CreatedAt != row.CreatedAt || desired.ParentID != row.ParentID {
		return ProvisionSpec{}, errors.Join(ErrSchemaIncompatible, errors.New("channelhost: registry row and genesis spec disagree"))
	}
	spec := ProvisionSpec{ChannelID: desired.ChannelID, Type: desired.Type, OwnerPrincipal: desired.OwnerPrincipal, CreatedAt: desired.CreatedAt}
	if desired.ParentID != "" {
		spec.Origin = &Origin{ParentChannelID: desired.ParentID, InitiatorPrincipal: desired.InitiatorPrincipal}
	}
	for _, decl := range desired.Declarations {
		spec.GenesisDeclarations = append(spec.GenesisDeclarations, GenesisDeclaration{DeclID: decl.DeclID, Kind: decl.Kind, Rendered: decl.Rendered})
	}
	return spec, nil
}
