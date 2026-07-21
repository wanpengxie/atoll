package channelhost

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const maxEncodedIDBytes = 180

var (
	ErrClosed             = errors.New("channelhost: closed")
	ErrServing            = errors.New("channelhost: channel already serving")
	ErrInvalidChannelID   = errors.New("channelhost: invalid channel id")
	ErrChannelRetired     = errors.New("channelhost: channel retired")
	ErrTombstoneExists    = errors.New("channelhost: tombstone already exists")
	ErrChannelNotFound    = errors.New("channelhost: channel not found")
	ErrSchemaIncompatible = errors.New("channelhost: schema incompatible")
	ErrOwnerInvariant     = errors.New("channelhost: owner invariant")
)

type Service interface {
	Provision(context.Context, ProvisionSpec) (ProvisionReceipt, error)
	Destroy(context.Context, channel.ID) error
	Open(context.Context, OpenSpec) error
	Census(context.Context) ([]CensusEntry, error)
	Close() error
}

type LocalHost interface {
	Service
	Acquire(channel.ID) (Bundle, bool)
}

type OpenSpec struct {
	ChannelID    channel.ID
	ExpectedType string
}

type Origin struct {
	ParentChannelID    channel.ID `json:"parent_channel_id"`
	InitiatorPrincipal string     `json:"initiator_principal"`
}

type GenesisDeclaration struct {
	DeclID    string                   `json:"decl_id"`
	Principal string                   `json:"principal"`
	Kind      actor.Kind               `json:"kind"`
	Rendered  channel.RenderedSnapshot `json:"rendered_snapshot"`
}

type ProvisionSpec struct {
	ChannelID           channel.ID           `json:"channel_id"`
	Type                string               `json:"type"`
	OwnerPrincipal      string               `json:"owner_principal"`
	GenesisDeclarations []GenesisDeclaration `json:"genesis_declarations"`
	DefaultSourceDeclID string               `json:"default_source_decl_id"`
	CreatedAt           int64                `json:"created_at"`
	Origin              *Origin              `json:"origin,omitempty"`
}

type ProvisionReceipt struct {
	ChannelID channel.ID `json:"channel_id"`
	CreatedAt int64      `json:"created_at"`
}

type CensusState string

const (
	CensusOpen    CensusState = "open"
	CensusPresent CensusState = "present"
)

type CensusEntry struct {
	ChannelID channel.ID
	State     CensusState
}

// HomeDeps contains only realm-owned resolution and notification seams. Channel
// execution, binding admission, and planning stay behind the membrane.
type HomeDeps struct {
	CompositionResolver  home.CompositionResolver
	IntroductionResolver home.IntroductionResolver
	OnMembershipChange   func(channel.ID, []string)
	Logger               *slog.Logger
}

type entryState uint8

const (
	stateServing entryState = iota + 1
	stateSealing
	stateSealed
)

type entry struct {
	home       *home.Home
	sysOp      SysOp
	generation uint64
	state      entryState
	closed     bool
}

type ChannelHost struct {
	root   string
	deps   HomeDeps
	logger *slog.Logger

	mu      sync.RWMutex
	closed  bool
	entries map[channel.ID]*entry
	locks   map[channel.ID]*sync.Mutex
	nextGen atomic.Uint64
}

var _ LocalHost = (*ChannelHost)(nil)

func New(root string, deps HomeDeps) (*ChannelHost, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("channelhost: root required")
	}
	if deps.CompositionResolver == nil {
		return nil, errors.New("channelhost: CompositionResolver required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("channelhost: create root: %w", err)
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ChannelHost{root: root, deps: deps, logger: logger, entries: make(map[channel.ID]*entry), locks: make(map[channel.ID]*sync.Mutex)}, nil
}

func encodeID(id channel.ID) (string, error) {
	if id == "" {
		return "", ErrInvalidChannelID
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte(id))
	if len(encoded) == 0 || len(encoded) > maxEncodedIDBytes {
		return "", ErrInvalidChannelID
	}
	return encoded, nil
}

func decodeID(encoded string) (channel.ID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) == 0 {
		return "", ErrInvalidChannelID
	}
	if base64.RawURLEncoding.EncodeToString(raw) != encoded {
		return "", ErrInvalidChannelID
	}
	return channel.ID(raw), nil
}

func (h *ChannelHost) paths(id channel.ID) (main, tombstone string, err error) {
	encoded, err := encodeID(id)
	if err != nil {
		return "", "", err
	}
	main = filepath.Join(h.root, encoded+".db")
	return main, main + ".tombstone", nil
}

func (h *ChannelHost) idLock(id channel.ID) *sync.Mutex {
	h.mu.Lock()
	defer h.mu.Unlock()
	lock := h.locks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		h.locks[id] = lock
	}
	return lock
}

func (h *ChannelHost) Provision(ctx context.Context, spec ProvisionSpec) (ProvisionReceipt, error) {
	lock := h.idLock(spec.ChannelID)
	lock.Lock()
	defer lock.Unlock()
	if err := h.checkOpen(); err != nil {
		return ProvisionReceipt{}, err
	}
	if spec.Type == "" || spec.OwnerPrincipal == "" || spec.CreatedAt <= 0 {
		return ProvisionReceipt{}, errors.New("channelhost: invalid provision spec")
	}
	main, tombstone, err := h.paths(spec.ChannelID)
	if err != nil {
		return ProvisionReceipt{}, err
	}
	h.mu.RLock()
	current := h.entries[spec.ChannelID]
	h.mu.RUnlock()
	if current != nil {
		return ProvisionReceipt{}, ErrServing
	}
	if exists(tombstone) {
		return ProvisionReceipt{}, ErrChannelRetired
	}
	// A failed, never-published attempt is rebuilt from scratch under the same
	// per-ID lock. Tombstones are never part of this cleanup set.
	for _, path := range []string{main, main + "-wal", main + "-shm"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return ProvisionReceipt{}, fmt.Errorf("channelhost: clean unpublished image: %w", err)
		}
	}
	genesis := storespec.ChannelGenesis{ChannelID: string(spec.ChannelID), Type: spec.Type, OwnerPrincipal: spec.OwnerPrincipal, CreatedAt: spec.CreatedAt}
	if spec.Origin != nil {
		genesis.ParentChannelID = string(spec.Origin.ParentChannelID)
		genesis.InitiatorPrincipal = spec.Origin.InitiatorPrincipal
	}
	homeInstance, err := h.openHome(spec.ChannelID, main, true, &genesis)
	if err != nil {
		return ProvisionReceipt{}, err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = home.Shutdown(homeInstance)
		}
	}()
	if _, err := home.BootstrapOwner(ctx, homeInstance, spec.OwnerPrincipal); err != nil {
		return ProvisionReceipt{}, fmt.Errorf("channelhost: admit owner: %w", err)
	}
	for _, declaration := range spec.GenesisDeclarations {
		if err := declaration.Rendered.Validate(); err != nil {
			return ProvisionReceipt{}, fmt.Errorf("channelhost: invalid genesis declaration %q: %w", declaration.DeclID, err)
		}
		placement, err := storePlacement(declaration.Rendered.Placement)
		if err != nil {
			return ProvisionReceipt{}, err
		}
		config := json.RawMessage(append([]byte(nil), declaration.Rendered.Config...))
		if _, err := home.BootstrapDeclaration(ctx, homeInstance, home.DeclareRequest{
			SourceDeclID: declaration.DeclID, Principal: declaration.Principal, Kind: declaration.Kind,
			Class: declaration.Rendered.Class, Config: &config, Placement: placement,
			TIdle: declaration.Rendered.TIdleMS, MakeDefault: declaration.DeclID == spec.DefaultSourceDeclID,
			CreatedAt: spec.CreatedAt, RenderSeq: declaration.Rendered.RenderSeq,
		}); err != nil {
			return ProvisionReceipt{}, fmt.Errorf("channelhost: declare genesis %q: %w", declaration.DeclID, err)
		}
		rows, err := homeInstance.View().DeclaredBySource(ctx, declaration.DeclID)
		if err != nil || len(rows) != 1 || rows[0].Class != declaration.Rendered.Class {
			return ProvisionReceipt{}, fmt.Errorf("channelhost: genesis declaration %q failed readback", declaration.DeclID)
		}
	}
	if err := home.Shutdown(homeInstance); err != nil {
		return ProvisionReceipt{}, fmt.Errorf("channelhost: close bootstrap home: %w", err)
	}
	succeeded = true
	return ProvisionReceipt{ChannelID: spec.ChannelID, CreatedAt: spec.CreatedAt}, nil
}

func storePlacement(in channel.Placement) (storespec.Placement, error) {
	switch in.Kind {
	case channel.PlacementServer:
		return storespec.NewServerPlacement(), nil
	case channel.PlacementDaemon:
		return storespec.NewDaemonPlacement(in.DesiredHost)
	default:
		return storespec.Placement{}, channel.ErrInvalidPlacement
	}
}

func (h *ChannelHost) Open(ctx context.Context, spec OpenSpec) error {
	lock := h.idLock(spec.ChannelID)
	lock.Lock()
	defer lock.Unlock()
	if err := h.checkOpen(); err != nil {
		return err
	}
	if spec.ExpectedType == "" {
		return errors.New("channelhost: expected type required")
	}
	main, tombstone, err := h.paths(spec.ChannelID)
	if err != nil {
		return err
	}
	if exists(tombstone) || !exists(main) {
		return ErrChannelNotFound
	}
	h.mu.RLock()
	current := h.entries[spec.ChannelID]
	h.mu.RUnlock()
	if current != nil && current.state == stateServing {
		return nil
	}
	if current != nil {
		return ErrChannelNotFound
	}
	genesis := storespec.ChannelGenesis{ChannelID: string(spec.ChannelID), Type: spec.ExpectedType}
	// Read the owner from genesis first through a narrow store-only pass would
	// duplicate assembly. Home validates ID/type, then ChannelHost reads the
	// trusted owner from the resulting View and compares it to stored genesis.
	homeInstance, err := h.openHome(spec.ChannelID, main, false, &genesis)
	if err != nil {
		if strings.Contains(err.Error(), "owner invariant") {
			return errors.Join(ErrOwnerInvariant, err)
		}
		if strings.Contains(err.Error(), "schema incompatible") {
			return errors.Join(ErrSchemaIncompatible, err)
		}
		return err
	}
	generation := h.nextGen.Add(1)
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = home.Shutdown(homeInstance)
		return ErrClosed
	}
	if existing := h.entries[spec.ChannelID]; existing != nil && existing.state == stateServing {
		h.mu.Unlock()
		_ = home.Shutdown(homeInstance)
		return nil
	}
	h.entries[spec.ChannelID] = &entry{home: homeInstance, sysOp: home.SystemOps(homeInstance), generation: generation, state: stateServing}
	h.mu.Unlock()
	return nil
}

func (h *ChannelHost) openHome(id channel.ID, path string, bootstrap bool, genesis *storespec.ChannelGenesis) (*home.Home, error) {
	config := home.Config{
		ChannelID: id, DBPath: path, Bootstrap: bootstrap, MustExistDB: !bootstrap,
		CompositionResolver: h.deps.CompositionResolver, IntroductionResolver: h.deps.IntroductionResolver,
		Logger: h.logger,
	}
	if bootstrap {
		config.Genesis = genesis
	} else {
		config.ExpectedGenesis = genesis
	}
	if h.deps.OnMembershipChange != nil {
		config.OnMembershipChange = func(principal string) { h.deps.OnMembershipChange(id, []string{principal}) }
	}
	return home.Open(config)
}

func (h *ChannelHost) Acquire(id channel.ID) (Bundle, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return nil, false
	}
	entry := h.entries[id]
	if entry == nil || entry.state != stateServing || entry.home == nil {
		return nil, false
	}
	return &bundle{home: entry.home, sysOp: entry.sysOp, generation: entry.generation}, true
}

func (h *ChannelHost) Destroy(_ context.Context, id channel.ID) error {
	lock := h.idLock(id)
	lock.Lock()
	defer lock.Unlock()
	if err := h.checkOpen(); err != nil {
		return err
	}
	main, tombstone, err := h.paths(id)
	if err != nil {
		return err
	}
	h.mu.Lock()
	current := h.entries[id]
	if current != nil {
		current.state = stateSealing
	}
	alreadyClosed := current != nil && current.closed
	h.mu.Unlock()
	if current != nil && !alreadyClosed {
		// home.Shutdown is a slow IO operation and stays outside h.mu, but the
		// entry state flip is a mutated field Acquire reads under h.mu.RLock, so
		// the flip is always performed under h.mu.
		if err := home.Shutdown(current.home); err != nil {
			return fmt.Errorf("channelhost: close before seal: %w", err)
		}
		h.mu.Lock()
		current.closed = true
		current.state = stateSealed
		h.mu.Unlock()
	}
	if exists(tombstone) {
		if exists(main) {
			return ErrTombstoneExists
		}
		if err := h.finishSidecars(main); err != nil {
			return err
		}
		h.mu.Lock()
		delete(h.entries, id)
		h.mu.Unlock()
		return nil
	}
	if !exists(main) {
		if exists(main+"-wal") || exists(main+"-shm") {
			return errors.New("channelhost: main database missing with live sidecar")
		}
		h.mu.Lock()
		delete(h.entries, id)
		h.mu.Unlock()
		return nil
	}
	if err := renameNoReplace(main, tombstone); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return ErrTombstoneExists
		}
		return fmt.Errorf("channelhost: tombstone main: %w", err)
	}
	if err := h.finishSidecars(main); err != nil {
		return err
	}
	h.mu.Lock()
	delete(h.entries, id)
	h.mu.Unlock()
	return nil
}

func (h *ChannelHost) finishSidecars(main string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		source := main + suffix
		if !exists(source) {
			continue
		}
		if err := renameNoReplace(source, source+".tombstone"); err != nil {
			if errors.Is(err, unix.EEXIST) {
				return ErrTombstoneExists
			}
			return fmt.Errorf("channelhost: tombstone sidecar: %w", err)
		}
	}
	return nil
}

func exists(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular()
}

func (h *ChannelHost) Census(_ context.Context) ([]CensusEntry, error) {
	entries, err := os.ReadDir(h.root)
	if err != nil {
		return nil, err
	}
	out := make([]CensusEntry, 0)
	for _, file := range entries {
		if file.Type().IsRegular() == false || !strings.HasSuffix(file.Name(), ".db") {
			continue
		}
		id, err := decodeID(strings.TrimSuffix(file.Name(), ".db"))
		if err != nil {
			h.logger.Warn("channelhost census ignored invalid candidate", "name", file.Name(), "err", err)
			continue
		}
		state := CensusPresent
		if _, ok := h.Acquire(id); ok {
			state = CensusOpen
		}
		out = append(out, CensusEntry{ChannelID: id, State: state})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ChannelID < out[j].ChannelID })
	return out, nil
}

func (h *ChannelHost) checkOpen() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return ErrClosed
	}
	return nil
}

func (h *ChannelHost) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	entries := h.entries
	h.entries = make(map[channel.ID]*entry)
	h.mu.Unlock()
	var errs []error
	for id, entry := range entries {
		// Share the per-ID lock with the three lifecycle verbs so an in-flight
		// Destroy on the same channel cannot rename a database file whose Home
		// this Close is still shutting down, and neither double-shuts the Home.
		// Whoever wins the per-ID lock runs to completion before the other
		// observes `closed`; a Destroy that only wins after Close set h.closed
		// bails at its checkOpen.
		lock := h.idLock(id)
		lock.Lock()
		h.mu.Lock()
		skip := entry.home == nil || entry.closed
		h.mu.Unlock()
		if !skip {
			if err := home.Shutdown(entry.home); err != nil {
				errs = append(errs, fmt.Errorf("channelhost: close %s: %w", id, err))
			}
			h.mu.Lock()
			entry.closed = true
			entry.state = stateSealed
			h.mu.Unlock()
		}
		lock.Unlock()
	}
	return errors.Join(errs...)
}
