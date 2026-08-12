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

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/lagoon"
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
	Destroy(context.Context, channel.ID) error
	Open(context.Context, OpenSpec) error
	Census(context.Context) ([]CensusEntry, error)
	Close(context.Context) error
}

type LocalHost interface {
	Service
	Acquire(channel.ID) (Bundle, bool)
	Poke(channel.ID) bool
}

type OpenSpec struct {
	ChannelID    channel.ID
	ExpectedType string
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

// HomeDeps contains only space-owned resolution and notification seams. Channel
// execution, binding admission, and planning stay behind the membrane.
type HomeDeps struct {
	CompositionResolver  home.CompositionResolver
	IntroductionResolver home.IntroductionResolver
	DaemonRoutes         platform.DaemonRoutes
	RegistryBindings     home.BindingReader
	OnMembraneOpen       func(channel.ID, uint64, platform.DaemonMembrane)
	OnMembraneClose      func(channel.ID, uint64)
	Logger               *slog.Logger
}

type entryState uint8

const (
	stateServing entryState = iota + 1
	stateSealing
	stateSealed
)

type entry struct {
	home           *home.Home
	generation     uint64
	state          entryState
	closed         bool
	destroying     bool
	membraneClosed bool
	// genesisType is the channel type Open verified against genesis; the
	// serving fast path re-checks ExpectedType against it instead of skipping
	// the strict validation.
	genesisType string
}

type ChannelHost struct {
	root        string
	deps        HomeDeps
	logger      *slog.Logger
	registry    RegistryReader
	convergence *convergenceState
	shutdown    func(*home.Home, context.Context) error

	mu      sync.RWMutex
	closed  bool
	entries map[channel.ID]*entry
	locks   map[channel.ID]*sync.Mutex
	nextGen atomic.Uint64
}

var _ LocalHost = (*ChannelHost)(nil)

func (h *ChannelHost) Poke(id channel.ID) bool {
	h.mu.RLock()
	entry := h.entries[id]
	if entry == nil || entry.home == nil || entry.state != stateServing || entry.closed {
		h.mu.RUnlock()
		return false
	}
	home.Poke(entry.home)
	h.mu.RUnlock()
	return true
}

func New(root string, registry RegistryReader, deps HomeDeps) (*ChannelHost, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("channelhost: root required")
	}
	if registry == nil {
		return nil, errors.New("channelhost: registry required")
	}
	if deps.CompositionResolver == nil {
		return nil, errors.New("channelhost: CompositionResolver required")
	}
	if deps.IntroductionResolver == nil {
		return nil, errors.New("channelhost: IntroductionResolver required")
	}
	if deps.RegistryBindings == nil {
		return nil, errors.New("channelhost: RegistryBindings required")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("channelhost: create root: %w", err)
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	h := &ChannelHost{
		root: root, deps: deps, logger: logger, registry: registry,
		entries: make(map[channel.ID]*entry), locks: make(map[channel.ID]*sync.Mutex),
		shutdown: home.ShutdownWithin,
	}
	h.convergence = newConvergenceState()
	return h, nil
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

// DBPath returns the ordinary channel database path used by ChannelHost. Boot
// uses it only to locate c0 before any host exists; schema creation still goes
// through runtime.OpenChannel, the same store-opening path Provision uses.
func DBPath(root string, id channel.ID) (string, error) {
	encoded, err := encodeID(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, encoded+".db"), nil
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

// KNOWN, deliberately unhandled at this stage: the per-ID lock table only
// grows — every channel ID ever touched keeps its mutex until process exit.
// Bounded by lifetime channel count; revisit with a refcounted keyed lock if a
// long-lived space's churn ever makes this measurable.
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

func (h *ChannelHost) provisionGenesis(ctx context.Context, spec lagoon.GenesisSpec) error {
	lock := h.idLock(spec.ChannelID)
	lock.Lock()
	defer lock.Unlock()
	if err := h.checkOpen(); err != nil {
		return err
	}
	if spec.Type == "" || spec.OwnerPrincipal == "" || spec.CreatedAt <= 0 {
		return errors.New("channelhost: invalid genesis spec")
	}
	main, tombstone, err := h.paths(spec.ChannelID)
	if err != nil {
		return err
	}
	h.mu.RLock()
	current := h.entries[spec.ChannelID]
	h.mu.RUnlock()
	if current != nil {
		return ErrServing
	}
	if exists(tombstone) {
		return ErrChannelRetired
	}
	// A failed, never-published attempt is rebuilt from scratch under the same
	// per-ID lock. Tombstones are never part of this cleanup set.
	for _, path := range []string{main, main + "-wal", main + "-shm"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("channelhost: clean unpublished image: %w", err)
		}
	}
	genesis := storespec.ChannelGenesis{ChannelID: string(spec.ChannelID), Type: spec.Type, OwnerPrincipal: spec.OwnerPrincipal, CreatedAt: spec.CreatedAt}
	genesis.ParentChannelID = string(spec.ParentID)
	genesis.InitiatorPrincipal = spec.InitiatorPrincipal
	bootstrapDeclarations := make([]home.DeclareRequest, 0, len(spec.Declarations))
	for _, declaration := range spec.Declarations {
		if err := declaration.Rendered.Validate(); err != nil {
			return fmt.Errorf("channelhost: invalid genesis declaration %q: %w", declaration.DeclID, err)
		}
		placement, err := storePlacement(declaration.Rendered.Placement)
		if err != nil {
			return err
		}
		config := json.RawMessage(append([]byte(nil), declaration.Rendered.Config...))
		bootstrapDeclarations = append(bootstrapDeclarations, home.DeclareRequest{
			SourceDeclID: declaration.DeclID, Kind: declaration.Kind,
			Class: declaration.Rendered.Class, Config: &config, Placement: placement,
			CreatedAt: spec.CreatedAt,
		})
	}
	homeInstance, err := h.openHome(
		spec.ChannelID, main, true, &genesis,
		spec.OwnerPrincipal, bootstrapDeclarations,
	)
	if err != nil {
		return err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = home.Shutdown(homeInstance)
		}
	}()
	for _, declaration := range spec.Declarations {
		ids, err := homeInstance.View().DeclaredInstances(ctx, declaration.DeclID)
		if err != nil || len(ids) != 1 {
			return fmt.Errorf("channelhost: genesis declaration %q failed readback", declaration.DeclID)
		}
	}
	if err := home.Shutdown(homeInstance); err != nil {
		return fmt.Errorf("channelhost: close bootstrap home: %w", err)
	}
	succeeded = true
	return nil
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
	var notifyGeneration uint64
	var notifyMembrane platform.DaemonMembrane
	defer func() {
		lock.Unlock()
		if notifyGeneration != 0 && h.deps.OnMembraneOpen != nil {
			h.deps.OnMembraneOpen(spec.ChannelID, notifyGeneration, notifyMembrane)
		}
	}()
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
		if current.genesisType != spec.ExpectedType {
			return errors.Join(ErrSchemaIncompatible, fmt.Errorf("channelhost: serving channel is type %q, directory expects %q", current.genesisType, spec.ExpectedType))
		}
		return nil
	}
	if current != nil {
		return ErrChannelNotFound
	}
	genesis := storespec.ChannelGenesis{ChannelID: string(spec.ChannelID), Type: spec.ExpectedType}
	// Owner has exactly one home — the immutable genesis pointer — so opening
	// checks only that the pointer is present. There is no second account to
	// cross-check it against.
	homeInstance, err := h.openHome(spec.ChannelID, main, false, &genesis, "", nil)
	if err != nil {
		if errors.Is(err, home.ErrOwnerInvariant) {
			return errors.Join(ErrOwnerInvariant, err)
		}
		if errors.Is(err, home.ErrSchemaMismatch) {
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
	h.entries[spec.ChannelID] = &entry{home: homeInstance, generation: generation, state: stateServing, genesisType: spec.ExpectedType}
	h.mu.Unlock()
	notifyGeneration = generation
	notifyMembrane = home.DaemonMembrane(homeInstance)
	return nil
}

func (h *ChannelHost) openHome(
	id channel.ID,
	path string,
	bootstrap bool,
	genesis *storespec.ChannelGenesis,
	bootstrapOwner string,
	bootstrapDeclarations []home.DeclareRequest,
) (*home.Home, error) {
	config := home.Config{
		ChannelID: id, DBPath: path, Bootstrap: bootstrap, MustExistDB: !bootstrap,
		CompositionResolver: h.deps.CompositionResolver, IntroductionResolver: h.deps.IntroductionResolver,
		Logger: h.logger, BootstrapOwnerPrincipal: bootstrapOwner,
		BootstrapDeclarations: bootstrapDeclarations,
		DaemonRoutes:          h.deps.DaemonRoutes,
		RegistryBindings:      h.deps.RegistryBindings,
	}
	if bootstrap {
		config.Genesis = genesis
	} else {
		config.ExpectedGenesis = genesis
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
	return &bundle{home: entry.home, generation: entry.generation}, true
}

func (h *ChannelHost) Destroy(ctx context.Context, id channel.ID) error {
	lock := h.idLock(id)
	lock.Lock()
	if err := h.checkOpen(); err != nil {
		lock.Unlock()
		return err
	}
	main, tombstone, err := h.paths(id)
	if err != nil {
		lock.Unlock()
		return err
	}
	var notifyGeneration uint64
	h.mu.Lock()
	current := h.entries[id]
	if current != nil {
		if current.destroying {
			h.mu.Unlock()
			lock.Unlock()
			return ErrChannelNotFound
		}
		current.destroying = true
		current.state = stateSealing
		if !current.membraneClosed {
			current.membraneClosed = true
			notifyGeneration = current.generation
		}
	}
	alreadyClosed := current != nil && current.closed
	h.mu.Unlock()
	lock.Unlock()
	if notifyGeneration != 0 && h.deps.OnMembraneClose != nil {
		h.deps.OnMembraneClose(id, notifyGeneration)
	}
	lock.Lock()
	defer func() {
		if current != nil {
			h.mu.Lock()
			current.destroying = false
			h.mu.Unlock()
		}
		lock.Unlock()
	}()
	if current != nil {
		h.mu.RLock()
		alreadyClosed = current.closed
		h.mu.RUnlock()
	}
	if current != nil && !alreadyClosed {
		// home.Shutdown is a slow IO operation and stays outside h.mu, but the
		// entry state flip is a mutated field Acquire reads under h.mu.RLock, so
		// the flip is always performed under h.mu.
		if err := h.shutdown(current.home, ctx); err != nil {
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

// Close tears down every serving Home under the caller's budget: one shared
// deadline bounds the whole sweep, so a wedged store cannot hold the process
// shutdown hostage past it. A Home that could not close in time stays
// registered with its account in the returned error — a repeat Close retries
// exactly those — and its store close keeps running in the background, where
// process death is what finally reclaims it.
func (h *ChannelHost) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	entries := make(map[channel.ID]*entry, len(h.entries))
	notifications := make(map[channel.ID]uint64)
	for id, e := range h.entries {
		entries[id] = e
		e.state = stateSealing
		if !e.membraneClosed {
			e.membraneClosed = true
			notifications[id] = e.generation
		}
	}
	h.mu.Unlock()
	var errs []error
	if err := h.stopConvergence(ctx); err != nil {
		errs = append(errs, fmt.Errorf("channelhost: stop convergence: %w", err))
	}
	if h.deps.OnMembraneClose != nil {
		for id, generation := range notifications {
			h.deps.OnMembraneClose(id, generation)
		}
	}
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
		failed := false
		if !skip {
			// Only a clean shutdown marks the entry closed: a failed Home close
			// stays honestly un-closed (Home's owner-join/teardown/store close
			// sequence is retryable; never fake terminal state on an error).
			if err := h.shutdown(entry.home, ctx); err != nil {
				failed = true
				errs = append(errs, fmt.Errorf("channelhost: close %s: %w", id, err))
			} else {
				h.mu.Lock()
				entry.closed = true
				entry.state = stateSealed
				h.mu.Unlock()
			}
		}
		if !failed {
			// Only a cleanly closed (or already-dead) entry leaves the registry:
			// a failed Home close stays registered so a repeat Close retries it —
			// dropping it here would turn the next Close into a fake nil success
			// while the store handle stays live.
			h.mu.Lock()
			delete(h.entries, id)
			h.mu.Unlock()
		}
		lock.Unlock()
	}
	return errors.Join(errs...)
}
