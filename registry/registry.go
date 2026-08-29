package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// Deps is the HOST CONTEXT a constructor builds against (which channel, the
// workspace root, the device identity, a logger). It is NOT instance identity —
// "which instance, with what config" comes from InstanceSpec. Splitting the two
// is the whole point: a class is a template; the host context + an InstanceSpec
// together produce one running instance.
type Deps struct {
	ChannelID    channel.ID // the channel this cell is scoped to
	WorkspaceDir string     // workspace root (device / agent situation facts)
	// DeviceID is the server-assigned identity of the daemon installation that
	// hosts this cell. It is routing identity. It never comes from hostname or a
	// CLI label.
	DeviceID string
	// DeviceLabel is presentation only. A constructor must never use it for a
	// resource address, placement decision, or identity comparison.
	DeviceLabel string
	Logger      *slog.Logger
}

// InstanceSpec is one actor instance's deployment params: its id and opaque
// per-instance config. A class (actors/<x>) is a template; an instance =
// class + InstanceSpec.
//
//   - ID == "" → "use the class's default id" (the fat-daemon one-of-each case).
//   - ID != "" → instantiate this class under that id; the SAME class can be
//     instantiated many times under different ids (multi-agent falls out here).
//
// Every class, including device, fills the seat ID the plan supplied. Physical
// device identity is independent host context in Deps.DeviceID; conflating it
// with an actor seat id makes the constructor disagree with the registry plan.
type InstanceSpec struct {
	ID actor.ActorID
	// Config is THE per-instance config injection point — the composition spec's
	// "ctx.Config" (K2=a/S8). It is NOT a new runtime surface; the constructor
	// closure captures it into its Proc Def (Constructor(spec,deps) → Def →
	// New() per incarnation; see
	// actorbase.Def's doc). Either way config rides the constructor, NOT the
	// capability bundle: it is an independent PARAMETER, never welded into
	// actorcaps.Caps (S-P16 红线; enforced by archtest.TestConfigNotInCaps).
	// A config change replaces Controller desired and therefore builds a fresh
	// incarnation over a fresh snapshot; there is no live hot-read of Config.
	Config json.RawMessage
}

// Constructor builds one instance of a class: given the deployment spec (id +
// config) and the host context, it produces the ActorDecl the host spawns. The
// id comes from the spec, NOT baked into the constructor — so one class yields
// many instances. A bad config / missing creds is a hard error.
type Constructor func(spec InstanceSpec, ctx Deps) (platform.ActorDecl, error)

// ClassDecl is a class's directory-level declaration: the class-level facts
// knowable WITHOUT running the constructor (pre-Build), plus the constructor
// itself. Kind is the one such fact today — Admit needs it before any cell is
// built, and it was previously locked inside the Constructor's return value
// (unreachable pre-Build). Future class-level facts are additive fields here;
// the Register signature no longer changes when they arrive.
type ClassDecl struct {
	Kind      actor.Kind
	Placement channelspec.PlacementKind
	Manifest  introspect.Manifest
	New       Constructor
	// DefaultConfig is the class-owned baseline for every instance. It belongs
	// beside the constructor and schema: providers know their own operational
	// defaults; boot, declarations, and callers must not copy those defaults.
	// The registry snapshots and validates it at registration, then shallowly
	// overlays the per-instance JSON object before validation and construction.
	DefaultConfig func() json.RawMessage
	// ValidateConfig is the acceptance gate. ConfigSchema is the same contract
	// stated forward: the gate can only say no after the fact, and a caller
	// writing a declaration has nothing else to go on — the shape lives in Go
	// types a template author cannot read. Optional, because a class taking no
	// config has nothing to describe.
	ValidateConfig func(json.RawMessage) error
	ConfigSchema   json.RawMessage
	defaultConfig  json.RawMessage
}

// ErrUnknownClass distinguishes "no such class" from "config invalid": the
// two ailments need opposite user action (fix the class name vs fix the
// config), so callers must be able to tell them apart.
var ErrUnknownClass = errors.New("registry: unknown class")

// ValidateConfig performs every check a class voluntarily makes available at
// acceptance time. The registry always owns the JSON-object shape check;
// constructors remain fail-closed for host/environment-dependent conditions.
func ValidateConfig(class string, config json.RawMessage) error {
	mu.RLock()
	d, found := reg[class]
	mu.RUnlock()
	if !found {
		return fmt.Errorf("%w: %q", ErrUnknownClass, class)
	}
	_, err := resolveConfig(class, d, config)
	return err
}

// ResolveConfig returns the effective class config: the class-owned default
// with the instance object overlaid at the top level. Arrays and nested values
// are intentionally atomic — an instance that supplies selections replaces
// the default selection catalog instead of accidentally splicing two policy
// documents together.
func ResolveConfig(class string, config json.RawMessage) (json.RawMessage, error) {
	mu.RLock()
	d, found := reg[class]
	mu.RUnlock()
	if !found {
		return nil, fmt.Errorf("%w: %q", ErrUnknownClass, class)
	}
	return resolveConfig(class, d, config)
}

// ResolveDefaultConfig is the composition-root door used by boot: unlike the
// general resolver it refuses classes that have no declared baseline, because
// silently carving a special actor from only an override would recreate the
// very boot-owned configuration this API removes.
func ResolveDefaultConfig(class string, config json.RawMessage) (json.RawMessage, error) {
	mu.RLock()
	d, found := reg[class]
	mu.RUnlock()
	if !found {
		return nil, fmt.Errorf("%w: %q", ErrUnknownClass, class)
	}
	if len(d.defaultConfig) == 0 {
		return nil, fmt.Errorf("registry: class %q has no default config", class)
	}
	return resolveConfig(class, d, config)
}

func resolveConfig(class string, d ClassDecl, override json.RawMessage) (json.RawMessage, error) {
	base, err := configObject(class, "default", d.defaultConfig)
	if err != nil {
		return nil, err
	}
	provided, err := configObject(class, "instance", override)
	if err != nil {
		return nil, err
	}
	if len(base) == 0 && len(provided) == 0 && len(d.defaultConfig) == 0 && len(override) == 0 {
		if d.ValidateConfig != nil {
			if err := d.ValidateConfig(nil); err != nil {
				return nil, fmt.Errorf("registry: invalid config for %q: %w", class, err)
			}
		}
		return nil, nil
	}
	for key, value := range provided {
		base[key] = append(json.RawMessage(nil), value...)
	}
	effective, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("registry: encode effective config for %q: %w", class, err)
	}
	if d.ValidateConfig != nil {
		if err := d.ValidateConfig(effective); err != nil {
			return nil, fmt.Errorf("registry: invalid config for %q: %w", class, err)
		}
	}
	return effective, nil
}

func configObject(class, source string, raw json.RawMessage) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	if len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, fmt.Errorf("registry: %s config for %q must be a JSON object", source, class)
	}
	return out, nil
}

var (
	mu  sync.RWMutex
	reg = map[string]ClassDecl{}
)

// Register records a class's ClassDecl under its class key. Called from an
// actor package's init(); a duplicate class is a programmer error (panic, like
// sql.Register).
func Register(class string, d ClassDecl) {
	if d.Placement != channelspec.PlacementServer && d.Placement != channelspec.PlacementDaemon {
		panic("registry: class placement required: " + class)
	}
	if d.Manifest.Class == "" {
		d.Manifest.Class = class
	}
	if err := introspect.ValidateManifest(d.Manifest); err != nil {
		panic("registry: invalid manifest for " + class + ": " + err.Error())
	}
	if d.DefaultConfig != nil {
		d.defaultConfig = append(json.RawMessage(nil), d.DefaultConfig()...)
		if _, err := resolveConfig(class, d, nil); err != nil {
			panic("registry: invalid default config for " + class + ": " + err.Error())
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if _, dup := reg[class]; dup {
		panic("registry: duplicate class registration: " + class)
	}
	reg[class] = d
}

// ClassPlacement returns the single class-level placement fact used by both
// genesis rendering and runtime introduction.
func ClassPlacement(class string) (channelspec.PlacementKind, bool) {
	mu.RLock()
	defer mu.RUnlock()
	d, ok := reg[class]
	if !ok {
		return "", false
	}
	return d.Placement, true
}

// ClassKind returns a class's declared Kind (pure table lookup, no construction)
// and whether the class is registered. It is the pre-Build kind source Admit
// resolves against.
func ClassKind(class string) (actor.Kind, bool) {
	mu.RLock()
	defer mu.RUnlock()
	d, ok := reg[class]
	if !ok {
		return "", false
	}
	return d.Kind, true
}

// ClassConfigSchema returns a class's published config shape, if it declared
// one. It is a pure table lookup like the two above, and answers the question
// a declaration author has to answer before writing config at all.
func ClassConfigSchema(class string) (json.RawMessage, bool) {
	mu.RLock()
	defer mu.RUnlock()
	d, ok := reg[class]
	if !ok || len(d.ConfigSchema) == 0 {
		return nil, false
	}
	return append(json.RawMessage(nil), d.ConfigSchema...), true
}

// ClassDefaultConfig returns the immutable provider/class baseline advertised
// to declaration authors and consumed by special constructors such as boot's
// root steward carving. Runtime instances use ResolveConfig/Build and therefore
// receive the same baseline without callers materialising it themselves.
func ClassDefaultConfig(class string) (json.RawMessage, bool) {
	mu.RLock()
	defer mu.RUnlock()
	d, ok := reg[class]
	if !ok || len(d.defaultConfig) == 0 {
		return nil, false
	}
	return append(json.RawMessage(nil), d.defaultConfig...), true
}

// classes returns the registered class keys, sorted (stable iteration order).
// Build uses it for diagnostics; RegisteredClasses exposes a snapshot for
// whole-registry invariants without exposing mutable declarations.
func classes() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(reg))
	for c := range reg {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// RegisteredClasses returns a stable snapshot of class keys for whole-registry
// validation. Callers still resolve class facts through the narrow lookups.
func RegisteredClasses() []string { return classes() }

// Build instantiates one instance of class with the given spec + host context.
// Unknown class or a constructor error are returned to the caller (who asked for
// it explicitly).
func Build(class string, spec InstanceSpec, ctx Deps) (platform.ActorDecl, error) {
	mu.RLock()
	d, found := reg[class]
	mu.RUnlock()
	if !found {
		return platform.ActorDecl{}, fmt.Errorf("%w: %q (registered: %v)", ErrUnknownClass, class, classes())
	}
	effective, err := resolveConfig(class, d, spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	spec.Config = effective
	decl, err := d.New(spec, ctx)
	if err != nil {
		return platform.ActorDecl{}, fmt.Errorf("registry: build %q: %w", class, err)
	}
	// Consistency guard: the constructed decl's Kind must match the class's
	// declared Kind — a drift means the directory declaration lies about what
	// the constructor produces. Fail loud rather than admit a mislabelled cell.
	if decl.Kind != d.Kind {
		return platform.ActorDecl{}, fmt.Errorf("registry: build %q: constructed kind %q ≠ declared kind %q", class, decl.Kind, d.Kind)
	}
	// The constructor may project an INSTANCE manifest — per-instance words such
	// as agent.select's selections schema, which only the instance config knows.
	// Respect it; the class-level manifest is the fallback for constructors that
	// declare none. Unconditionally overwriting here silently erased every
	// instance projection (actor.describe is a per-instance answer, not a class
	// directory row).
	if decl.Factory.Proc.Manifest.Class == "" {
		decl.Factory.Proc.Manifest = d.Manifest
	} else {
		// An instance projection must still be a valid manifest, and its Class
		// is normalized to the registered class: the words are the instance's
		// truth, the identity is the directory's. (A generic body such as
		// peeractor legitimately carries its own Class — rejecting the mismatch
		// would refuse every template-named proxy; overwriting only the Class
		// keeps describe consistent with the directory row.)
		if err := introspect.ValidateManifest(decl.Factory.Proc.Manifest); err != nil {
			return platform.ActorDecl{}, fmt.Errorf("registry: build %q: instance manifest invalid: %w", class, err)
		}
		decl.Factory.Proc.Manifest.Class = class
	}
	return decl, nil
}
