package actorhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

var (
	ErrInvalidAttemptKey = errors.New("actorhost: invalid attempt key")
	ErrInvalidDomain     = errors.New("actorhost: invalid execution domain")
	ErrInvalidDesired    = errors.New("actorhost: invalid desired")
	ErrSameAttemptDrift  = errors.New("actorhost: same attempt changed immutable desired")
	ErrHostClosed        = errors.New("actorhost: host closed")
	ErrAttachRejected    = errors.New("actorhost: attach rejected")

	ErrNotHosted = errors.New("actorhost: actor not hosted")
)

// AttemptKey identifies one process-local logical Run. It is opaque outside
// actor control. Actorhost only validates it and compares its whole canonical
// UUIDv7 value when preventing a delayed route predecessor from replacing its
// successor.
type AttemptKey string

// NewAttemptKey mints a canonical UUIDv7 token. The Controller is the semantic
// owner of minting; this constructor lives here because AttemptKey is a Host
// input type.
func NewAttemptKey() (AttemptKey, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidAttemptKey, err)
	}
	return AttemptKey(id.String()), nil
}

// ParseAttemptKey validates canonical lowercase UUIDv7 text.
func ParseAttemptKey(raw string) (AttemptKey, error) {
	id, err := uuid.Parse(raw)
	if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 || id.String() != raw {
		return "", ErrInvalidAttemptKey
	}
	return AttemptKey(raw), nil
}

func (k AttemptKey) valid() bool {
	_, err := ParseAttemptKey(string(k))
	return err == nil
}

// compareAttemptKeys compares canonical UUIDv7 whole values. It is structural,
// not an authorization or cross-process epoch decision.
func compareAttemptKeys(left, right AttemptKey) (int, error) {
	if !left.valid() || !right.valid() {
		return 0, ErrInvalidAttemptKey
	}
	return strings.Compare(string(left), string(right)), nil
}

// ExecutionDomain names one HostSupervisor's execution scope.
type ExecutionDomain string

func (d ExecutionDomain) valid() bool { return strings.TrimSpace(string(d)) != "" }

// ExecutionSpec is the immutable input used to construct a local body.
type ExecutionSpec struct {
	Kind   actor.Kind
	Class  string
	Config json.RawMessage
}

func (s ExecutionSpec) canonical() (ExecutionSpec, error) {
	if _, ok := actor.ParseKind(string(s.Kind)); !ok {
		return ExecutionSpec{}, ErrInvalidDesired
	}
	out := s
	if len(s.Config) == 0 {
		out.Config = nil
		return out, nil
	}
	dec := json.NewDecoder(bytes.NewReader(s.Config))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return ExecutionSpec{}, fmt.Errorf("%w: config: %v", ErrInvalidDesired, err)
	}
	if dec.More() {
		return ExecutionSpec{}, fmt.Errorf("%w: config trailing value", ErrInvalidDesired)
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ExecutionSpec{}, fmt.Errorf("%w: config trailing value", ErrInvalidDesired)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ExecutionSpec{}, fmt.Errorf("%w: config: %v", ErrInvalidDesired, err)
	}
	out.Config = encoded
	return out, nil
}

func executionSpecEqual(left, right ExecutionSpec) bool {
	return left.Kind == right.Kind &&
		left.Class == right.Class &&
		bytes.Equal(left.Config, right.Config)
}

// Equal reports whether two construction inputs name the same canonical body
// definition. It canonicalizes both values so composition sources can match an
// exact Host build input without depending on JSON formatting.
func (s ExecutionSpec) Equal(other ExecutionSpec) bool {
	left, leftErr := s.canonical()
	right, rightErr := other.canonical()
	return leftErr == nil && rightErr == nil && executionSpecEqual(left, right)
}

// Desired is a strict tagged union. Only BodyDesired and CarrierDesired can
// implement it.
type Desired interface {
	desired()
	Actor() actor.ActorID
	Attempt() AttemptKey
}

// BodyDesired requires one exact local Unit.
type BodyDesired struct {
	ActorID       actor.ActorID
	AttemptKey    AttemptKey
	ExecutionSpec ExecutionSpec
}

func (BodyDesired) desired()               {}
func (d BodyDesired) Actor() actor.ActorID { return d.ActorID }
func (d BodyDesired) Attempt() AttemptKey  { return d.AttemptKey }

// CarrierDesired requires a remote route when its peer is connected. Route
// absence is represented by Actual=nil, not by a zero-resource actual.
type CarrierDesired struct {
	ActorID    actor.ActorID
	AttemptKey AttemptKey
	PeerDomain ExecutionDomain
}

func (CarrierDesired) desired()               {}
func (d CarrierDesired) Actor() actor.ActorID { return d.ActorID }
func (d CarrierDesired) Attempt() AttemptKey  { return d.AttemptKey }

type desiredValue struct {
	body    *BodyDesired
	carrier *CarrierDesired
}

func normalizeDesired(input Desired) (desiredValue, error) {
	switch d := input.(type) {
	case BodyDesired:
		return normalizeBody(d)
	case *BodyDesired:
		if d == nil {
			return desiredValue{}, ErrInvalidDesired
		}
		return normalizeBody(*d)
	case CarrierDesired:
		return normalizeCarrier(d)
	case *CarrierDesired:
		if d == nil {
			return desiredValue{}, ErrInvalidDesired
		}
		return normalizeCarrier(*d)
	default:
		return desiredValue{}, ErrInvalidDesired
	}
}

func normalizeBody(d BodyDesired) (desiredValue, error) {
	if err := validateCoordinate(d.ActorID, d.AttemptKey); err != nil {
		return desiredValue{}, err
	}
	spec, err := d.ExecutionSpec.canonical()
	if err != nil {
		return desiredValue{}, err
	}
	d.ExecutionSpec = spec
	return desiredValue{body: &d}, nil
}

func normalizeCarrier(d CarrierDesired) (desiredValue, error) {
	if err := validateCoordinate(d.ActorID, d.AttemptKey); err != nil {
		return desiredValue{}, err
	}
	if !d.PeerDomain.valid() {
		return desiredValue{}, ErrInvalidDomain
	}
	return desiredValue{carrier: &d}, nil
}

func validateCoordinate(id actor.ActorID, key AttemptKey) error {
	// No system guard: the value ledger upstream never produces a system
	// coordinate (the kernel has no record), so re-screening for it here would
	// be redundant buckshot.
	if id == "" {
		return ErrInvalidDesired
	}
	if !key.valid() {
		return ErrInvalidAttemptKey
	}
	return nil
}

func (d desiredValue) actorID() actor.ActorID {
	if d.body != nil {
		return d.body.ActorID
	}
	if d.carrier != nil {
		return d.carrier.ActorID
	}
	return ""
}

func (d desiredValue) attemptKey() AttemptKey {
	if d.body != nil {
		return d.body.AttemptKey
	}
	if d.carrier != nil {
		return d.carrier.AttemptKey
	}
	return ""
}

func (d desiredValue) equal(other desiredValue) bool {
	switch {
	case d.body != nil && other.body != nil:
		return d.body.ActorID == other.body.ActorID &&
			d.body.AttemptKey == other.body.AttemptKey &&
			executionSpecEqual(d.body.ExecutionSpec, other.body.ExecutionSpec)
	case d.carrier != nil && other.carrier != nil:
		return d.carrier.ActorID == other.carrier.ActorID &&
			d.carrier.AttemptKey == other.carrier.AttemptKey &&
			d.carrier.PeerDomain == other.carrier.PeerDomain
	default:
		return false
	}
}

func (d desiredValue) clonePublic() Desired {
	if d.body != nil {
		out := *d.body
		out.ExecutionSpec.Config = append(json.RawMessage(nil), out.ExecutionSpec.Config...)
		return out
	}
	if d.carrier != nil {
		return *d.carrier
	}
	return nil
}

// ActorEndpoint is the common local/remote delivery surface.
type ActorEndpoint interface {
	Deliver(*message.Envelope) error
	CancelRequest(message.ID)
}

// BindingResource is the behavior behind one remote route. HostSupervisor
// never stores or compares this open interface directly; NewBinding wraps it
// in a pointer-backed, comparable Binding value first.
type BindingResource interface {
	ActorEndpoint
	Close() error
	Done() <-chan struct{}
}

type bindingRef struct {
	resource BindingResource
}

// Binding is an opaque exact route identity. Its only comparable field is the
// private ref pointer, so equality is total even when the wrapped resource has
// a non-comparable dynamic implementation.
type Binding struct {
	ref *bindingRef
}

// NewBinding seals one route resource into an exact comparable handle.
func NewBinding(resource BindingResource) (Binding, error) {
	if nilBindingResource(resource) || resource.Done() == nil {
		return Binding{}, fmt.Errorf("%w: nil binding", ErrInvalidDesired)
	}
	return Binding{ref: &bindingRef{resource: resource}}, nil
}

func nilBindingResource(resource BindingResource) bool {
	if resource == nil {
		return true
	}
	value := reflect.ValueOf(resource)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Valid reports whether the handle owns an exact route resource.
func (b Binding) Valid() bool { return b.ref != nil && b.ref.resource != nil }

func (b Binding) Deliver(env *message.Envelope) error {
	if !b.Valid() {
		return ErrNotHosted
	}
	return b.ref.resource.Deliver(env)
}

func (b Binding) CancelRequest(id message.ID) {
	if b.Valid() {
		b.ref.resource.CancelRequest(id)
	}
}

func (b Binding) Close() error {
	if !b.Valid() {
		return nil
	}
	return b.ref.resource.Close()
}

func (b Binding) Done() <-chan struct{} {
	if !b.Valid() {
		return nil
	}
	return b.ref.resource.Done()
}

// BodyBuildInput is the complete immutable input to one body builder.
type BodyBuildInput struct {
	ActorID       actor.ActorID
	AttemptKey    AttemptKey
	ExecutionSpec ExecutionSpec
	Self          actorrt.Incarnation
	Identity      IdentityCurrent
	Attempt       AttemptCurrent
	Current       ActualCurrent
}

// BodyBuilder constructs one actor implementation for one prepared Unit.
type BodyBuilder func(BodyBuildInput) actorrt.Actor

// ActualCurrent is an exact sliding-window current check welded to one
// {Host, ActorID, AttemptKey, Incarnation}. It does not drain in-flight work.
type ActualCurrent struct {
	host *HostSupervisor
	id   actor.ActorID
	key  AttemptKey
	self actorrt.Incarnation
}

// IdentityCurrent is an opaque accepted-level probe for one ActorID. It does
// not inspect the current physical Unit or route.
type IdentityCurrent struct {
	host *HostSupervisor
	id   actor.ActorID
}

func (c IdentityCurrent) IsCurrent() bool {
	return c.host != nil && c.host.identityCurrent(c.id)
}

// AttemptCurrent is an opaque accepted-level probe for one logical A/G run.
// It deliberately contains no physical Incarnation coordinate.
type AttemptCurrent struct {
	host *HostSupervisor
	id   actor.ActorID
	key  AttemptKey
}

func (c AttemptCurrent) IsCurrent() bool {
	return c.host != nil && c.host.attemptCurrent(c.id, c.key)
}

func (c ActualCurrent) IsCurrent() bool {
	return c.host != nil && c.host.isCurrent(c.id, c.key, c.self)
}

// Coordinate exposes the logical coordinate for composition code without
// exposing exact Unit ownership.
func (c ActualCurrent) Coordinate() (actor.ActorID, AttemptKey) {
	return c.id, c.key
}

// HostEventSink receives already-current-filtered Unit events.
type HostEventSink interface {
	OnBodyExited(actor.ActorID, AttemptKey, actorrt.Incarnation, error)
	OnBodyObs(actor.ActorID, AttemptKey, actorrt.Incarnation, actorrt.ObsKind, actorrt.ObsValue)
}

// ActualKind is a diagnostic snapshot tag.
type ActualKind uint8

const (
	ActualNone ActualKind = iota
	ActualBody
	ActualRoute
)

// Snapshot is an immutable diagnostic view used by status and tests.
type Snapshot struct {
	Desired   Desired
	Actual    ActualKind
	Attempt   AttemptKey
	Unit      *actorrt.Unit
	Binding   Binding
	StartedAt time.Time
	Building  bool
	Retiring  int
	Retrying  bool
}
