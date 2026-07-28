package link

// Existence-is-information, held across the avatar boundary.
//
// The door answers a zero-rights Stat with the SAME verdict it gives for a
// resource that does not exist (QueryNotFound — accessdoor/query.go's
// deliberate 404-not-403 discipline), so a caller with no grants cannot probe
// for what exists. accessdoor's own tests pin that decision in-process. What
// they cannot reach is the third avatar: a cell holding a WIRE proxy runs the
// same query through an encode/decode round trip, and every field the wire
// drops, adds, or reshapes is a channel through which the two answers could
// start to differ.
//
// So this drives ONE real door from both avatars at once — the local handle
// directly, and the same handle through the real relay arm — and asserts the
// wire preserves the masquerade exactly: identical results across avatars, and
// (the actual security property) an identical result for "no rights" and "no
// such resource" as seen from the wire. The positive control at the end proves
// the wire is not simply flattening every Stat into the same answer.

import (
	"context"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// --- the smallest real door that can answer a Stat ---------------------------

// statProbeAuthority is the welded caller. Admit succeeds: this test is about
// the RIGHTS layer's masquerade, not the admission fence in front of it.
type statProbeAuthority struct{ id actor.ActorID }

func (a statProbeAuthority) ActorID() actor.ActorID { return a.id }
func (a statProbeAuthority) Admit() error           { return nil }

var _ capauth.Authority = statProbeAuthority{}

// statProbeRegistry is existence + R. Stat consults exactly these two facts,
// so everything else on the Registry contract stays inert and loud.
type statProbeRegistry struct {
	rows  map[resource.ResourceID]resourcespec.ResourceMeta
	grant map[resource.ResourceID][]access.Operation
}

func (r *statProbeRegistry) Resolve(
	_ context.Context,
	id resource.ResourceID,
) (resourcespec.ResourceMeta, bool, error) {
	meta, ok := r.rows[id]
	return meta, ok, nil
}

func (r *statProbeRegistry) ActorAllows(
	_ context.Context,
	_ actor.ActorID,
	id resource.ResourceID,
	op access.Operation,
) (bool, error) {
	for _, allowed := range r.grant[id] {
		if allowed == op {
			return true, nil
		}
	}
	return false, nil
}

func (r *statProbeRegistry) MembersAllow(context.Context, resource.ResourceID, access.Operation) (bool, error) {
	return false, nil
}

func (*statProbeRegistry) Create(
	context.Context, resource.ResourceID, resourcespec.ResourceKind, actor.ActorID,
	string, string, []byte, resourcespec.ResourceBirthPlan,
) error {
	return errStatProbeUnexercised
}

func (*statProbeRegistry) ReserveCreate(
	context.Context, resource.ResourceID, resourcespec.ResourceKind, actor.ActorID,
	string, string, bool, resourcespec.ResourceBirthPlan,
) (string, error) {
	return "", errStatProbeUnexercised
}

func (*statProbeRegistry) CommitReservation(
	context.Context, string,
) (resourcespec.LandedResource, bool, error) {
	return resourcespec.LandedResource{}, false, errStatProbeUnexercised
}

func (*statProbeRegistry) SetGrant(context.Context, resource.ResourceID, access.Grant) error {
	return errStatProbeUnexercised
}

func (*statProbeRegistry) Delete(context.Context, resource.ResourceID) error {
	return errStatProbeUnexercised
}

func (*statProbeRegistry) ClearTombstone(context.Context, string) (bool, error) {
	return false, errStatProbeUnexercised
}

func (*statProbeRegistry) List(
	context.Context, string, int, string,
) ([]resourcespec.ResourceRow, string, error) {
	return nil, "", errStatProbeUnexercised
}

func (*statProbeRegistry) ReservationDaemon(context.Context, string) (string, bool, error) {
	return "", false, errStatProbeUnexercised
}

func (*statProbeRegistry) TombstoneDaemon(context.Context, string) (string, bool, error) {
	return "", false, errStatProbeUnexercised
}

func (*statProbeRegistry) ListReservationsByDaemon(
	context.Context, string,
) ([]resourcespec.ReservationRow, error) {
	return nil, errStatProbeUnexercised
}

func (*statProbeRegistry) ListTombstonesByDaemon(
	context.Context, string,
) ([]resourcespec.TombstoneRow, error) {
	return nil, errStatProbeUnexercised
}

func (*statProbeRegistry) ListByPlacementDaemon(
	context.Context, string,
) ([]resourcespec.ResourceRow, error) {
	return nil, errStatProbeUnexercised
}

func (*statProbeRegistry) SweepExpiredReservations(
	context.Context, string, int64,
) ([]resourcespec.ReservationRow, error) {
	return nil, errStatProbeUnexercised
}

func (*statProbeRegistry) TouchReservationsByCoords(context.Context, string, []string, int64) error {
	return errStatProbeUnexercised
}

// statProbeDriver / statProbeState exist only because the door fail-fasts at
// assembly on a missing byte realizer; Stat never reaches either.
type statProbeDriver struct{}

func (statProbeDriver) Read(context.Context, resource.ResourceID) ([]byte, bool, error) {
	return nil, false, errStatProbeUnexercised
}
func (statProbeDriver) Write(context.Context, resource.ResourceID, []byte) error {
	return errStatProbeUnexercised
}
func (statProbeDriver) Delete(context.Context, resource.ResourceID) error {
	return errStatProbeUnexercised
}

type statProbeState struct{}

func (statProbeState) Create(context.Context, actor.ActorID, resource.ResourceID, []byte) error {
	return errStatProbeUnexercised
}
func (statProbeState) Read(context.Context, actor.ActorID, resource.ResourceID) ([]byte, bool, error) {
	return nil, false, errStatProbeUnexercised
}
func (statProbeState) Write(context.Context, actor.ActorID, resource.ResourceID, []byte) (bool, error) {
	return false, errStatProbeUnexercised
}
func (statProbeState) Delete(context.Context, actor.ActorID, resource.ResourceID) (bool, error) {
	return false, errStatProbeUnexercised
}

// statProbeFacts is an ordinary active member — NOT an owner, whose root
// authority would grant every op and dissolve the whole scenario.
type statProbeFacts struct{}

func (statProbeFacts) ResourceActorFacts(
	context.Context,
	actor.ActorID,
) (storespec.ResourceActorFacts, error) {
	return storespec.ResourceActorFacts{Active: true}, nil
}

var errStatProbeUnexercised = errStatProbe("stat masquerade rig: arm unexercised")

type errStatProbe string

func (e errStatProbe) Error() string { return string(e) }

// --- the two avatars ---------------------------------------------------------

const (
	statProbeVisible   = resource.ResourceID("res:visible-to-me")
	statProbeForbidden = resource.ResourceID("res:exists-but-not-mine")
	statProbeAbsent    = resource.ResourceID("res:no-such-thing")
)

// newStatMasqueradeAvatars builds one real door and returns its two faces: the
// in-process handle a local cell holds, and the wire proxy an out-of-process
// cell holds, whose home end runs that very handle (the same delegation
// remoteingress.Access's Stat arm performs).
func newStatMasqueradeAvatars(t *testing.T) (accessdoor.ResourceAccessHandle, accessdoor.ResourceAccessHandle) {
	t.Helper()
	minter, err := accessdoor.New(accessdoor.Deps{
		Registry: &statProbeRegistry{
			rows: map[resource.ResourceID]resourcespec.ResourceMeta{
				statProbeVisible: {
					Kind: resourcespec.KindKV, CreatedAt: 1700000000001, CreatedBy: "agent:creator",
				},
				statProbeForbidden: {
					Kind: resourcespec.KindKV, CreatedAt: 1700000000002, CreatedBy: "agent:someone-else",
				},
			},
			grant: map[resource.ResourceID][]access.Operation{
				statProbeVisible: {access.OpRead},
			},
		},
		Drivers:   accessdoor.DriverTable{resourcespec.KindKV: statProbeDriver{}},
		Authority: statProbeFacts{},
		State:     statProbeState{},
	})
	if err != nil {
		t.Fatalf("assemble door: %v", err)
	}
	local := minter.MintAuthority(statProbeAuthority{id: "agent:wire-arm"})

	ing := &wireArmIngress{}
	ing.setAccess(func(req remoteingress.AccessRequest) (remoteingress.AccessResponse, error) {
		if req.Kind != remoteingress.AccessStat {
			return remoteingress.AccessResponse{}, remoteingress.ErrInvalidRequest
		}
		result, statErr := local.Stat(context.Background(), req.Resource)
		return remoteingress.AccessResponse{Stat: result}, statErr
	})
	rig := newWireArmRig(t, ing)
	return local, rig.access
}

// The masquerade survives the wire in both directions: each avatar reports the
// same thing the other does, and from the wire "you have no rights" is
// byte-for-byte the same answer as "there is no such resource".
func TestWireStatZeroRightsMasqueradesAsNotFoundOnBothAvatars(t *testing.T) {
	t.Parallel()
	local, wire := newStatMasqueradeAvatars(t)
	ctx := context.Background()

	statBoth := func(id resource.ResourceID) (accessdoor.StatResult, accessdoor.StatResult) {
		t.Helper()
		localResult, err := local.Stat(ctx, id)
		if err != nil {
			t.Fatalf("local Stat(%s): %v", id, err)
		}
		// A Go error here would itself be a side channel: a caller could tell
		// the two cases apart by whether the call failed.
		wireResult, err := wire.Stat(ctx, id)
		if err != nil {
			t.Fatalf("wire Stat(%s) returned a Go error for a reject VERDICT: %v", id, err)
		}
		return localResult, wireResult
	}

	localForbidden, wireForbidden := statBoth(statProbeForbidden)
	localAbsent, wireAbsent := statBoth(statProbeAbsent)

	// Precondition (the door's own contract, restated so a regression there
	// cannot masquerade as a wire success).
	if localForbidden.Reject != accessdoor.QueryNotFound ||
		!reflect.DeepEqual(localForbidden, localAbsent) {
		t.Fatalf("the door itself distinguishes zero-rights (%+v) from absent (%+v)",
			localForbidden, localAbsent)
	}

	// Avatar parity: the wire proxy reports exactly what the local handle does.
	if !reflect.DeepEqual(wireForbidden, localForbidden) {
		t.Fatalf("zero-rights: wire avatar = %+v, local avatar = %+v", wireForbidden, localForbidden)
	}
	if !reflect.DeepEqual(wireAbsent, localAbsent) {
		t.Fatalf("absent: wire avatar = %+v, local avatar = %+v", wireAbsent, localAbsent)
	}

	// The security property itself, judged from the wire alone.
	if !reflect.DeepEqual(wireForbidden, wireAbsent) {
		t.Fatalf("over the wire, a forbidden resource (%+v) is distinguishable from an absent one (%+v)",
			wireForbidden, wireAbsent)
	}
	if wireForbidden.Reject != accessdoor.QueryNotFound {
		t.Fatalf("wire reject = %q, want %q", wireForbidden.Reject, accessdoor.QueryNotFound)
	}
	// Nothing about the hidden resource leaks through the payload fields: no
	// kind, no creator, no creation time, no ops.
	if wireForbidden.Meta != (accessdoor.StatMeta{}) {
		t.Fatalf("a zero-rights Stat carried metadata over the wire: %+v", wireForbidden.Meta)
	}
	if len(wireForbidden.Ops) != 0 {
		t.Fatalf("a zero-rights Stat carried ops over the wire: %v", wireForbidden.Ops)
	}
}

// Positive control: with rights, the wire carries the full projection — so the
// indistinguishability above is a property of the VERDICT, not of a wire that
// throws every Stat away.
func TestWireStatWithRightsCarriesTheFullProjection(t *testing.T) {
	t.Parallel()
	local, wire := newStatMasqueradeAvatars(t)
	ctx := context.Background()

	localResult, err := local.Stat(ctx, statProbeVisible)
	if err != nil {
		t.Fatalf("local Stat: %v", err)
	}
	wireResult, err := wire.Stat(ctx, statProbeVisible)
	if err != nil {
		t.Fatalf("wire Stat: %v", err)
	}
	if localResult.Reject != "" || len(localResult.Ops) == 0 {
		t.Fatalf("a granted Stat was rejected by the door: %+v", localResult)
	}
	if !reflect.DeepEqual(wireResult, localResult) {
		t.Fatalf("granted Stat: wire avatar = %+v, local avatar = %+v", wireResult, localResult)
	}
	if wireResult.Meta.CreatedBy != "agent:creator" || wireResult.Meta.CreatedAt != 1700000000001 {
		t.Fatalf("the wire lost the projection's meta: %+v", wireResult.Meta)
	}
	if len(wireResult.Ops) != 1 || wireResult.Ops[0] != access.OpRead {
		t.Fatalf("the wire lost the effective op set: %v", wireResult.Ops)
	}
}
