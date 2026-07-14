package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// TestIngressStateRules exercises the four-step actor-scoped ingress sequence,
// pinning the divergence from the channel-scoped ingress: set is a CATEGORY error
// (ErrOpNotInScope), never ErrMalformed, and never a verdict.
func TestIngressStateRules(t *testing.T) {
	grant := func(ops ...access.Operation) *access.Grant {
		return &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: ops}
	}

	tests := []struct {
		name string
		op   access.Operation
		id   resource.ResourceID
		args []byte
		gr   *access.Grant
		// exactly one of the following is expected:
		wantMalformed  bool
		wantNotInScope bool
	}{
		// ① closed set — garbage op is a wire-shape fault, NOT ErrOpNotInScope.
		{name: "garbage op → malformed", op: access.Operation("frobnicate"), id: "k", wantMalformed: true},

		// ② empty id.
		{name: "empty id → malformed", op: access.OpCreate, id: "", wantMalformed: true},

		// ③ set is a category error regardless of grant shape.
		{name: "set without grant → not in scope", op: access.OpSet, id: "k", wantNotInScope: true},
		{name: "set with grant → not in scope", op: access.OpSet, id: "k", gr: grant(access.OpRead), wantNotInScope: true},
		{name: "set with bogus grant → not in scope (not malformed)", op: access.OpSet, id: "k",
			gr: &access.Grant{GranteeKind: "role", Ops: []access.Operation{access.OpCreate}}, wantNotInScope: true},

		// ④ op×shape: no grant on this locus; delete is by-id.
		{name: "create with grant → malformed", op: access.OpCreate, id: "k", gr: grant(access.OpRead), wantMalformed: true},
		{name: "read with grant → malformed", op: access.OpRead, id: "k", gr: grant(access.OpRead), wantMalformed: true},
		{name: "write with grant → malformed", op: access.OpWrite, id: "k", gr: grant(access.OpRead), wantMalformed: true},
		{name: "delete with grant → malformed", op: access.OpDelete, id: "k", gr: grant(access.OpRead), wantMalformed: true},
		{name: "delete with args → malformed", op: access.OpDelete, id: "k", args: []byte("x"), wantMalformed: true},

		// well-formed shapes.
		{name: "create ok", op: access.OpCreate, id: "k", args: []byte("v")},
		{name: "create empty args ok", op: access.OpCreate, id: "k", args: []byte{}},
		{name: "read ok", op: access.OpRead, id: "k"},
		{name: "read selector args tolerated", op: access.OpRead, id: "k", args: []byte("sel")},
		{name: "write ok", op: access.OpWrite, id: "k", args: []byte("v")},
		{name: "delete no args ok", op: access.OpDelete, id: "k"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ingressState(tc.op, tc.id, tc.args, tc.gr)
			switch {
			case tc.wantMalformed:
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("err = %v, want ErrMalformed", err)
				}
				// negative assertion: malformed is NOT the category error.
				if errors.Is(err, ErrOpNotInScope) {
					t.Fatalf("err = ErrOpNotInScope, want ErrMalformed (wire-shape fault ≠ category error)")
				}
			case tc.wantNotInScope:
				if !errors.Is(err, ErrOpNotInScope) {
					t.Fatalf("err = %v, want ErrOpNotInScope", err)
				}
				// negative assertion: the category error is NOT a bad-wire fault.
				if errors.Is(err, ErrMalformed) {
					t.Fatalf("err = ErrMalformed, want ErrOpNotInScope (category error ≠ wire-shape fault)")
				}
			default:
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
			}
		})
	}
}

// TestInvokeActorScopedTree walks the collapsed decision tree branch by branch,
// asserting the verdict mapping and the welded owner reaching the StateStore.
func TestInvokeActorScopedTree(t *testing.T) {
	const owner = actor.ActorID("o")
	const id = resource.ResourceID("k")

	t.Run("create ok", func(t *testing.T) {
		st := &fakeStateStore{}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpCreate, id, []byte("v"))
		mustAccept(t, out, err)
		if len(st.createCalls) != 1 || st.createCalls[0].owner != owner || st.createCalls[0].id != id {
			t.Fatalf("create call = %+v", st.createCalls)
		}
	})

	t.Run("create collision → already_exists", func(t *testing.T) {
		st := &fakeStateStore{createErr: resourcespec.ErrAlreadyExists}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpCreate, id, nil)
		mustVerdict(t, out, err, access.AlreadyExists)
	})

	t.Run("create under inactive owner → owner_inactive", func(t *testing.T) {
		st := &fakeStateStore{createErr: resourcespec.ErrOwnerInactive}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpCreate, id, nil)
		mustVerdict(t, out, err, access.OwnerInactive)
	})

	t.Run("read existing row with bytes → Found:true", func(t *testing.T) {
		st := &fakeStateStore{readPresent: true, readValue: []byte("v")}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpRead, id, nil)
		mustAccept(t, out, err)
		if !out.Found || string(out.Value) != "v" {
			t.Fatalf("out = %+v, want Found:true Value:v", out)
		}
	})

	t.Run("read existing row, NULL bytes → resolved-but-empty (Found:false)", func(t *testing.T) {
		st := &fakeStateStore{readPresent: true, readValue: nil}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpRead, id, nil)
		mustAccept(t, out, err)
		if out.Found {
			t.Fatalf("out.Found = true, want false (existing row + NULL bytes = resolved-but-empty)")
		}
	})

	t.Run("read existing row, empty non-nil bytes → Found:true", func(t *testing.T) {
		st := &fakeStateStore{readPresent: true, readValue: []byte{}}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpRead, id, nil)
		mustAccept(t, out, err)
		if !out.Found {
			t.Fatalf("out.Found = false, want true (empty non-nil bytes are a value)")
		}
	})

	t.Run("read absent → resource_not_found", func(t *testing.T) {
		st := &fakeStateStore{readPresent: false}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpRead, id, nil)
		mustVerdict(t, out, err, access.ResourceNotFound)
	})

	t.Run("write existing → ok", func(t *testing.T) {
		st := &fakeStateStore{writePresent: true}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpWrite, id, []byte("v"))
		mustAccept(t, out, err)
		if len(st.writeCalls) != 1 || string(st.writeCalls[0].bytes) != "v" {
			t.Fatalf("write call = %+v", st.writeCalls)
		}
	})

	t.Run("write absent → resource_not_found", func(t *testing.T) {
		st := &fakeStateStore{writePresent: false}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpWrite, id, []byte("v"))
		mustVerdict(t, out, err, access.ResourceNotFound)
	})

	t.Run("delete existing → ok", func(t *testing.T) {
		st := &fakeStateStore{deletePresent: true}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpDelete, id, nil)
		mustAccept(t, out, err)
		if len(st.deleteCalls) != 1 {
			t.Fatalf("delete calls = %d, want 1", len(st.deleteCalls))
		}
	})

	t.Run("delete absent → resource_not_found", func(t *testing.T) {
		st := &fakeStateStore{deletePresent: false}
		d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
		out, err := d.invokeActorScoped(t.Context(), owner, access.OpDelete, id, nil)
		mustVerdict(t, out, err, access.ResourceNotFound)
	})
}

// TestInvokeActorScopedDriverError demonstrates that an executor failure on
// each mutating/reading op maps to a driver_error VERDICT (nil Go error), never a
// Go error — folding it into a Go error would leave driver_error unproducible.
func TestInvokeActorScopedDriverError(t *testing.T) {
	const owner = actor.ActorID("o")
	const id = resource.ResourceID("k")
	boom := errors.New("store broken")

	cases := []struct {
		name string
		st   *fakeStateStore
		op   access.Operation
		args []byte
	}{
		{name: "create", st: &fakeStateStore{createErr: boom}, op: access.OpCreate, args: []byte("v")},
		{name: "read", st: &fakeStateStore{readErr: boom}, op: access.OpRead},
		{name: "write", st: &fakeStateStore{writeErr: boom}, op: access.OpWrite, args: []byte("v")},
		{name: "delete", st: &fakeStateStore{deleteErr: boom}, op: access.OpDelete},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newStateDoor(tc.st, &fakeRegistry{}, &fakeMembership{})
			out, err := d.invokeActorScoped(t.Context(), owner, tc.op, id, tc.args)
			mustVerdict(t, out, err, access.DriverError)
		})
	}
}

// TestExecuteFailureCallerCancellation pins the executeFailure split: when the
// request ctx is already cancelled, an EXECUTE-stage store
// failure is the CALLER'S OWN hand, not a resource-plane fact — it surfaces as a
// Go error, never a driver_error verdict (which would tell the caller "your
// driver broke" about a cancellation it issued itself). Uniform across both
// trees; this exercises the collapsed branch.
func TestExecuteFailureCallerCancellation(t *testing.T) {
	const owner = actor.ActorID("o")
	const id = resource.ResourceID("k")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	st := &fakeStateStore{readErr: cancelled.Err()}
	d := newStateDoor(st, &fakeRegistry{}, &fakeMembership{})
	out, err := d.invokeActorScoped(cancelled, owner, access.OpRead, id, nil)
	if err == nil {
		t.Fatalf("cancelled ctx: got verdict %+v, want a Go error (caller's own cancellation is not a resource-plane verdict)", out)
	}
	if out.RejectReason != "" {
		t.Fatalf("cancelled ctx: got RejectReason %q alongside error, want zero Outcome", out.RejectReason)
	}
}

// TestActorScopedNeverConsultsChannelPlane is the trio of negative assertions the
// scope law demands: the collapsed branch consults NO membership and NO Registry,
// and never produces access_denied (there is no possible world in which the owner
// is denied its own namespace).
func TestActorScopedNeverConsultsChannelPlane(t *testing.T) {
	const owner = actor.ActorID("o")
	const id = resource.ResourceID("k")

	ops := []struct {
		op   access.Operation
		st   *fakeStateStore
		args []byte
	}{
		{op: access.OpCreate, st: &fakeStateStore{}, args: []byte("v")},
		{op: access.OpRead, st: &fakeStateStore{readPresent: true, readValue: []byte("v")}},
		{op: access.OpWrite, st: &fakeStateStore{writePresent: true}, args: []byte("v")},
		{op: access.OpDelete, st: &fakeStateStore{deletePresent: true}},
	}
	for _, tc := range ops {
		t.Run(string(tc.op), func(t *testing.T) {
			reg := &fakeRegistry{}
			mem := &fakeMembership{}
			d := newStateDoor(tc.st, reg, mem)
			out, err := d.invokeActorScoped(t.Context(), owner, tc.op, id, tc.args)
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}
			if out.RejectReason == access.AccessDenied {
				t.Fatalf("collapsed branch produced access_denied — impossible verdict at owner-only locus")
			}
			if reg.calls != 0 {
				t.Fatalf("Registry consulted %d times, want 0 (no R at this locus)", reg.calls)
			}
			if mem.calls != 0 {
				t.Fatalf("Membership consulted %d times, want 0 (no membership at this locus)", mem.calls)
			}
		})
	}
}

// TestMintStateWeldsOwner: MintState hands back a handle whose welded owner is the
// namespace coordinate reaching the StateStore — never read off the Invoke call.
func TestMintStateWeldsOwner(t *testing.T) {
	st := &fakeStateStore{}
	m, err := New(Deps{
		Registry:   &fakeRegistry{},
		Drivers:    DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Membership: &fakeMembership{},
		State:      st,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := m.MintState("owner-a")

	// set on an actor-scoped handle → ErrOpNotInScope, StateStore never touched.
	if _, err := h.Invoke(t.Context(), access.OpSet, "k", nil,
		&access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead}}); !errors.Is(err, ErrOpNotInScope) {
		t.Fatalf("set err = %v, want ErrOpNotInScope", err)
	}
	if len(st.createCalls)+len(st.readCalls)+len(st.writeCalls)+len(st.deleteCalls) != 0 {
		t.Fatalf("StateStore touched on an out-of-scope op")
	}

	// well-formed create → welded owner reaches Create.
	out, err := h.Invoke(t.Context(), access.OpCreate, "k", []byte("v"), nil)
	mustAccept(t, out, err)
	if len(st.createCalls) != 1 || st.createCalls[0].owner != "owner-a" {
		t.Fatalf("create call = %+v, want owner-a welded", st.createCalls)
	}
}
