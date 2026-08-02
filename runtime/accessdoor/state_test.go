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

// TestIngressStateRules exercises the actor-scoped ingress sequence — the
// SAME named checks the channel-scoped ingress runs.
func TestIngressStateRules(t *testing.T) {
	tests := []struct {
		name          string
		op            access.Operation
		id            resource.ResourceID
		args          []byte
		wantMalformed bool
	}{
		// ① closed set.
		{name: "garbage op → malformed", op: access.Operation("frobnicate"), id: "k", wantMalformed: true},
		{name: "retired set verb → malformed (out of set)", op: access.Operation("set"), id: "k", wantMalformed: true},

		// ② empty id.
		{name: "empty id → malformed", op: access.OpCreate, id: "", wantMalformed: true},

		// ③ op×shape: delete is by-id.
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
			err := ingressState(tc.op, tc.id, tc.args)
			if tc.wantMalformed {
				if !errors.Is(err, ErrMalformed) {
					t.Fatalf("err = %v, want ErrMalformed", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
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
		Registry:  &fakeRegistry{},
		Drivers:   DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: &fakeMembership{},
		State:     st,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := m.MintStateAuthority(accessAuthority("owner-a"))

	// an out-of-set op → ErrMalformed, StateStore never touched.
	if _, err := h.Invoke(t.Context(), access.Operation("bogus"), "k", nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("bogus op err = %v, want ErrMalformed", err)
	}
	if len(st.createCalls)+len(st.readCalls)+len(st.writeCalls)+len(st.deleteCalls) != 0 {
		t.Fatalf("StateStore touched on a malformed op")
	}

	// well-formed create → welded owner reaches Create.
	out, err := h.Invoke(t.Context(), access.OpCreate, "k", []byte("v"))
	mustAccept(t, out, err)
	if len(st.createCalls) != 1 || st.createCalls[0].owner != "owner-a" {
		t.Fatalf("create call = %+v, want owner-a welded", st.createCalls)
	}
}
