package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/access"
	"github.com/wanpengxie/ActOS/runtime/resourcespec"
)

// --- create branch ---

func TestInvokeCreate(t *testing.T) {
	t.Run("existing id rejects already_exists", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV()}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "a", access.OpCreate, "r1", []byte("v"), nil)
		mustVerdict(t, out, err, access.AlreadyExists)
		if len(reg.createCalls) != 0 {
			t.Fatalf("Create must not run when id exists")
		}
	})

	t.Run("non-member rejects access_denied", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
		out, err := d.invoke(t.Context(), "x", access.OpCreate, "r1", nil, nil)
		mustVerdict(t, out, err, access.AccessDenied)
		if len(reg.createCalls) != 0 {
			t.Fatalf("Create must not run for a non-member")
		}
	})

	t.Run("member creates with KindKV and initial bytes", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "a", access.OpCreate, "r1", []byte("hi"), nil)
		mustAccept(t, out, err)
		if len(reg.createCalls) != 1 {
			t.Fatalf("expected one Create call, got %d", len(reg.createCalls))
		}
		got := reg.createCalls[0]
		if got.kind != resourcespec.KindKV || got.creator != "a" || string(got.initial) != "hi" {
			t.Fatalf("Create args = %+v", got)
		}
	})

	t.Run("Create ErrAlreadyExists maps to already_exists verdict", func(t *testing.T) {
		reg := &fakeRegistry{createErr: resourcespec.ErrAlreadyExists}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "a", access.OpCreate, "r1", nil, nil)
		mustVerdict(t, out, err, access.AlreadyExists)
	})

	t.Run("Create other error maps to driver_error verdict", func(t *testing.T) {
		reg := &fakeRegistry{createErr: errors.New("boom")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "a", access.OpCreate, "r1", nil, nil)
		mustVerdict(t, out, err, access.DriverError)
	})

	t.Run("membership error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{err: errors.New("mem down")})
		_, err := d.invoke(t.Context(), "a", access.OpCreate, "r1", nil, nil)
		if err == nil {
			t.Fatalf("expected Go error on membership failure")
		}
	})
}

// --- resolve / not-found for object ops ---

func TestInvokeObjectOpNotFound(t *testing.T) {
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpDelete} {
		reg := &fakeRegistry{resolveExists: false}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		out, err := d.invoke(t.Context(), "a", op, "r1", nil, nil)
		mustVerdict(t, out, err, access.ResourceNotFound)
	}
	// set too (needs a grant operand, but resolve happens first).
	reg := &fakeRegistry{resolveExists: false}
	d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
	g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead}}
	out, err := d.invoke(t.Context(), "a", access.OpSet, "r1", nil, g)
	mustVerdict(t, out, err, access.ResourceNotFound)
}

// --- A8 union authorization ---

func TestInvokeAuthorizationUnion(t *testing.T) {
	t.Run("actor entry allows", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		d := newDoor(reg, &fakeDriver{readFound: true, readValue: []byte("v")}, &fakeMembership{})
		out, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
		if string(out.Value) != "v" {
			t.Fatalf("value = %q", out.Value)
		}
	})

	t.Run("members entry allows a current member", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: false, membersAllow: true}
		d := newDoor(reg, &fakeDriver{readFound: true}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "c", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
	})

	t.Run("members entry denies a non-member", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: false, membersAllow: true}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: false})
		out, err := d.invoke(t.Context(), "c", access.OpRead, "r1", nil, nil)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("no entry denies", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: false, membersAllow: false}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{isMember: true})
		out, err := d.invoke(t.Context(), "b", access.OpRead, "r1", nil, nil)
		mustVerdict(t, out, err, access.AccessDenied)
	})

	t.Run("ActorAllows error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllowsErr: errors.New("x")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})

	t.Run("MembersAllow error is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), membersAllowErr: errors.New("x")}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
		_, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})

	t.Run("IsMember error during union is a Go error", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), membersAllow: true}
		d := newDoor(reg, &fakeDriver{}, &fakeMembership{err: errors.New("x")})
		_, err := d.invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		if err == nil {
			t.Fatalf("expected Go error")
		}
	})
}

// --- execute side-effects for each op ---

func TestInvokeExecuteEffects(t *testing.T) {
	t.Run("read returns driver value and found", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{readValue: []byte("payload"), readFound: true}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
		if !out.Found || string(out.Value) != "payload" {
			t.Fatalf("out = %+v", out)
		}
	})

	t.Run("read resolved-but-empty is accepted with found=false", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{readFound: false}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustAccept(t, out, err)
		if out.Found {
			t.Fatalf("found should be false")
		}
	})

	t.Run("write hits the driver", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpWrite, "r1", []byte("new"), nil)
		mustAccept(t, out, err)
		if len(drv.writeCalls) != 1 || string(drv.writeCalls[0]) != "new" {
			t.Fatalf("write calls = %v", drv.writeCalls)
		}
	})

	t.Run("set hits the registry not the driver", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{}
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead}}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpSet, "r1", nil, g)
		mustAccept(t, out, err)
		if len(reg.setGrants) != 1 || reg.setGrants[0].Grantee != "b" {
			t.Fatalf("set grants = %v", reg.setGrants)
		}
	})

	t.Run("delete orders driver before registry", func(t *testing.T) {
		reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
		drv := &fakeDriver{}
		out, err := newDoor(reg, drv, &fakeMembership{}).invoke(t.Context(), "a", access.OpDelete, "r1", nil, nil)
		mustAccept(t, out, err)
		if drv.deleteCalls != 1 || len(reg.deleteCalls) != 1 {
			t.Fatalf("driver deletes=%d registry deletes=%d", drv.deleteCalls, len(reg.deleteCalls))
		}
	})
}

// --- driver_error materialization: EXECUTE failures are verdicts, not Go errors ---

func TestInvokeDriverErrorVerdict(t *testing.T) {
	base := func() *fakeRegistry {
		return &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
	}

	t.Run("read error", func(t *testing.T) {
		out, err := newDoor(base(), &fakeDriver{readErr: errors.New("io")}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("write error", func(t *testing.T) {
		out, err := newDoor(base(), &fakeDriver{writeErr: errors.New("io")}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpWrite, "r1", []byte("v"), nil)
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("delete driver error", func(t *testing.T) {
		reg := base()
		out, err := newDoor(reg, &fakeDriver{deleteErr: errors.New("io")}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpDelete, "r1", nil, nil)
		mustVerdict(t, out, err, access.DriverError)
		if len(reg.deleteCalls) != 0 {
			t.Fatalf("registry Delete must not run after a driver Delete failure")
		}
	})
	t.Run("delete registry error", func(t *testing.T) {
		reg := base()
		reg.deleteErr = errors.New("db")
		out, err := newDoor(reg, &fakeDriver{}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpDelete, "r1", nil, nil)
		mustVerdict(t, out, err, access.DriverError)
	})
	t.Run("set registry error", func(t *testing.T) {
		reg := base()
		reg.setGrantErr = errors.New("db")
		g := &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead}}
		out, err := newDoor(reg, &fakeDriver{}, &fakeMembership{}).
			invoke(t.Context(), "a", access.OpSet, "r1", nil, g)
		mustVerdict(t, out, err, access.DriverError)
	})
}

// --- assembly defects surface as Go errors (not verdicts) ---

func TestInvokeResolveErrorIsGoError(t *testing.T) {
	reg := &fakeRegistry{resolveErr: errors.New("db down")}
	_, err := newDoor(reg, &fakeDriver{}, &fakeMembership{}).invoke(t.Context(), "a", access.OpRead, "r1", nil, nil)
	if err == nil {
		t.Fatalf("resolve failure must be a Go error")
	}
}

func TestInvokeMissingDriverIsGoError(t *testing.T) {
	// Resolve returns a kind with no registered driver → assembly defect.
	reg := &fakeRegistry{resolveExists: true, resolveMeta: resourcespec.ResourceMeta{Kind: resourcespec.ResourceKind("file")}, actorAllows: true}
	d := &door{deps: Deps{
		Registry:   reg,
		Drivers:    DriverTable{resourcespec.KindKV: &fakeDriver{}}, // KindKV present, "file" absent
		Membership: &fakeMembership{},
	}}
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpDelete} {
		_, err := d.invoke(context.Background(), "a", op, "r1", nil, nil)
		if err == nil {
			t.Fatalf("op %q with no driver must be a Go error", op)
		}
	}
}

// mustVerdict asserts an accepted-error-free reject with the given reason.
func mustVerdict(t *testing.T, out Outcome, err error, want access.FailureReason) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if out.RejectReason != want {
		t.Fatalf("reason = %q, want %q", out.RejectReason, want)
	}
	if out.Accepted() {
		t.Fatalf("Accepted() = true, want reject %q", want)
	}
}

// mustAccept asserts an accepted, error-free outcome.
func mustAccept(t *testing.T, out Outcome, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected Go error: %v", err)
	}
	if !out.Accepted() {
		t.Fatalf("Accepted() = false, reason %q", out.RejectReason)
	}
}
