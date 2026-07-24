package accessdoor

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// TestNewFailFast: assembly-time validation rejects an incomplete Deps and a
// missing day-1 KindKV driver — never deferring the failure to first op.
func TestNewFailFast(t *testing.T) {
	full := Deps{
		Registry:  &fakeRegistry{},
		Drivers:   DriverTable{resourcespec.KindKV: &fakeDriver{}},
		Authority: &fakeMembership{},
		State:     &fakeStateStore{},
	}

	t.Run("complete Deps assembles", func(t *testing.T) {
		if _, err := New(full); err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
	})

	t.Run("missing Registry", func(t *testing.T) {
		d := full
		d.Registry = nil
		if _, err := New(d); err == nil {
			t.Fatalf("expected error for nil Registry")
		}
	})

	t.Run("missing Authority", func(t *testing.T) {
		d := full
		d.Authority = nil
		if _, err := New(d); err == nil {
			t.Fatalf("expected error for nil Membership")
		}
	})

	t.Run("missing State", func(t *testing.T) {
		d := full
		d.State = nil
		if _, err := New(d); err == nil {
			t.Fatalf("expected error for nil State")
		}
	})

	t.Run("nil Drivers table", func(t *testing.T) {
		d := full
		d.Drivers = nil
		if _, err := New(d); err == nil {
			t.Fatalf("expected error for nil Drivers")
		}
	})

	t.Run("Drivers table without KindKV", func(t *testing.T) {
		d := full
		d.Drivers = DriverTable{resourcespec.ResourceKind("file"): &fakeDriver{}}
		if _, err := New(d); err == nil {
			t.Fatalf("expected error when KindKV driver is missing")
		}
	})
}

func TestAccessRejectsInvalidAdmission(t *testing.T) {
	reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
	authority := &fakeMembership{isMember: true}
	m, err := New(Deps{Registry: reg, Drivers: DriverTable{resourcespec.KindKV: &fakeDriver{}}, Authority: authority, State: &fakeStateStore{}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.MintAdmitted(storespec.IdentityAdmission{}).Invoke(context.Background(), access.OpRead, "r", nil, nil)
	if !errors.Is(err, ErrAuthorInactive) {
		t.Fatalf("inactive author outcome=(%+v,%v)", out, err)
	}
}

// TestMintedHandleRunsFullPath: a handle from the sealed Minter runs ingress +
// overreach + tree, and welds its caller into the create/authorization checks.
func TestMintedHandleRunsFullPath(t *testing.T) {
	reg := &fakeRegistry{}
	drv := &fakeDriver{}
	mem := &fakeMembership{isMember: true}
	m, err := New(Deps{Registry: reg, Drivers: DriverTable{resourcespec.KindKV: drv}, Authority: mem, State: &fakeStateStore{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := m.MintAdmitted(accessAdmission("a"))

	// malformed (set without grant) → Go error, tree never reached.
	if _, err := h.Invoke(t.Context(), access.OpSet, "r1", nil, nil); err == nil {
		t.Fatalf("expected ErrMalformed for set with nil grant")
	}
	if len(reg.createCalls) != 0 {
		t.Fatalf("no store call should occur on a malformed request")
	}

	// a bare op=create through Invoke is rejected outright (期11 §3.1 "create
	// 单入口") — the resource face has exactly one create locus, the Create
	// method below.
	if _, err := h.Invoke(t.Context(), access.OpCreate, "r1", []byte("v"), nil); !errors.Is(err, ErrCreateViaInvoke) {
		t.Fatalf("expected ErrCreateViaInvoke for a bare op=create through Invoke, got %v", err)
	}
	if len(reg.createCalls) != 0 {
		t.Fatalf("no store call should occur for a bare Invoke(op=create)")
	}

	// well-formed create via the Create method → welded caller reaches
	// Registry.Create.
	out, err := h.Create(t.Context(), "r1", resourcespec.CreateSpec{Kind: resourcespec.KindKV}, []byte("v"))
	mustAccept(t, out, err)
	if len(reg.createCalls) != 1 || reg.createCalls[0].creator != "a" {
		t.Fatalf("create call = %+v", reg.createCalls)
	}
	if _, _, err := h.Open(t.Context(), resource.ResourceID("file:server"), access.OpRead); !errors.Is(err, ErrFileCapabilityUnavailable) {
		t.Fatalf("server file face err=%v, want capability unavailable", err)
	}
}
