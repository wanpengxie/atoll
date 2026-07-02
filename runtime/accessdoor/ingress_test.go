package accessdoor

import (
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// TestIngressRules exercises every ingress structural rule; each malformed shape
// must surface as ErrMalformed (a protocol error, never a verdict).
func TestIngressRules(t *testing.T) {
	grant := func(k access.GranteeKind, g actor.ActorID, ops ...access.Operation) *access.Grant {
		return &access.Grant{GranteeKind: k, Grantee: g, Ops: ops}
	}

	tests := []struct {
		name    string
		op      access.Operation
		id      resource.ResourceID
		args    []byte
		grant   *access.Grant
		wantErr bool
	}{
		{name: "op not in closed set", op: access.Operation("frobnicate"), wantErr: true},

		// resource id: an invocation without an object is structurally not an access.
		{name: "empty resource id", op: access.OpCreate, id: "", wantErr: true},

		// op×Args: delete/set carry no Args.
		{name: "delete with args", op: access.OpDelete, args: []byte("x"), wantErr: true},
		{name: "delete no args ok", op: access.OpDelete},
		{name: "set with args", op: access.OpSet, args: []byte("x"), grant: grant(access.GranteeActor, "b", access.OpRead), wantErr: true},
		{name: "create empty args ok", op: access.OpCreate, args: []byte{}},
		{name: "read selector args ignored not rejected", op: access.OpRead, args: []byte("selector")},

		// op×Grant: set ⟺ Grant present.
		{name: "set with nil grant", op: access.OpSet, wantErr: true},
		{name: "write with grant", op: access.OpWrite, grant: grant(access.GranteeActor, "b", access.OpRead), wantErr: true},
		{name: "create with grant", op: access.OpCreate, grant: grant(access.GranteeActor, "b", access.OpRead), wantErr: true},

		// ValidateGrant structure.
		{name: "grantee_kind out of set", op: access.OpSet, grant: &access.Grant{GranteeKind: "role", Ops: []access.Operation{access.OpRead}}, wantErr: true},
		{name: "actor kind empty grantee", op: access.OpSet, grant: grant(access.GranteeActor, "", access.OpRead), wantErr: true},
		{name: "members kind non-empty grantee", op: access.OpSet, grant: grant(access.GranteeMembers, "b", access.OpRead), wantErr: true},
		{name: "grant ops contain create", op: access.OpSet, grant: grant(access.GranteeActor, "b", access.OpCreate), wantErr: true},
		{name: "grant op out of set", op: access.OpSet, grant: &access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.Operation("bogus")}}, wantErr: true},

		// well-formed set (structurally; overreach is a separate step).
		{name: "well-formed actor grant", op: access.OpSet, grant: grant(access.GranteeActor, "b", access.OpRead, access.OpWrite)},
		{name: "well-formed members grant", op: access.OpSet, grant: grant(access.GranteeMembers, "", access.OpRead)},
		// structurally legal even with set/delete ops (day1OpsOverreach rejects, not ingress).
		{name: "grant set/delete ops structurally ok", op: access.OpSet, grant: grant(access.GranteeActor, "b", access.OpSet, access.OpDelete)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := tc.id
			if id == "" && tc.name != "empty resource id" {
				id = "kv:x" // default well-formed object for cases probing other rules
			}
			err := ingress(tc.op, id, tc.args, tc.grant)
			if tc.wantErr {
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

// TestDay1OpsOverreach: granting anything beyond {read,write} is overreach; the
// step applies only to op=set.
func TestDay1OpsOverreach(t *testing.T) {
	tests := []struct {
		name     string
		op       access.Operation
		grant    *access.Grant
		wantOver bool
		wantOK   bool
	}{
		{name: "non-set op does not apply", op: access.OpRead, wantOK: false},
		{name: "set nil grant does not apply", op: access.OpSet, wantOK: false},
		{name: "set read/write within day-1", op: access.OpSet, grant: &access.Grant{Ops: []access.Operation{access.OpRead, access.OpWrite}}, wantOK: true, wantOver: false},
		{name: "set delete overreaches", op: access.OpSet, grant: &access.Grant{Ops: []access.Operation{access.OpRead, access.OpDelete}}, wantOK: true, wantOver: true},
		{name: "set set overreaches", op: access.OpSet, grant: &access.Grant{Ops: []access.Operation{access.OpSet}}, wantOK: true, wantOver: true},
		{name: "set empty ops revoke within day-1", op: access.OpSet, grant: &access.Grant{Ops: nil}, wantOK: true, wantOver: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			over, ok := day1OpsOverreach(tc.op, tc.grant)
			if over != tc.wantOver || ok != tc.wantOK {
				t.Fatalf("over,ok = %v,%v want %v,%v", over, ok, tc.wantOver, tc.wantOK)
			}
		})
	}
}

// TestOverreachVerdict: the overreach path returns an access_denied VERDICT
// through the handle, not a Go error — it is a reject, not malformed.
func TestOverreachVerdict(t *testing.T) {
	reg := &fakeRegistry{resolveExists: true, resolveMeta: metaKV(), actorAllows: true}
	d := newDoor(reg, &fakeDriver{}, &fakeMembership{})
	h := boundHandle{door: d, caller: "a"}

	out, err := h.Invoke(t.Context(), access.OpSet, "r1", nil,
		&access.Grant{GranteeKind: access.GranteeActor, Grantee: "b", Ops: []access.Operation{access.OpRead, access.OpDelete}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out.RejectReason != access.AccessDenied {
		t.Fatalf("reason = %q, want access_denied", out.RejectReason)
	}
	if len(reg.setGrants) != 0 {
		t.Fatalf("SetGrant must not run on overreach")
	}
}
