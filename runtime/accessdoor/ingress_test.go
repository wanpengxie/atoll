package accessdoor

import (
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// TestIngressRules exercises every ingress structural rule; each malformed shape
// must surface as ErrMalformed (a protocol error, never a verdict).
func TestIngressRules(t *testing.T) {
	tests := []struct {
		name    string
		op      access.Operation
		id      resource.ResourceID
		args    []byte
		wantErr bool
	}{
		{name: "op not in closed set", op: access.Operation("frobnicate"), wantErr: true},
		{name: "retired set verb is out of set", op: access.Operation("set"), wantErr: true},

		// resource id: an invocation without an object is structurally not an access.
		{name: "empty resource id", op: access.OpCreate, id: "", wantErr: true},

		// op×Args: delete carries no Args.
		{name: "delete with args", op: access.OpDelete, args: []byte("x"), wantErr: true},
		{name: "delete no args ok", op: access.OpDelete},
		{name: "create empty args ok", op: access.OpCreate, args: []byte{}},
		{name: "read selector args ignored not rejected", op: access.OpRead, args: []byte("selector")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := tc.id
			if id == "" && tc.name != "empty resource id" {
				id = "kv:x" // default well-formed object for cases probing other rules
			}
			err := ingress(tc.op, id, tc.args)
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
