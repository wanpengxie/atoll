package base

import (
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

type contextReadSys struct {
	actorbase.Sys
	resource contextReadResource
}

func (s contextReadSys) Resource() actorbase.ResourceHandle { return s.resource }

type contextReadResource struct {
	actorbase.ResourceHandle
	outcome accessdoor.Outcome
	err     error
}

func (r contextReadResource) Read(resource.ResourceID) (accessdoor.Outcome, error) {
	return r.outcome, r.err
}

func TestLoadContextDistinguishesMissingFromFailure(t *testing.T) {
	for _, tc := range []struct {
		name           string
		outcome        accessdoor.Outcome
		err            error
		wantErr, found bool
	}{
		{name: "empty"},
		{name: "absent", outcome: accessdoor.Outcome{RejectReason: access.ResourceNotFound}},
		{name: "denied", outcome: accessdoor.Outcome{RejectReason: access.AccessDenied}, wantErr: true},
		{name: "driver", outcome: accessdoor.Outcome{RejectReason: access.DriverError}, wantErr: true},
		{name: "transport", err: errors.New("read failed"), wantErr: true},
		{name: "invalid", outcome: accessdoor.Outcome{Found: true, Value: []byte(`{`)}, wantErr: true},
		{name: "valid", outcome: accessdoor.Outcome{Found: true, Value: []byte(`{"messages":[]}`)}, found: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, found, err := LoadContext(contextReadSys{resource: contextReadResource{outcome: tc.outcome, err: tc.err}}, "main")
			if found != tc.found || (err != nil) != tc.wantErr {
				t.Fatalf("found=%v err=%v", found, err)
			}
		})
	}
}
