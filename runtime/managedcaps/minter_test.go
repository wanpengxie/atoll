package managedcaps

import (
	"testing"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

type testPenMinter struct{ harness.Minter }
type testAccessMinter struct{ accessdoor.AccessMinter }
type testStateResolver struct{ accessdoor.StateHandleResolver }
type testScheduleMinter struct{ schedule.Minter }
type testLifecycle struct{ LifecycleOperations }

func TestNewRequiresLedgerViewMinter(t *testing.T) {
	_, err := New(
		testPenMinter{},
		testAccessMinter{},
		testStateResolver{},
		testScheduleMinter{},
		testLifecycle{},
		nil,
	)
	if err != ErrInvalidInput {
		t.Fatalf("New error = %v, want %v", err, ErrInvalidInput)
	}
}
