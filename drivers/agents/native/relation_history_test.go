package native

import (
	"context"
	"errors"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"testing"
)

type relationViewSys struct {
	*testSys
	view actorcaps.LedgerView
}

func (s relationViewSys) View() actorcaps.LedgerView { return s.view }

func TestRelationHistoryUsesBoundedViewAndPreservesProjectionOnFailure(t *testing.T) {
	for _, failure := range []error{nil, actorcaps.ErrLedgerLimit, context.DeadlineExceeded, errors.New("storage unavailable")} {
		calls := 0
		sys := relationViewSys{testSys: newTestSys(newTestState()), view: nativeTestView{read: func(ctx context.Context, q actorcaps.LedgerRead) (actorcaps.LedgerSnapshot, error) {
			calls++
			if q.MaxRows != 1000 || q.MaxBytes != 4<<20 || q.Session != "" {
				t.Fatalf("unbounded request: %+v", q)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing deadline")
			}
			return actorcaps.LedgerSnapshot{}, failure
		}}}
		branch := &session{ID: "branch", Holder: "old-holder", Merge: "manual", Execution: "current-turn"}
		c := &controller{sessions: map[string]*session{"branch": branch}}
		err := c.refreshSessionRelations(sys)
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
		if failure != nil && (branch.Holder != "old-holder" || branch.Merge != "manual" || branch.Execution != "current-turn") {
			t.Fatalf("partial projection applied: %+v", branch)
		}
	}
}
