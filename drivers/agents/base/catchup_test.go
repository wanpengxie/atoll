package base

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// The boot that most needs catch-up is a rejoin, and that is exactly when the
// outbound link is still coming up. Giving up on the first refusal would make
// the mechanism absent precisely when there is something to catch up on.
func TestCatchUpRetriesWhileTheLinkIsStillComingUp(t *testing.T) {
	sys := newTestSys()
	sys.callErrs = 3
	loadCatchup(context.Background(), sys)
	if sys.calls != 4 {
		t.Fatalf("catch-up gave up on a link that was coming up: attempts=%d", sys.calls)
	}
}

// A link that never comes up must not hold boot: the retry lives inside the
// query budget and then lets go.
func TestCatchUpStopsRetryingWithinItsBudget(t *testing.T) {
	sys := newTestSys()
	sys.callErrs = 1 << 30
	start := time.Now()
	if items := loadCatchup(context.Background(), sys); items != nil {
		t.Fatalf("a dead link produced context: %#v", items)
	}
	if elapsed := time.Since(start); elapsed > catchupQueryBudget*2 {
		t.Fatalf("catch-up held boot for %s", elapsed)
	}
}

func TestCatchUpTakenOnceAndNeverRequeued(t *testing.T) {
	l, e := newUnitLoop()
	l.background = []ContextItem{{Seq: 1, Rendered: "recent"}}
	l.enqueue(bufferedMsg("1", "actor:a", 1), false)
	if e.starts != 1 || len(e.backgrounds[0]) != 1 {
		t.Fatalf("first start background = %#v", e.backgrounds)
	}
	l.state = stateIdle
	l.committing = map[OpID]*operation{}
	l.enqueue(bufferedMsg("2", "actor:a", 1), false)
	if e.starts != 2 || len(e.backgrounds[1]) != 0 {
		t.Fatalf("second start background = %#v", e.backgrounds[1])
	}
}

func TestCatchUpCharacterBudgetDropsOldestWholeRowsAndMarksOversize(t *testing.T) {
	const budget = 64 << 10
	for _, tt := range []struct {
		name  string
		items []ContextItem
		mark  string
	}{
		{
			name: "mixed rows overflow",
			items: []ContextItem{
				{Seq: 1, Rendered: strings.Repeat("a", budget/2)},
				{Seq: 2, Rendered: strings.Repeat("界", budget/3)},
				{Seq: 3, Rendered: strings.Repeat("🙂", budget/3)},
			},
			mark: "截断",
		},
		{
			name: "single oversize",
			items: []ContextItem{
				{Seq: 1, Rendered: strings.Repeat("🙂", budget+1)},
				{Seq: 2, Rendered: "kept"},
			},
			mark: "超长单条",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := budgetContext(tt.items, budget)
			total := 0
			for _, item := range got {
				total += utf8.RuneCountInString(item.Rendered)
			}
			if total > budget || len(got) == 0 || !strings.Contains(got[0].Rendered, tt.mark) {
				t.Fatalf("budgeted context chars=%d rows=%#v", total, got)
			}
			if tt.name == "single oversize" && (len(got) != 2 || got[1].Seq != 2) {
				t.Fatalf("oversize row was sliced or retained: %#v", got)
			}
		})
	}
}
