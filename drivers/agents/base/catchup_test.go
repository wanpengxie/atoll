package base

import (
	"strings"
	"testing"
	"unicode/utf8"
)

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
