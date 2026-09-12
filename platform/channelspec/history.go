package channelspec

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
)

// HistoryWindowQuery asks for a bounded historical projection ending strictly
// before BeforeSeq. TargetRows is a soft raw-ledger target: the view may scan
// farther backwards until it reaches a root-turn boundary and contains at
// least MinimumCompleteRoots completed root turns.
type HistoryWindowQuery struct {
	BeforeSeq            int64
	TargetRows           int
	MinimumCompleteRoots int
}

// VisibleMessageRow is retained as a source-compatible name for callers that
// construct history fixtures. It is the actor-facing ledger row, not a storage
// DTO.
type VisibleMessageRow = actorcaps.LedgerRow

// HistoryWindow is a semantically closed timeline page. Rows are ascending and
// projected: every request is retained, completed requests retain their
// terminal response, open requests retain only their latest provisional
// response, and standalone visible messages remain present.
type HistoryWindow struct {
	Rows      []actorcaps.LedgerRow
	HeadSeq   int64
	OldestSeq int64
	NewestSeq int64
	HasOlder  bool
}

const historyScanBatch = actorcaps.MaxVisiblePageRows

// ReadHistoryWindow reads backwards until a root-turn-safe boundary is
// available, then projects away historical housekeeping and intermediate
// progress. Live feeds remain the full visible ledger.
func ReadHistoryWindow(ctx context.Context, view actorcaps.LedgerView, query HistoryWindowQuery) (HistoryWindow, error) {
	target := query.TargetRows
	if target <= 0 {
		target = 200
	}
	minimumRoots := query.MinimumCompleteRoots
	if minimumRoots <= 0 {
		minimumRoots = 20
	}
	before := query.BeforeSeq
	var accumulated []actorcaps.LedgerRow
	var head int64
	for {
		page, snapshotHead, hasOlder, err := view.ReadVisibleBeforeSeq(ctx, before, historyScanBatch)
		if err != nil {
			return HistoryWindow{}, err
		}
		if head == 0 {
			head = snapshotHead
		}
		if len(page) > 0 {
			accumulated = append(page, accumulated...)
			before = page[0].Seq
		}
		boundary, ready := historyBoundary(accumulated, target, minimumRoots, !hasOlder)
		if ready {
			return projectHistoryWindow(accumulated, boundary, head, hasOlder || boundary > 0), nil
		}
		if !hasOlder || len(page) == 0 {
			return projectHistoryWindow(accumulated, 0, head, false), nil
		}
	}
}

func historyBoundary(rows []actorcaps.LedgerRow, target, minimumRoots int, atBeginning bool) (int, bool) {
	if len(rows) == 0 {
		return 0, atBeginning
	}
	if len(rows) < target && !atBeginning {
		return 0, false
	}
	rootIndexes := make([]int, 0)
	completeRootIndexes := make([]int, 0)
	terminalParents := make(map[message.ID]bool)
	for _, row := range rows {
		if row.IsTerminal {
			terminalParents[row.Envelope.ParentID] = true
		}
	}
	for index, row := range rows {
		envelope := row.Envelope
		if envelope.Kind != message.KindRequest || envelope.ParentID != "" {
			continue
		}
		if HousekeepingWord(envelope.Type) {
			continue
		}
		rootIndexes = append(rootIndexes, index)
		if terminalParents[envelope.ID] {
			completeRootIndexes = append(completeRootIndexes, index)
		}
	}
	if len(rootIndexes) == 0 {
		hasTurnRows := false
		for _, row := range rows {
			if row.Envelope.Kind == message.KindRequest || row.Envelope.Kind == message.KindResponse {
				hasTurnRows = true
				break
			}
		}
		if !hasTurnRows && len(rows) >= target {
			return len(rows) - target, true
		}
		if !atBeginning {
			return 0, false
		}
		return 0, true
	}
	cutoff := len(rows) - target
	if cutoff < 0 {
		cutoff = 0
	}
	boundary := -1
	for _, index := range rootIndexes {
		if index > cutoff {
			break
		}
		boundary = index
	}
	if boundary < 0 {
		if !atBeginning {
			return 0, false
		}
		boundary = 0
	}
	if len(completeRootIndexes) < minimumRoots {
		if !atBeginning {
			return 0, false
		}
		return 0, true
	}
	minimumBoundary := completeRootIndexes[len(completeRootIndexes)-minimumRoots]
	if minimumBoundary < boundary {
		boundary = minimumBoundary
	}
	return boundary, true
}

func projectHistoryWindow(raw []actorcaps.LedgerRow, boundary int, head int64, hasOlder bool) HistoryWindow {
	if boundary < 0 || boundary > len(raw) {
		boundary = 0
	}
	rows := raw[boundary:]
	housekeeping := make(map[message.ID]bool)
	for _, row := range rows {
		if row.Envelope.Kind == message.KindRequest && HousekeepingWord(row.Envelope.Type) {
			housekeeping[row.Envelope.ID] = true
		}
	}
	terminalParents := make(map[message.ID]bool)
	latestProvisional := make(map[message.ID]int64)
	for _, row := range rows {
		if row.Envelope.Kind != message.KindResponse || row.Envelope.ParentID == "" {
			continue
		}
		if row.IsTerminal {
			terminalParents[row.Envelope.ParentID] = true
		} else if row.Seq > latestProvisional[row.Envelope.ParentID] {
			latestProvisional[row.Envelope.ParentID] = row.Seq
		}
	}
	projected := make([]actorcaps.LedgerRow, 0, len(rows))
	for _, row := range rows {
		if housekeeping[row.Envelope.ID] || housekeeping[row.Envelope.ParentID] {
			continue
		}
		include := row.Envelope.Kind != message.KindResponse || row.IsTerminal
		if row.Envelope.Kind == message.KindResponse && !row.IsTerminal && !terminalParents[row.Envelope.ParentID] {
			include = latestProvisional[row.Envelope.ParentID] == row.Seq
		}
		if include {
			projected = append(projected, row)
		}
	}
	window := HistoryWindow{Rows: projected, HeadSeq: head, HasOlder: hasOlder}
	if len(rows) > 0 {
		window.OldestSeq = rows[0].Seq
	}
	if len(projected) > 0 {
		window.NewestSeq = projected[len(projected)-1].Seq
	}
	return window
}
