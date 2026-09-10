package native

import (
	"context"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"time"
)

const maxRelationHistoryMessages = 1000
const maxRelationHistoryBytes = 4 << 20
const relationHistoryTimeout = 5 * time.Second

var errRelationHistoryLimit = actorcaps.ErrLedgerLimit

func readRelationHistory(sys actorbase.Sys) ([]logMessage, int64, error) {
	ctx, cancel := context.WithTimeout(sys.Life(), relationHistoryTimeout)
	defer cancel()
	snapshot, err := sys.View().Read(ctx, actorcaps.LedgerRead{MaxRows: maxRelationHistoryMessages, MaxBytes: maxRelationHistoryBytes})
	if err != nil {
		return nil, 0, err
	}
	return nativeLedgerRows(snapshot.Rows), snapshot.HeadSeq, nil
}

func nativeLedgerRows(rows []actorcaps.LedgerRow) []logMessage {
	out := make([]logMessage, 0, len(rows))
	for _, row := range rows {
		e := row.Envelope
		out = append(out, logMessage{Seq: row.Seq, ID: e.ID, Sender: e.Sender, Audience: e.Audience, Kind: e.Kind, MessageType: e.Type, ParentID: e.ParentID, Terminal: row.IsTerminal, TSReceived: e.TSReceived, PayloadText: string(e.Payload)})
	}
	return out
}
