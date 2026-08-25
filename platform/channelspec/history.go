package channelspec

import "github.com/wanpengxie/atoll/protocol/message"

// HistoryWindowQuery asks for a bounded historical projection ending strictly
// before BeforeSeq. TargetRows is a soft raw-ledger target: the view may scan
// farther backwards until it reaches a root-turn boundary and contains at
// least MinimumCompleteRoots completed root turns.
type HistoryWindowQuery struct {
	BeforeSeq            int64
	TargetRows           int
	MinimumCompleteRoots int
}

// VisibleMessageRow is the platform-owned read DTO crossing the channel
// membrane. Storage-only columns other than the read cursor do not escape.
type VisibleMessageRow struct {
	Seq      int64
	Envelope message.Envelope
}

// HistoryWindow is a semantically closed timeline page. Rows are ascending and
// projected: every request is retained, completed requests retain their
// terminal response, open requests retain only their latest provisional
// response, and standalone visible messages remain present.
type HistoryWindow struct {
	Rows      []VisibleMessageRow
	HeadSeq   int64
	OldestSeq int64
	NewestSeq int64
	HasOlder  bool
}
