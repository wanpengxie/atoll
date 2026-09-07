package storespec

import "context"

// VisibleExchangeQuery is a read-only, channel-bound message lookup. It uses
// only envelope structure; search text and conversation policy belong to the
// platform. The explicit port avoids widening the live-feed reader.
type VisibleExchangeQuery interface {
	ReadVisibleExchanges(context.Context, int64, int64, int) (ExchangePage, error)
	FindVisibleSeq(context.Context, int64) (StoredRow, bool, error)
	FindVisibleID(context.Context, string) (StoredRow, bool, error)
	ReadVisibleReply(context.Context, string, int64) (StoredRow, bool, error)
	ReadVisibleWindow(context.Context, int64, int64, bool, int) (MessageWindow, error)
}

// MessageWindow is exclusive of the cursor, ordered nearest-first in the
// requested direction. Responses use the same snapshot projection as exchanges.
type MessageWindow struct {
	Rows    []StoredRow
	HeadSeq int64
	HasMore bool
}

// ExchangePage selects non-response messages newest first by their own seq.
// Each request includes its terminal response, or latest provisional when
// still open, as of HeadSeq. A nested request is its own exchange. Standalone
// events have just one row. Rows inside an exchange are chronological.
type ExchangePage struct {
	Exchanges [][]StoredRow
	HeadSeq   int64
	HasOlder  bool
}
