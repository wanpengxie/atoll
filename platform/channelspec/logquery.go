package channelspec

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

var ErrLogMessageNotFound = errors.New("visible conversation message not found")

// LogQueryRequest is the ordinary system.log.query request body. Statistics,
// search, shared timeline context and exact reads support iterative discovery.
type LogQueryRequest struct {
	View        string `json:"view,omitempty"`
	ReadID      string `json:"read_id,omitempty"`
	RelatedTo   string `json:"related_to,omitempty"`
	Text        string `json:"text,omitempty"`
	Sender      string `json:"sender,omitempty"`
	Participant string `json:"participant,omitempty"`
	SenderKind  string `json:"sender_kind,omitempty"`
	MessageType string `json:"message_type,omitempty"`
	FromTS      int64  `json:"from_ts,omitempty"`
	ToTS        int64  `json:"to_ts,omitempty"`
	BeforeSeq   int64  `json:"before_seq,omitempty"`
	HeadSeq     int64  `json:"head_seq,omitempty"`
	Limit       int    `json:"limit,omitempty"`
	ReadSeq     int64  `json:"read_seq,omitempty"`
	Offset      int    `json:"offset,omitempty"`
	GroupBy     string `json:"group_by,omitempty"`
	AroundSeq   int64  `json:"around_seq,omitempty"`
	AfterSeq    int64  `json:"after_seq,omitempty"`
	Radius      int    `json:"radius,omitempty"`
}

func (q LogQueryRequest) Validate() error {
	if q.View != "" && q.View != "conversation" && q.View != "raw" {
		return fmt.Errorf("view must be conversation (default) or raw")
	}
	if len(q.ReadID) > 256 || len(q.RelatedTo) > 256 {
		return fmt.Errorf("message IDs must be at most 256 bytes")
	}
	if q.BeforeSeq < 0 || q.AfterSeq < 0 || q.AroundSeq < 0 || q.Radius < 0 || q.Radius > 10 || q.HeadSeq < 0 || q.FromTS < 0 || q.ToTS < 0 || q.ReadSeq < 0 || q.Offset < 0 || q.Limit < 0 || q.Limit > 20 {
		return fmt.Errorf("coordinates must be nonnegative; limit is 1..20 (0 defaults to 5); radius is 1..10 (0 defaults to 3)")
	}
	if q.ToTS > 0 && q.FromTS >= q.ToTS {
		return fmt.Errorf("from_ts must be less than to_ts (exclusive)")
	}
	if utf8.RuneCountInString(q.Text) > 256 || len(q.Sender) > 256 || len(q.Participant) > 256 || len(q.MessageType) > 256 {
		return fmt.Errorf("text is at most 256 characters; sender, participant and message_type at most 256 bytes")
	}
	if q.SenderKind != "" {
		if _, ok := actor.ParseKind(q.SenderKind); !ok {
			return fmt.Errorf("sender_kind must be human, agent, tool, peer or system")
		}
	}
	if q.Text != "" && strings.TrimSpace(q.Text) == "" {
		return fmt.Errorf("text cannot be whitespace only")
	}
	hasFilter := strings.TrimSpace(q.Text) != "" || q.Sender != "" || q.Participant != "" || q.SenderKind != "" || q.MessageType != "" || q.FromTS > 0 || q.ToTS > 0
	if q.AroundSeq > 0 {
		if hasFilter || q.ReadID != "" || q.RelatedTo != "" || q.ReadSeq != 0 || q.Offset != 0 || q.GroupBy != "" || q.Limit != 0 {
			return fmt.Errorf("around_seq accepts only radius (default 3, max 10), head_seq and before_seq/after_seq continuation cursors; omit filters")
		}
		if (q.HeadSeq > 0 && (q.AroundSeq > q.HeadSeq || q.AfterSeq > q.HeadSeq)) || q.BeforeSeq > q.AroundSeq || (q.AfterSeq > 0 && q.AfterSeq < q.AroundSeq) {
			return fmt.Errorf("context cursors must lie on their respective sides of around_seq, within the snapshot")
		}
		return nil
	}
	if q.AfterSeq != 0 || q.Radius != 0 {
		return fmt.Errorf("after_seq and radius require around_seq")
	}
	if q.ReadSeq > 0 || q.ReadID != "" || q.RelatedTo != "" {
		modes := 0
		if q.ReadSeq > 0 {
			modes++
		}
		if q.ReadID != "" {
			modes++
		}
		if q.RelatedTo != "" {
			modes++
		}
		if modes != 1 || hasFilter || q.GroupBy != "" || q.BeforeSeq != 0 || q.Limit != 0 || (q.RelatedTo != "" && q.Offset != 0) {
			return fmt.Errorf("choose one of read_seq/read_id/related_to; accepts view/head_seq, and offset for reads only")
		}
		return nil
	}
	if q.Offset != 0 {
		return fmt.Errorf("offset requires read_seq")
	}
	if q.GroupBy != "" {
		switch q.GroupBy {
		case "sender", "sender_kind", "message_type", "day":
		default:
			return fmt.Errorf("group_by must be sender, sender_kind, message_type or day (UTC)")
		}
		if q.Limit != 0 {
			return fmt.Errorf("statistics use the scan budget, not limit; omit limit")
		}
		return nil
	}
	if !hasFilter && q.BeforeSeq == 0 {
		return fmt.Errorf("provide a search filter or before_seq; use system.log.recent for the latest conversation")
	}
	return nil
}

type LogQueryResponse struct {
	View          string            `json:"view"`
	Guidance      []string          `json:"guidance"`
	Next          []LogQueryAction  `json:"next,omitempty"`
	Related       []LogQueryRelated `json:"related,omitempty"`
	Turns         []LogQueryTurn    `json:"turns"`
	HeadSeq       int64             `json:"head_seq"`
	NextBeforeSeq int64             `json:"next_before_seq"`
	HasMore       bool              `json:"has_more"`
	ScanLimited   bool              `json:"scan_limited"`
	Scanned       int               `json:"scanned"`
	Message       *LogQueryMessage  `json:"message,omitempty"`
	Stats         *LogQueryStats    `json:"stats,omitempty"`
	Context       *LogQueryContext  `json:"context,omitempty"`
}

// Counts apply only to this scanned page. Sum pages with identical filters and
// HeadSeq until HasMore=false to obtain exhaustive snapshot totals.
type LogQueryStats struct {
	Questions        int              `json:"questions"`
	Answers          int              `json:"answers"`
	States           map[string]int   `json:"states"`
	Scope            string           `json:"scope"`
	GroupBy          string           `json:"group_by"`
	MatchedMessages  int              `json:"matched_messages"`
	MatchedExchanges int              `json:"matched_exchanges"`
	Buckets          []LogQueryBucket `json:"buckets"`
}

type LogQueryBucket struct {
	Key       string `json:"key"`
	Messages  int    `json:"messages"`
	FirstSeq  int64  `json:"first_seq"`
	LastSeq   int64  `json:"last_seq"`
	FromTS    int64  `json:"from_ts"`
	ThroughTS int64  `json:"through_ts"`
}

// Chronological proximity is NOT a causal thread. ParentID remains authoritative.
type LogQueryContext struct {
	Anchor            LogQueryMessage   `json:"anchor"`
	Before            []LogQueryMessage `json:"before"`
	After             []LogQueryMessage `json:"after"`
	NextBeforeSeq     int64             `json:"next_before_seq"`
	NextAfterSeq      int64             `json:"next_after_seq"`
	HasOlder          bool              `json:"has_older"`
	HasNewer          bool              `json:"has_newer"`
	BeforeScanLimited bool              `json:"before_scan_limited"`
	AfterScanLimited  bool              `json:"after_scan_limited"`
}

type LogQueryTurn struct {
	State       string            `json:"state"`
	Seq         int64             `json:"seq"`
	MatchedSeqs []int64           `json:"matched_seqs"`
	Messages    []LogQueryMessage `json:"messages"`
}

// PayloadText is normalized JSON, or an explicitly marked character slice of
// it. Keeping it separate from an envelope prevents an excerpt being mistaken
// for the original complete payload. read_seq + offset recovers every chunk.
type LogQueryMessage struct {
	ContentSource string             `json:"content_source"`
	State         string             `json:"state,omitempty"`
	Relations     []LogQueryRelation `json:"relations,omitempty"`
	NextRead      *LogQueryRequest   `json:"next_read,omitempty"`
	Seq           int64              `json:"seq"`
	ID            message.ID         `json:"id"`
	Sender        message.Sender     `json:"sender"`
	Audience      message.Audience   `json:"audience"`
	Kind          message.Kind       `json:"kind"`
	MessageType   string             `json:"message_type"`
	ParentID      message.ID         `json:"parent_id,omitempty"`
	TSReceived    int64              `json:"ts_received"`
	Terminal      bool               `json:"terminal"`
	PayloadText   string             `json:"payload_text"`
	TotalChars    int                `json:"total_chars"`
	Offset        int                `json:"offset"`
	NextOffset    *int               `json:"next_offset,omitempty"`
	Truncated     bool               `json:"truncated"`
}

type LogQueryAction struct {
	Reason  string          `json:"reason"`
	Request LogQueryRequest `json:"request"`
}
type LogQueryRelation struct {
	Kind      string `json:"kind"`
	MessageID string `json:"message_id"`
}
type LogQueryRelated struct {
	Relation    LogQueryRelation `json:"relation"`
	Message     *LogQueryMessage `json:"message,omitempty"`
	Unavailable bool             `json:"unavailable,omitempty"`
}
