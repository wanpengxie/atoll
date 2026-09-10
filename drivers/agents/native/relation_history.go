package native

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	agentloop "github.com/wanpengxie/atoll/drivers/tools/agentlooper/api"
	"github.com/wanpengxie/atoll/lib/actorbase"
)

const (
	maxRelationHistoryMessages = 1000
	maxRelationHistoryQueries  = 128 // Includes full-text continuations, not just pages.
	maxRelationHistoryBytes    = 4 << 20
	relationHistoryTimeout     = 5 * time.Second
)

var errRelationHistoryLimit = errors.New("relation_history_limit_exceeded")

type relationHistoryReader struct {
	sys     actorbase.Sys
	ctx     context.Context
	queries int
	bytes   int
}

func (r *relationHistoryReader) query(payload any) (logQueryResponse, error) {
	if r.queries >= maxRelationHistoryQueries {
		return logQueryResponse{}, fmt.Errorf("%w: query budget exhausted", errRelationHistoryLimit)
	}
	r.queries++
	page, err := callSystemQueryContext(r.sys, r.ctx, payload)
	if err != nil {
		return logQueryResponse{}, err
	}
	for _, turn := range page.Turns {
		for _, row := range turn.Messages {
			r.bytes += len(row.PayloadText)
		}
	}
	if page.Message != nil {
		r.bytes += len(page.Message.PayloadText)
	}
	if r.bytes > maxRelationHistoryBytes {
		return logQueryResponse{}, fmt.Errorf("%w: text budget exhausted", errRelationHistoryLimit)
	}
	return page, nil
}

func readRelationHistory(sys actorbase.Sys) ([]logMessage, int64, error) {
	ctx, cancel := context.WithTimeout(sys.Life(), relationHistoryTimeout)
	defer cancel()
	r := relationHistoryReader{sys: sys, ctx: ctx}
	before, head := int64(0), int64(0)
	read := 0
	var items []logMessage
	for {
		// One exchange can contain a request and a response. Reserve room for
		// both: the limit is messages, not matched exchanges or relevant facts.
		limit := min(20, (maxRelationHistoryMessages-read)/2)
		if limit == 0 {
			return nil, head, fmt.Errorf("%w: at most %d messages", errRelationHistoryLimit, maxRelationHistoryMessages)
		}
		req := map[string]any{"view": "raw", "limit": limit}
		if before > 0 {
			req["before_seq"] = before
		}
		if head > 0 {
			req["head_seq"] = head
		}
		page, err := r.query(req)
		if err != nil {
			return nil, head, err
		}
		if head == 0 {
			head = page.HeadSeq
		}
		for _, turn := range page.Turns {
			read += len(turn.Messages)
			if read > maxRelationHistoryMessages {
				return nil, head, errRelationHistoryLimit
			}
			for _, row := range turn.Messages {
				if relationMessageType(row.MessageType) {
					items = append(items, row)
				}
			}
		}
		if !page.HasMore {
			break
		}
		if page.NextBeforeSeq <= 0 || (before > 0 && page.NextBeforeSeq >= before) {
			return nil, head, errors.New("project session relations: pagination made no progress")
		}
		before = page.NextBeforeSeq
	}
	// Unrelated model/tool bodies are never expanded. Relevant bodies share the
	// same query, byte and time budgets as the history pages.
	for i := range items {
		if !items[i].Truncated {
			continue
		}
		var text strings.Builder
		offset := 0
		for {
			page, err := r.query(map[string]any{"view": "raw", "read_seq": items[i].Seq, "head_seq": head, "offset": offset})
			if err != nil {
				return nil, head, err
			}
			if page.Message == nil {
				return nil, head, errors.New("project session relations: missing message")
			}
			text.WriteString(page.Message.PayloadText)
			if page.Message.NextOffset == nil {
				break
			}
			if *page.Message.NextOffset <= offset {
				return nil, head, errors.New("project session relations: read made no progress")
			}
			offset = *page.Message.NextOffset
		}
		items[i].PayloadText, items[i].Truncated = text.String(), false
	}
	return items, head, nil
}

func relationMessageType(word string) bool {
	switch word {
	case "session.track", "session.merge", agentloop.TypeStart, agentloop.TypeInput,
		agentloop.TypeReset, agentloop.TypeRename, agentloop.TypeStop,
		agentloop.TypeSessionOpened, agentloop.TypeReport, agentloop.TypeSessionSync, agentloop.TypeSync:
		return true
	}
	return false
}
