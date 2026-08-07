package base

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

const (
	catchupLimit       = 5
	catchupCharBudget  = 64 << 10
	catchupQueryBudget = 5 * time.Second
)

type logbookResponse struct {
	Messages []struct {
		Seq     int64 `json:"seq"`
		Message struct {
			Sender struct {
				ID string `json:"id"`
			} `json:"sender"`
			Kind    string          `json:"kind"`
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
		} `json:"message"`
	} `json:"messages"`
}

func loadCatchup(ctx context.Context, sys actorbase.Sys) []ContextItem {
	pending, err := sys.Call(actor.SystemActorID, registry.TypeLogbookRecent, map[string]any{"limit": catchupLimit})
	if err != nil {
		return nil
	}
	qctx, cancel := context.WithTimeout(ctx, catchupQueryBudget)
	defer cancel()
	msg, err := pending.Wait(qctx, catchupQueryBudget)
	if err != nil || msg.ID == "" {
		return nil
	}
	var response logbookResponse
	if json.Unmarshal(msg.Payload, &response) != nil {
		return nil
	}
	items := make([]ContextItem, 0, len(response.Messages))
	for _, row := range response.Messages {
		rendered := fmt.Sprintf("[%s %s %s] %s", row.Message.Sender.ID, row.Message.Kind, row.Message.Type, strings.TrimSpace(string(row.Message.Payload)))
		items = append(items, ContextItem{Seq: row.Seq, Sender: row.Message.Sender.ID, Kind: row.Message.Kind, Type: row.Message.Type, Payload: append([]byte(nil), row.Message.Payload...), Rendered: rendered})
	}
	return budgetContext(items, catchupCharBudget)
}

func budgetContext(items []ContextItem, budget int) []ContextItem {
	if budget <= 0 {
		return nil
	}
	kept := make([]ContextItem, 0, len(items))
	total := 0
	oversize := false
	for _, item := range items {
		n := utf8.RuneCountInString(item.Rendered)
		if n > budget {
			oversize = true
			continue
		}
		kept = append(kept, item)
		total += n
	}
	truncated := false
	for len(kept) > 0 && total > budget {
		total -= utf8.RuneCountInString(kept[0].Rendered)
		kept = kept[1:]
		truncated = true
	}
	if oversize || truncated {
		mark := "[频道最近记录已从最旧处整条截断]"
		if oversize {
			mark = "[频道最近记录含超长单条，已整条略去]"
		}
		markLen := utf8.RuneCountInString(mark)
		for len(kept) > 0 && markLen+total > budget {
			total -= utf8.RuneCountInString(kept[0].Rendered)
			kept = kept[1:]
		}
		if markLen <= budget {
			kept = append([]ContextItem{{Rendered: mark}}, kept...)
		}
	}
	return kept
}

func (l *agentLoop) takeBackground() []ContextItem {
	background := l.background
	l.background = nil
	return background
}
