package base

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

const (
	catchupLimit         = 5
	catchupCharBudget    = 64 << 10
	catchupQueryBudget   = 5 * time.Second
	catchupRetryInterval = 100 * time.Millisecond
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

func loadCatchup(ctx context.Context, sys actorbase.Sys) []runtimeproto.ContextItem {
	// Catch-up is best-effort — a failure must never block boot — but it must
	// never fail silently either: without a line here, an agent that quietly
	// stopped seeing the channel's recent history looks identical to one that
	// had nothing to catch up on.
	pending, err := callCatchupWithinBudget(ctx, sys)
	if err != nil {
		slog.Warn("agent catch-up query not sent", "actor", sys.Self(), "error", err)
		return nil
	}
	qctx, cancel := context.WithTimeout(ctx, catchupQueryBudget)
	defer cancel()
	msg, err := pending.Wait(qctx, catchupQueryBudget)
	if err != nil || msg.ID == "" {
		slog.Warn("agent catch-up query unanswered", "actor", sys.Self(), "error", err)
		return nil
	}
	var response logbookResponse
	if err := json.Unmarshal(msg.Payload, &response); err != nil {
		slog.Warn("agent catch-up answer undecodable", "actor", sys.Self(), "error", err)
		return nil
	}
	items := make([]runtimeproto.ContextItem, 0, len(response.Messages))
	for _, row := range response.Messages {
		if row.Message.Kind == string(message.KindEvent) && strings.HasPrefix(row.Message.Type, "activity.") && row.Message.Sender.ID != string(sys.Self()) {
			continue
		}
		rendered := fmt.Sprintf("[%s %s %s] %s", row.Message.Sender.ID, row.Message.Kind, row.Message.Type, strings.TrimSpace(string(row.Message.Payload)))
		items = append(items, runtimeproto.ContextItem{Seq: row.Seq, Sender: row.Message.Sender.ID, Kind: row.Message.Kind, Type: row.Message.Type, Payload: append([]byte(nil), row.Message.Payload...), Text: rendered})
	}
	return budgetContext(items, catchupCharBudget)
}

// callCatchupWithinBudget retries submission while the send itself is failing.
// Catch-up runs at boot, and the boot that most needs it — an agent rejoining
// after time away — happens exactly while its outbound link is coming up. A
// single attempt there gives up milliseconds before the link is usable, so the
// mechanism would be missing precisely when it has something to catch up on.
// The retry stays inside the same budget the query itself gets, so a genuinely
// unavailable link still never delays boot beyond it.
func callCatchupWithinBudget(ctx context.Context, sys actorbase.Sys) (actorbase.Pending, error) {
	deadline := time.Now().Add(catchupQueryBudget)
	for attempt := 0; ; attempt++ {
		pending, err := sys.Call(actor.SystemActorID, platform.TypeLogbookRecent, map[string]any{"limit": catchupLimit})
		if err == nil {
			if attempt > 0 {
				slog.Info("agent catch-up query sent after link came up", "actor", sys.Self(), "attempts", attempt+1)
			}
			return pending, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(catchupRetryInterval):
		}
	}
}

func budgetContext(items []runtimeproto.ContextItem, budget int) []runtimeproto.ContextItem {
	if budget <= 0 {
		return nil
	}
	kept := make([]runtimeproto.ContextItem, 0, len(items))
	total := 0
	oversize := false
	for _, item := range items {
		n := utf8.RuneCountInString(item.Text)
		if n > budget {
			oversize = true
			continue
		}
		kept = append(kept, item)
		total += n
	}
	truncated := false
	for len(kept) > 0 && total > budget {
		total -= utf8.RuneCountInString(kept[0].Text)
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
			total -= utf8.RuneCountInString(kept[0].Text)
			kept = kept[1:]
		}
		if markLen <= budget {
			kept = append([]runtimeproto.ContextItem{{Text: mark}}, kept...)
		}
	}
	return kept
}
