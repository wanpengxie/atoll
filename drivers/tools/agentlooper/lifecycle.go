package agentlooper

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

const maxSessionHistoryMessages = 4096
const sessionContextUnavailable = "session_context_unavailable"

type looperLifeSys struct {
	actorbase.Sys
	life context.Context
}

func (s looperLifeSys) Life() context.Context { return s.life }

func failSessionContext(sys actorbase.Sys, msg actorbase.Msg, err error) {
	_, _ = sys.Fail(msg, sessionContextUnavailable,
		"Cannot establish consistent session context within the 4096-message history limit; open a new session. "+err.Error())
}

func validateSessionContext(object agentbase.ContextObject) error {
	if len(object.Messages) > maxSessionHistoryMessages {
		return fmt.Errorf("session context exceeds %d messages", maxSessionHistoryMessages)
	}
	return validateHistory(object.Messages)
}

// All history reads for one command, including recursive bases and full-text
// continuations, share one budget and one ledger snapshot. Nothing survives the
// command, and these reads never settle or restart historical executions.
type historyBudgetKey struct{}
type historyBudget struct {
	rows, scanned, calls, bytes int
	head                        int64
	headSet                     bool
	cache                       map[string][]ledgerRow
}

func newHistoryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Value(historyBudgetKey{}) != nil {
		return ctx, func() {}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	return context.WithValue(ctx, historyBudgetKey{}, &historyBudget{cache: map[string][]ledgerRow{}}), cancel
}

func historyLimitError() error {
	return fmt.Errorf("session history read budget exceeded (at most %d messages)", maxSessionHistoryMessages)
}

func callHistory(ctx context.Context, sys actorbase.Sys, cause message.Cause, payload any) (json.RawMessage, error) {
	budget := ctx.Value(historyBudgetKey{}).(*historyBudget)
	if budget.calls >= 512 || budget.bytes >= 16<<20 {
		return nil, historyLimitError()
	}
	budget.calls++
	raw, err := call(ctx, sys, cause, actor.SystemActorID, message.TypeSystemLogQuery, payload)
	if err != nil {
		return nil, err
	}
	budget.bytes += len(raw)
	if budget.bytes > 16<<20 {
		return nil, historyLimitError()
	}
	return raw, nil
}
