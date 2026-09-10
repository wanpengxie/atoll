package agentlooper

import (
	"context"
	"fmt"
	"time"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
)

const maxSessionHistoryMessages = 4096
const sessionContextUnavailable = "session_context_unavailable"

type looperLifeSys struct {
	actorbase.Sys
	life context.Context
}

func (s looperLifeSys) Life() context.Context { return s.life }

func failSessionContext(sys actorbase.Sys, msg actorbase.Msg, err error) {
	_, _ = fail(sys, msg, sessionContextUnavailable,
		"Cannot establish consistent session context within the 4096-message history limit; open a new session. "+err.Error())
}

func validateSessionContext(object agentbase.ContextObject) error {
	if len(object.Messages) > maxSessionHistoryMessages {
		return fmt.Errorf("session context exceeds %d messages", maxSessionHistoryMessages)
	}
	return validateHistory(object.Messages)
}

// All history reads for one command, including ancestor expansion, use one
// bounded View snapshot. Nothing survives the
// command, and these reads never settle or restart historical executions.
type historyBudgetKey struct{}
type historyBudget struct {
	snapshot *actorcaps.LedgerSnapshot
	cache    map[string][]ledgerRow
}

func newHistoryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Value(historyBudgetKey{}) != nil {
		return ctx, func() {}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	return context.WithValue(ctx, historyBudgetKey{}, &historyBudget{cache: map[string][]ledgerRow{}}), cancel
}
