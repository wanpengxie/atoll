package base

import (
	"errors"

	"github.com/wanpengxie/atoll/lib/actorbase"
)

func (l *agentLoop) reply(item *requestItem, value any) {
	if item == nil || item.closed {
		return
	}
	if _, err := l.sys.Reply(item.msg, value); err != nil {
		if isClosed(err) {
			item.closed = true
		}
		return
	}
	item.closed = true
	item.scope.Revoke()
}

func (l *agentLoop) fail(item *requestItem, code, detail string) {
	if item == nil || item.closed {
		return
	}
	if _, err := l.sys.Fail(item.msg, code, detail); err != nil {
		if isClosed(err) {
			item.closed = true
		}
		return
	}
	item.closed = true
	item.scope.Revoke()
}

func isClosed(err error) bool { return errors.Is(err, actorbase.ErrRequestClosed) }
