package agentlooper

import "github.com/wanpengxie/atoll/lib/actorbase"

// callFailure keeps message delivery correlation distinct from model tool IDs.
type callFailure struct {
	Code      string
	Detail    string
	RequestID string
	cause     error
	Response  actorbase.Msg
}

func (e *callFailure) Error() string { return e.Code + ": " + e.Detail }
func (e *callFailure) Unwrap() error { return e.cause }
