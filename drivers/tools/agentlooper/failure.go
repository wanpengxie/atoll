package agentlooper

// callFailure keeps message delivery correlation distinct from model tool IDs.
type callFailure struct {
	Code      string
	Detail    string
	RequestID string
	cause     error
}

func (e *callFailure) Error() string { return e.Code + ": " + e.Detail }
func (e *callFailure) Unwrap() error { return e.cause }
