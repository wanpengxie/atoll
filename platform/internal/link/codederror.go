package link

type codedAckError struct {
	code    string
	message string
}

func (e codedAckError) Error() string     { return e.message }
func (e codedAckError) ErrorCode() string { return e.code }

// decodeAckError reconstructs the definite error text returned by the peer.
// Domain-specific sentinel mapping deliberately lives at the owning boundary;
// the transport does not invent lifecycle or liveness policy.
func decodeAckError(code, message string) error {
	if code == "" && message == "" {
		return nil
	}
	if message == "" {
		message = code
	}
	return codedAckError{code: code, message: message}
}
