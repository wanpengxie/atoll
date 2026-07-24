package link

import "errors"

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
	return errors.New(message)
}
