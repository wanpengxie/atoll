package harness

import "context"

type clientFingerprintContextKey struct{}

// WithClientFingerprint attaches shell-ingress persistence metadata to one
// write. The value flows through the harness into MessageLog.Append but never
// becomes part of protocol/message.Envelope.
func WithClientFingerprint(ctx context.Context, fingerprint string) context.Context {
	if fingerprint == "" {
		return ctx
	}
	return context.WithValue(ctx, clientFingerprintContextKey{}, fingerprint)
}

func clientFingerprintFromContext(ctx context.Context) string {
	value, _ := ctx.Value(clientFingerprintContextKey{}).(string)
	return value
}
