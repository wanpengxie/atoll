package requestctx

import (
	"context"
	"strings"

	"github.com/google/uuid"
)

const Header = "X-Request-ID"

type requestIDKey struct{}

func NewID() string {
	return uuid.NewString()
}

func Normalize(id string) string {
	return strings.TrimSpace(id)
}

func WithRequestID(ctx context.Context, id string) context.Context {
	id = Normalize(id)
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
