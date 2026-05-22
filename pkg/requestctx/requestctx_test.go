package requestctx

import (
	"context"
	"testing"
)

func TestWithRequestIDRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := WithRequestID(context.Background(), " req-1 ")
	if got := RequestID(ctx); got != "req-1" {
		t.Fatalf("RequestID=%q want req-1", got)
	}
}

func TestWithRequestIDIgnoresEmpty(t *testing.T) {
	t.Parallel()

	ctx := WithRequestID(context.Background(), " ")
	if got := RequestID(ctx); got != "" {
		t.Fatalf("RequestID=%q want empty", got)
	}
}
