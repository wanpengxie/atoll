package requestctx

import (
	"context"
	"regexp"
	"testing"
)

func TestNewIDReturnsUUIDV4(t *testing.T) {
	t.Parallel()

	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if id := NewID(); !re.MatchString(id) {
		t.Fatalf("NewID()=%q, want RFC 4122 v4 UUID", id)
	}
}

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
