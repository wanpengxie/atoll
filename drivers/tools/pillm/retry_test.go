package pillm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/tools/pibridge"
)

func retryTestConfig() Config {
	return Config{MaxRetries: 2, BaseDelayMS: 1, MaxDelayMS: 4, RequestTimeoutMS: 1000}
}
func noProgress(int, time.Duration) {}

func TestRetryClassificationAndAttemptCount(t *testing.T) {
	for _, test := range []struct {
		code string
		want int
	}{{"rate_limited", 3}, {"transient_provider", 3}, {"transport_error", 3}, {"invalid_args", 1}, {"context_invalid", 1}, {"auth", 1}, {"permission", 1}, {"model_not_found", 1}, {"unknown_provider_error", 1}, {"provider_error", 1}} {
		t.Run(test.code, func(t *testing.T) {
			calls := 0
			raw, n, err := retryGenerate(context.Background(), retryTestConfig(), func(context.Context) (json.RawMessage, error) {
				calls++
				if calls < 3 {
					return nil, &pibridge.Error{Code: test.code, Detail: "secret key must not escape"}
				}
				return json.RawMessage(`{"message":{"role":"assistant","content":[],"stopReason":"stop"}}`), nil
			}, noProgress)
			if calls != test.want || n != test.want {
				t.Fatalf("calls=%d attempts=%d", calls, n)
			}
			if test.want == 3 && (err != nil || raw == nil) {
				t.Fatal(err)
			}
			if test.want == 1 && (err == nil || raw != nil) {
				t.Fatal("permanent error returned success")
			}
		})
	}
}
func TestRetryCancellationAndRetryAfterBudget(t *testing.T) {
	for _, mode := range []string{"cancel", "budget"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			_, n, err := retryGenerate(ctx, retryTestConfig(), func(context.Context) (json.RawMessage, error) {
				calls++
				if mode == "cancel" {
					cancel()
				}
				return nil, &pibridge.Error{Code: "rate_limited", RetryAfterMS: 5000}
			}, noProgress)
			var failure *providerFailure
			if !errors.As(err, &failure) || calls != 1 || n != 1 {
				t.Fatalf("calls=%d attempts=%d error=%v", calls, n, err)
			}
			if failure.Code != "deadline_exceeded" && failure.Code != "cancelled" {
				t.Fatal(failure)
			}
		})
	}
}
func TestRetryBackoffCanBeCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	_, _, err := retryGenerate(ctx, retryTestConfig(), func(context.Context) (json.RawMessage, error) {
		calls++
		return nil, &pibridge.Error{Code: "rate_limited", RetryAfterMS: 500}
	}, func(_ int, delay time.Duration) {
		if delay > 0 {
			cancel()
		}
	})
	var failure *providerFailure
	if !errors.As(err, &failure) || failure.Code != "cancelled" || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}
func TestRetryFinalErrorMessageIsNotSuccessfulAssistant(t *testing.T) {
	for _, reason := range []string{"error", "aborted"} {
		raw, n, err := retryGenerate(context.Background(), retryTestConfig(), func(context.Context) (json.RawMessage, error) {
			return json.RawMessage(`{"message":{"role":"assistant","stopReason":"` + reason + `","content":[{"type":"text","text":"partial"}]}}`), nil
		}, noProgress)
		if raw == nil || n != 1 || err != nil {
			t.Fatalf("reason=%s attempts=%d result=%s error=%v", reason, n, raw, err)
		}
	}
}
func TestRetryDoesNotRefreshParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, n, err := retryGenerate(ctx, retryTestConfig(), func(ctx context.Context) (json.RawMessage, error) { <-ctx.Done(); return nil, ctx.Err() }, noProgress)
	var failure *providerFailure
	if !errors.As(err, &failure) || failure.Code != "deadline_exceeded" || n != 1 {
		t.Fatalf("attempts=%d error=%v", n, err)
	}
}

func TestCancelledBeforeBridgeAdmissionHasZeroAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	_, n, err := retryGenerate(ctx, retryTestConfig(), func(context.Context) (json.RawMessage, error) { calls++; return nil, nil }, func(int, time.Duration) { cancel() })
	if n != 0 || calls != 0 || err == nil {
		t.Fatalf("attempts=%d calls=%d error=%v", n, calls, err)
	}
}
