package pillm

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/wanpengxie/atoll/drivers/tools/pibridge"
)

type providerFailure struct {
	Code         string
	Status       int
	ProviderCode string
	RetryAfter   time.Duration
	Attempts     int
}

func (e *providerFailure) Error() string { return "provider request failed: " + e.Code }

func classify(err error) *providerFailure {
	if errors.Is(err, context.Canceled) {
		return &providerFailure{Code: "cancelled"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &providerFailure{Code: "deadline_exceeded"}
	}
	var bridge *pibridge.Error
	if errors.As(err, &bridge) {
		code := bridge.Code
		switch code {
		case "invalid_args", "context_invalid", "auth", "permission", "model_not_found", "rate_limited", "transient_provider", "transport_error", "cancelled", "deadline_exceeded":
		default:
			code = "unknown_provider_error"
		}
		return &providerFailure{Code: code, Status: bridge.Status, ProviderCode: bridge.ProviderCode, RetryAfter: time.Duration(bridge.RetryAfterMS) * time.Millisecond}
	}
	return &providerFailure{Code: "unknown_provider_error"}
}

func retryable(e *providerFailure) bool {
	return e.Code == "rate_limited" || e.Code == "transient_provider" || e.Code == "transport_error"
}

// retryGenerate owns one total deadline. Each attempt invokes the bridge anew;
// no failed or partial assistant is exposed as a successful result.
func retryGenerate(ctx context.Context, cfg Config, invoke func(context.Context) (json.RawMessage, error), progress func(int, time.Duration)) (json.RawMessage, int, error) {
	budget := time.Duration(cfg.RequestTimeoutMS) * time.Millisecond
	if budget <= 0 {
		budget = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			failure := classify(err)
			failure.Attempts = attempt
			return nil, attempt, failure
		}
		progress(attempt+1, 0)
		if err := ctx.Err(); err != nil {
			failure := classify(err)
			failure.Attempts = attempt
			return nil, attempt, failure
		}
		attempt++
		raw, err := invoke(ctx)
		if err == nil {
			var result struct {
				Message struct {
					Role    string            `json:"role"`
					Content []json.RawMessage `json:"content"`
					Stop    string            `json:"stopReason"`
				} `json:"message"`
			}
			if json.Unmarshal(raw, &result) != nil || result.Message.Role != "assistant" || result.Message.Content == nil {
				err = &pibridge.Error{Code: "unknown_provider_error"}
			} else if result.Message.Stop == "error" || result.Message.Stop == "aborted" {
				code := "unknown_provider_error"
				if result.Message.Stop == "aborted" {
					code = "cancelled"
				}
				err = &pibridge.Error{Code: code}
			}
		}
		if err == nil {
			return raw, attempt, nil
		}
		if ctx.Err() != nil {
			failure := classify(ctx.Err())
			failure.Attempts = attempt
			return nil, attempt, failure
		}
		failure := classify(err)
		failure.Attempts = attempt
		if !retryable(failure) || attempt > cfg.MaxRetries {
			return nil, attempt, failure
		}
		delay := time.Duration(cfg.BaseDelayMS) * time.Millisecond
		if delay <= 0 {
			delay = 500 * time.Millisecond
		}
		for i := 1; i < attempt; i++ {
			delay *= 2
		}
		maxDelay := time.Duration(cfg.MaxDelayMS) * time.Millisecond
		if maxDelay <= 0 {
			maxDelay = 5 * time.Second
		}
		if delay > maxDelay {
			delay = maxDelay
		}
		delay = time.Duration(float64(delay) * (0.75 + rand.Float64()*0.25))
		if failure.RetryAfter > 0 {
			delay = failure.RetryAfter
		}
		deadline, _ := ctx.Deadline()
		if time.Until(deadline) <= delay {
			failure.Code = "deadline_exceeded"
			return nil, attempt, failure
		}
		progress(attempt, delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			failure = classify(ctx.Err())
			failure.Attempts = attempt
			return nil, attempt, failure
		case <-timer.C:
		}
	}
}
