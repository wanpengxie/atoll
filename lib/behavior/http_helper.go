package behavior

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// HTTPDoer is the minimal interface HTTPClient depends on. *http.Client
// satisfies it directly; tests inject a recording stub.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// HTTPClientConfig parameterises HTTPClient.
//
// Zero values get safe defaults:
//   - Timeout         → 10s
//   - MaxRetries      → 2 (i.e. up to 3 attempts total)
//   - InitialBackoff  → 200ms
//   - MaxBackoff      → 2s
//   - BreakerThreshold → 5 consecutive failures
//   - BreakerCooldown → 10s
//
// Doer defaults to a fresh *http.Client with Timeout applied.
type HTTPClientConfig struct {
	// BaseURL is the URL prefix prepended to every relative path passed
	// to Do / Get / Post / etc. Trailing slash optional.
	BaseURL string

	// Timeout is the per-request wall-clock deadline (covers the full
	// request including body read).
	Timeout time.Duration

	// MaxRetries is the maximum number of retry attempts on retryable
	// errors (network failures + 5xx). Total attempts = MaxRetries + 1.
	// A pointer so "unset" (nil → default 2) is distinct from an explicit 0
	// (retries off, exactly 1 attempt) — a plain int cannot express "off".
	MaxRetries *int

	// InitialBackoff / MaxBackoff bound the exponential backoff between
	// retries. The actual delay doubles each retry up to MaxBackoff.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	// BreakerThreshold is the number of consecutive failures that trips
	// the circuit breaker into the open state.
	BreakerThreshold int

	// BreakerCooldown is the wall-clock window the breaker stays open
	// before allowing one probe through (half-open).
	BreakerCooldown time.Duration

	// Doer overrides the underlying transport. nil → fresh *http.Client.
	Doer HTTPDoer

	// Clock supplies the current time. Tests inject a fake clock; nil
	// falls back to time.Now.
	Clock func() time.Time

	// Logger receives non-fatal diagnostics. nil → discard.
	Logger *slog.Logger

	// Metrics receives counters / histograms. nil → NoopMetrics.
	Metrics Metrics

	// MetricName is the prefix used for all HTTP metrics; empty → "http".
	MetricName string
}

// HTTPClient is the F8 outbound HTTP helper. It wraps an HTTPDoer with:
//
//   - automatic retry on transient failures (network + 5xx)
//   - exponential backoff with cap
//   - simple consecutive-failure circuit breaker
//   - metrics + structured logging
//   - per-request response.Body buffering helper (Do returns body bytes)
//
// Safe for concurrent use; the breaker state is protected by an
// internal mutex.
type HTTPClient struct {
	cfg HTTPClientConfig

	mu                sync.Mutex
	consecutiveFails  int
	breakerOpenedAt   time.Time
	breakerHalfProbed bool
}

// NewHTTPClient builds a configured HTTPClient.
func NewHTTPClient(cfg HTTPClientConfig) *HTTPClient {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxRetries == nil {
		def := 2
		cfg.MaxRetries = &def
	} else if *cfg.MaxRetries < 0 {
		zero := 0
		cfg.MaxRetries = &zero
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 200 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 2 * time.Second
	}
	if cfg.BreakerThreshold <= 0 {
		cfg.BreakerThreshold = 5
	}
	if cfg.BreakerCooldown <= 0 {
		cfg.BreakerCooldown = 10 * time.Second
	}
	if cfg.Doer == nil {
		cfg.Doer = &http.Client{Timeout: cfg.Timeout}
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NoopMetrics{}
	}
	if cfg.MetricName == "" {
		cfg.MetricName = "http"
	}
	return &HTTPClient{cfg: cfg}
}

// HTTPResponse is the buffered result of HTTPClient.Do.
type HTTPResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// ErrBreakerOpen is returned when the circuit breaker rejects a
// request without contacting the transport.
var ErrBreakerOpen = errors.New("behavior: http breaker open")

// Do executes one request through the client and returns the buffered
// response. The request body, if any, must be safely re-readable across
// retries — callers pass a fresh io.Reader each time by re-encoding;
// HTTPClient does NOT replay the original Body.
//
// Use DoJSON for JSON bodies (they're encoded fresh on every attempt).
func (c *HTTPClient) Do(ctx context.Context, method, path string, body io.Reader, headers http.Header) (*HTTPResponse, error) {
	if !c.allowRequest() {
		c.cfg.Metrics.IncCounter(c.cfg.MetricName + ".breaker_rejected")
		return nil, ErrBreakerOpen
	}

	u, err := c.resolve(path)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, fmt.Errorf("behavior: buildrequest: %w", err)
	}
	for k, vs := range headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	start := c.cfg.Clock()
	resp, doErr := c.cfg.Doer.Do(req)
	elapsed := c.cfg.Clock().Sub(start)
	c.cfg.Metrics.ObserveHistogram(c.cfg.MetricName+".latency_ms", float64(elapsed.Milliseconds()),
		"method", method, "path", path)

	if doErr != nil {
		c.cfg.Metrics.IncCounter(c.cfg.MetricName+".error", "method", method, "path", path)
		c.recordFailure()
		return nil, fmt.Errorf("behavior: http %s %s: %w", method, path, doErr)
	}
	defer func() { _ = resp.Body.Close() }()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		c.recordFailure()
		return nil, fmt.Errorf("behavior: read response: %w", err)
	}

	c.cfg.Metrics.IncCounter(c.cfg.MetricName+".status",
		"method", method, "path", path, "code", httpStatusBucket(resp.StatusCode))

	if resp.StatusCode >= 500 {
		c.recordFailure()
	} else {
		c.recordSuccess()
	}

	return &HTTPResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       bodyBytes,
	}, nil
}

// DoWithRetry repeats Do with exponential backoff on transient failures.
// `bodyFactory` is called fresh on every attempt so the body is
// re-readable. Pass nil when the request has no body.
func (c *HTTPClient) DoWithRetry(ctx context.Context, method, path string, bodyFactory func() (io.Reader, error), headers http.Header) (*HTTPResponse, error) {
	var lastErr error
	delay := c.cfg.InitialBackoff
	maxRetries := *c.cfg.MaxRetries // non-nil after NewHTTPClient
	for attempt := 0; attempt <= maxRetries; attempt++ {
		var body io.Reader
		if bodyFactory != nil {
			b, err := bodyFactory()
			if err != nil {
				return nil, fmt.Errorf("behavior: buildbody attempt=%d: %w", attempt, err)
			}
			body = b
		}
		resp, err := c.Do(ctx, method, path, body, headers)
		if err == nil && resp.StatusCode < 500 {
			return resp, nil
		}
		if err != nil {
			lastErr = err
			if errors.Is(err, ErrBreakerOpen) {
				return nil, err
			}
		} else {
			lastErr = fmt.Errorf("behavior: http %s %s: status %d", method, path, resp.StatusCode)
		}
		if attempt == maxRetries {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = nextBackoff(delay, c.cfg.MaxBackoff)
	}
	return nil, lastErr
}

func nextBackoff(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}

func (c *HTTPClient) resolve(path string) (string, error) {
	if c.cfg.BaseURL == "" {
		// allow absolute path-style calls
		if _, err := url.Parse(path); err != nil {
			return "", fmt.Errorf("behavior: parse url: %w", err)
		}
		return path, nil
	}
	base := strings.TrimRight(c.cfg.BaseURL, "/")
	if path == "" {
		return base, nil
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path, nil
}

func (c *HTTPClient) allowRequest() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consecutiveFails < c.cfg.BreakerThreshold {
		return true
	}
	// breaker open — allow one probe after cooldown
	if c.cfg.Clock().Sub(c.breakerOpenedAt) >= c.cfg.BreakerCooldown {
		if !c.breakerHalfProbed {
			c.breakerHalfProbed = true
			return true
		}
	}
	return false
}

func (c *HTTPClient) recordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consecutiveFails++
	switch {
	case c.consecutiveFails == c.cfg.BreakerThreshold:
		// Threshold crossing: open the breaker.
		c.breakerOpenedAt = c.cfg.Clock()
		c.breakerHalfProbed = false
		c.cfg.Metrics.IncCounter(c.cfg.MetricName + ".breaker_opened")
		c.cfg.Logger.Warn("behavior.http.breaker.opened",
			"threshold", c.cfg.BreakerThreshold,
			"cooldown_ms", c.cfg.BreakerCooldown.Milliseconds(),
		)
	case c.breakerHalfProbed:
		// A half-open probe failed: re-open with a FRESH cooldown window and
		// disarm the probe, so the next cooldown re-allows exactly one probe.
		// Without this the breaker stays open forever — consecutiveFails is now
		// past the threshold so the crossing branch never fires again, and
		// breakerHalfProbed stays true, so allowRequest never permits a probe.
		c.breakerOpenedAt = c.cfg.Clock()
		c.breakerHalfProbed = false
	}
}

func (c *HTTPClient) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consecutiveFails >= c.cfg.BreakerThreshold {
		c.cfg.Metrics.IncCounter(c.cfg.MetricName + ".breaker_closed")
		c.cfg.Logger.Info("behavior.http.breaker.closed")
	}
	c.consecutiveFails = 0
	c.breakerHalfProbed = false
}

func httpStatusBucket(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	}
	return "other"
}
