package framework

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientHappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(HTTPClientConfig{BaseURL: srv.URL, Timeout: 2 * time.Second})
	resp, err := c.Do(context.Background(), http.MethodGet, "/x", nil, nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.StatusCode != 200 || string(resp.Body) != `{"ok":true}` {
		t.Fatalf("resp mismatch: %d %s", resp.StatusCode, resp.Body)
	}
}

func TestHTTPClientRetriesOn5xx(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		if hits < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := NewHTTPClient(HTTPClientConfig{
		BaseURL:        srv.URL,
		Timeout:        time.Second,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	})
	resp, err := c.DoWithRetry(context.Background(), http.MethodPost, "/x",
		func() (io.Reader, error) { return bytes.NewReader([]byte("body")), nil },
		nil,
	)
	if err != nil {
		t.Fatalf("DoWithRetry: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 after retry, got %d", resp.StatusCode)
	}
	if hits != 3 {
		t.Fatalf("hits got %d want 3", hits)
	}
}

func TestHTTPClientBreakerOpens(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	clock := newFixedClock(time.Unix(0, 0))
	logger := &recordingLogger{}
	metrics := NewMemoryMetrics()
	c := NewHTTPClient(HTTPClientConfig{
		BaseURL:          srv.URL,
		Timeout:          200 * time.Millisecond,
		BreakerThreshold: 3,
		BreakerCooldown:  100 * time.Millisecond,
		Clock:            clock.Now,
		Logger:           logger,
		Metrics:          metrics,
	})

	// Three failures trip the breaker (5xx isn't an error from Do's
	// perspective but recordFailure is still called inside Do).
	for i := 0; i < 3; i++ {
		_, _ = c.Do(context.Background(), http.MethodGet, "/x", nil, nil)
	}
	// Fourth call should be rejected by breaker.
	_, err := c.Do(context.Background(), http.MethodGet, "/x", nil, nil)
	if !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("expected ErrBreakerOpen, got %v", err)
	}
	if logger.containsValue("framework.http.breaker.opened") == false {
		// log msg is a positional arg
		found := false
		for _, line := range logger.snapshot() {
			if line.msg == "framework.http.breaker.opened" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected breaker opened log line, got %+v", logger.snapshot())
		}
	}
	if got := metrics.Counter("adapter.http.breaker_rejected"); got < 1 {
		t.Fatalf("breaker_rejected counter not incremented: %d", got)
	}
}

func TestHTTPClientResolvesBaseURL(t *testing.T) {
	c := NewHTTPClient(HTTPClientConfig{BaseURL: "https://example.com/api/"})
	got, err := c.resolve("/v1/x")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	want := "https://example.com/api/v1/x"
	if got != want {
		t.Fatalf("resolve: got %q want %q", got, want)
	}
	got, _ = c.resolve("v1/x")
	if got != want {
		t.Fatalf("resolve no-leading-slash: got %q want %q", got, want)
	}
}
