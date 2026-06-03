package behavior

import (
	"testing"
	"time"
)

// A failed half-open probe must re-open the breaker with a fresh cooldown
// window and disarm the probe, so the NEXT cooldown re-allows exactly one
// probe. Regression: previously the open branch only fired on the exact
// threshold-crossing failure, so a probe failure (consecutiveFails past the
// threshold) left breakerHalfProbed=true forever and allowRequest never
// permitted another probe — the breaker stuck open permanently.
func TestBreaker_HalfOpenProbeFailureReArms(t *testing.T) {
	now := time.Unix(0, 0)
	c := NewHTTPClient(HTTPClientConfig{
		BreakerThreshold: 2,
		BreakerCooldown:  100 * time.Millisecond,
		Clock:            func() time.Time { return now },
	})

	// Trip the breaker: threshold consecutive failures.
	c.recordFailure()
	c.recordFailure()
	if c.allowRequest() {
		t.Fatal("breaker should be open immediately after the threshold is hit")
	}

	// Cooldown elapses → exactly one half-open probe is allowed.
	now = now.Add(150 * time.Millisecond)
	if !c.allowRequest() {
		t.Fatal("breaker should allow one half-open probe after cooldown")
	}
	if c.allowRequest() {
		t.Fatal("only one probe per cooldown window")
	}

	// The probe FAILS.
	c.recordFailure()

	// After another cooldown a fresh probe must be allowed again.
	now = now.Add(150 * time.Millisecond)
	if !c.allowRequest() {
		t.Fatal("after a failed half-open probe the breaker must re-arm and allow a new probe")
	}

	// A successful probe closes the breaker.
	c.recordSuccess()
	if !c.allowRequest() {
		t.Fatal("breaker should be closed after a successful probe")
	}
}
