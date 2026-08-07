package base

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

func TestPersistRetriesBackoffAndEmitsLoudObsAtThreshold(t *testing.T) {
	sys := newTestSys()
	attempts := 0
	sys.state.put = func(resource.ResourceID, []byte) (accessdoor.Outcome, error) {
		attempts++
		if attempts <= 5 {
			return accessdoor.Outcome{}, errors.New("state unavailable")
		}
		return accessdoor.Outcome{}, nil
	}
	var waits []time.Duration
	persistLoop(sys, ResumeSeedKey, []byte("thread"), func(_ context.Context, delay time.Duration) bool {
		waits = append(waits, delay)
		return true
	})
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond}
	if len(waits) != len(want) {
		t.Fatalf("waits=%v", waits)
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("waits=%v want=%v", waits, want)
		}
	}
	if len(sys.obs) != 1 || sys.obs[0] != ObsCheckpointDrop {
		t.Fatalf("obs=%v", sys.obs)
	}
}
