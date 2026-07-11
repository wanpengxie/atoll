package link

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/ipc"
)

func TestAckCodedErrorsRoundTrip(t *testing.T) {
	for _, sentinel := range []error{ErrWriterNotLive, ErrAccessNotLive, ErrScheduleNotLive} {
		code, message := ipc.EncodeError(fmt.Errorf("wrapped: %w", sentinel))
		if code == "" || message == "" {
			t.Fatalf("EncodeError(%v) = (%q,%q), want both fields", sentinel, code, message)
		}
		if got := decodeAckError(code, message); !errors.Is(got, sentinel) {
			t.Fatalf("decodeAckError(%q,%q) = %v, want %v", code, message, got, sentinel)
		}
	}
}

func TestControlBudgetsAreNoNarrowerThanStreamWrites(t *testing.T) {
	if streamWriteBudget < 10*time.Second {
		t.Fatalf("stream write budget = %s, want >=10s", streamWriteBudget)
	}
	if reattachTimeout < streamWriteBudget || controlRPCTimeout < streamWriteBudget {
		t.Fatalf("control budgets (%s,%s) must cover stream write budget %s", reattachTimeout, controlRPCTimeout, streamWriteBudget)
	}
}

func TestAckCodedErrorsUnknownAndEmpty(t *testing.T) {
	if got := decodeAckError("future_code", "future message"); got == nil || got.Error() != "future message" {
		t.Fatalf("unknown code fallback = %v", got)
	}
	if got := decodeAckError("", ""); got != nil {
		t.Fatalf("empty error fields = %v, want nil", got)
	}
}
