package harness

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// Deps wiring contract: Validate refuses half-wired assemblies, New fills the
// NowMs / Logger defaults so a caller may omit them.

func TestDeps_ValidateMissingFields(t *testing.T) {
	if err := (Deps{}).Validate(); err == nil {
		t.Fatalf("empty Deps should fail Validate")
	}
	// ChannelID set, Log nil → missing-Log branch.
	if err := (Deps{ChannelID: testChannelID}).Validate(); err == nil {
		t.Fatalf("missing Log should fail")
	}
	// Fully wired → nil. (No ActorRegistry dep — the sender door trusts the
	// pen weld.)
	lg := stubLog{}
	if err := (Deps{ChannelID: testChannelID, Log: lg, Presence: testAuthority{}}).Validate(); err != nil {
		t.Fatalf("fully-wired Deps Validate = %v, want nil", err)
	}
}

// New must fill NowMs / Logger defaults when nil.
func TestNew_FillsDefaults(t *testing.T) {
	lg := stubLog{
		appendFn: func(context.Context, *message.Envelope, bool) (storespec.AppendResult, error) {
			return storespec.AppendResult{Seq: 1}, nil
		},
		findByID:   func(context.Context, message.ID) (*storespec.StoredRow, bool, error) { return nil, false, nil },
		hasFinalFn: func(context.Context, message.ID) (bool, error) { return false, nil },
	}
	// NowMs / Logger nil → defaults filled.
	m, _, err := New(Deps{ChannelID: testChannelID, Log: lg, Presence: testAuthority{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c := m.(*minter).chain
	// Drive a successful write so the default NowMs is actually invoked and a
	// real (non-test) clock value lands in ts_received.
	e := validEvent("m-default", "agent:p")
	res, err := c.write(ctxCallerKind("agent:p", actor.KindAgent), e)
	if err != nil || !res.Accepted() {
		t.Fatalf("write with defaulted deps: err=%v reason=%q", err, res.RejectReason)
	}
	if e.TSReceived == 0 {
		t.Fatalf("default NowMs not applied (ts_received still 0)")
	}
}
