package log_test

import (
	"strings"
	"testing"

	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestSeqOrdering — Seq is a typed int64 with native ordering.
func TestSeqOrdering(t *testing.T) {
	a := klog.Seq(1)
	b := klog.Seq(2)
	if a >= b {
		t.Errorf("Seq(1) should be < Seq(2)")
	}
	// Round-trip through int64.
	if int64(a) != 1 {
		t.Errorf("int64(Seq(1))=%d want 1", int64(a))
	}
}

// TestAppendErrorFormat — Error format includes reason; Detail appended
// when present.
func TestAppendErrorFormat(t *testing.T) {
	e := &klog.AppendError{
		Reason: message.HarnessTerminalDuplicate,
		Detail: "race lost on parent_id=req-1",
	}
	got := e.Error()
	if !strings.Contains(got, string(message.HarnessTerminalDuplicate)) {
		t.Errorf("Error()=%q missing reason wire form", got)
	}
	if !strings.Contains(got, "race lost on parent_id=req-1") {
		t.Errorf("Error()=%q missing detail", got)
	}
}

// TestAppendErrorBareReason — Error returns just the reason when Detail
// is empty.
func TestAppendErrorBareReason(t *testing.T) {
	e := &klog.AppendError{Reason: message.HarnessMessageIDConflict}
	if got := e.Error(); got != string(message.HarnessMessageIDConflict) {
		t.Errorf("Error()=%q want %q", got, message.HarnessMessageIDConflict)
	}
}

// TestAppendResultZeroValue — zero result has Seq=0, IsTerminal=false,
// Deduped=false (used by the harness to distinguish "no row written").
func TestAppendResultZeroValue(t *testing.T) {
	var r klog.AppendResult
	if r.Seq != 0 || r.IsTerminal || r.Deduped {
		t.Errorf("zero AppendResult=%+v want {0 false false}", r)
	}
}

// TestCursorZero — Cursor zero value is a valid starting point (cursor 0
// per L1 §6.3.4.3 — actor never advanced).
func TestCursorZero(t *testing.T) {
	var c klog.Cursor
	if c.LastConsumedSeq != 0 {
		t.Errorf("zero LastConsumedSeq=%d want 0", c.LastConsumedSeq)
	}
	if c.LastConsumedID != "" {
		t.Errorf("zero LastConsumedID=%q want empty", c.LastConsumedID)
	}
}

// TestAppendErrorIsError — *AppendError satisfies the error interface
// (defensive: callers use errors.As).
func TestAppendErrorIsError(t *testing.T) {
	var err error = &klog.AppendError{Reason: message.HarnessAuthFailed}
	if err.Error() == "" {
		t.Error("AppendError as error returns empty string")
	}
}
