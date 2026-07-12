package platform

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform/internal/subjectgate"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// humancell_verbs.go: the frame interpreter's small mapping helpers — Sys-verb
// error → error frame (裁决8), duration/timer/list-query conversions, and the
// resource Outcome → wire ResourceOutcome projection.

// mapVerbErr folds a Sys identity-verb error into an error frame. A typed
// WriteRejected surfaces its harness reason VERBATIM as the flat code (裁决8 平面
// 词律); every other error (membrane transient during teardown, infra) is the
// retryable unavailable code — never a raw internal string on the wire.
func mapVerbErr(err error, errFrame frameErr) subjectgate.Frame {
	var wr *actorbase.WriteRejected
	if errors.As(err, &wr) {
		return errFrame(wr.Reason, wr.Detail)
	}
	return errFrame(subjectgate.CodeUnavailable, err.Error())
}

func durationMs(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

func scheduleTimerID(s string) schedule.TimerID { return schedule.TimerID(s) }

func listQueryOf(q *subjectgate.ResourceQuery) accessdoor.ListQuery {
	if q == nil {
		return accessdoor.ListQuery{}
	}
	return accessdoor.ListQuery{Prefix: q.Prefix, Cursor: q.Cursor, Limit: q.Limit}
}

// resourceOutcomeFrame projects an accessdoor.Outcome (create/read/write/delete/
// share) into the wire ResourceOutcome form. A domain reject rides status; a Go
// error is infra/transport and maps through mapVerbErr. A read value that is not
// itself valid JSON is wrapped as a JSON string so the receipt always marshals
// (the wire field is json.RawMessage — arbitrary bytes must not break it).
func resourceOutcomeFrame(out accessdoor.Outcome, err error, receipt frameBuild, errFrame frameErr) subjectgate.Frame {
	if err != nil {
		return mapVerbErr(err, errFrame)
	}
	o := subjectgate.ResourceOutcome{Status: "ok"}
	if !out.Accepted() {
		o.Status = "rejected"
		o.Detail = string(out.RejectReason)
		return receipt(o)
	}
	if out.Found && len(out.Value) > 0 {
		if json.Valid(out.Value) {
			o.Value = json.RawMessage(out.Value)
		} else {
			raw, _ := json.Marshal(string(out.Value))
			o.Value = raw
		}
	}
	return receipt(o)
}
