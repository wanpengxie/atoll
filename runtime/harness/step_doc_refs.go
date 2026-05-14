package harness

import (
	"context"
	"strings"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepDocRefs implements L1 §10.2 step 7 — doc_refs path validation.
// Each entry must be a relative path that does not escape the channel
// workdir; absolute paths and `..` traversal segments are rejected.
type stepDocRefs struct{}

func newStepDocRefs(_ Deps) khar.Step { return &stepDocRefs{} }

func (s *stepDocRefs) ID() khar.StepID { return khar.StepDocRefs }

func (s *stepDocRefs) Run(_ context.Context, env *message.Envelope) (khar.Outcome, error) {
	if env.DocRefs == nil {
		return khar.Outcome{}, nil
	}
	for _, p := range *env.DocRefs {
		if p == "" {
			return khar.Outcome{
				RejectReason: message.HarnessDocRefsInvalid,
				Detail:       "doc_refs contains empty entry",
			}, nil
		}
		if strings.HasPrefix(p, "/") {
			return khar.Outcome{
				RejectReason: message.HarnessDocRefsInvalid,
				Detail:       "doc_refs absolute path: " + p,
			}, nil
		}
		// `..` anywhere in the path (split on / or \) is unsafe.
		segs := strings.FieldsFunc(p, func(r rune) bool { return r == '/' || r == '\\' })
		for _, seg := range segs {
			if seg == ".." {
				return khar.Outcome{
					RejectReason: message.HarnessDocRefsInvalid,
					Detail:       "doc_refs path escapes channel workdir: " + p,
				}, nil
			}
		}
	}
	return khar.Outcome{}, nil
}
