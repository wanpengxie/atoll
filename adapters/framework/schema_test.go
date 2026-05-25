package framework

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestValidateTypeDeclaration_HappyPath asserts the install-time check
// accepts a TypeDeclaration with non-empty allowed_kinds + a known
// terminal_convention.
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload schema is
// NOT part of TypeDeclaration; install does not validate payload
// schemas. The validator only enforces allowed_kinds membership +
// terminal_convention enum.
func TestValidateTypeDeclaration_HappyPath(t *testing.T) {
	td := adapter.TypeDeclaration{
		AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
		TerminalConvention: "payload_status",
	}
	if err := ValidateTypeDeclaration("biz.x", td); err != nil {
		t.Errorf("happy path: %v", err)
	}
}

func TestValidateTypeDeclaration_AcceptsOptionalConventionFields(t *testing.T) {
	td := adapter.TypeDeclaration{
		AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
		TerminalConvention: "payload_status",
		Description:        "Publish a note",
		PayloadExample:     json.RawMessage(`{"title":"hello"}`),
		PayloadFields: []adapter.FieldDoc{{
			Name:        "title",
			Required:    true,
			Description: "Note title",
			Example:     "hello",
		}},
		ErrorCodes: []adapter.ErrorDoc{{
			Code:        "publish_timeout",
			Description: "Publishing timed out",
			Recovery:    "Retry after checking the browser session",
		}},
		Notes: "Use concise titles.",
	}
	if err := ValidateTypeDeclaration("biz.publish", td); err != nil {
		t.Fatalf("optional convention fields should not affect validation: %v", err)
	}
}

func TestValidateTypeDeclaration_RejectsBadCases(t *testing.T) {
	cases := []struct {
		name   string
		td     adapter.TypeDeclaration
		reason message.InstallReason
	}{
		{
			"empty-allowed-kinds",
			adapter.TypeDeclaration{},
			message.InstallTypeRegistryInvalid,
		},
		{
			"invalid-kind",
			adapter.TypeDeclaration{AllowedKinds: []message.Kind{"weird"}},
			message.InstallTypeRegistryInvalid,
		},
		{
			"duplicate-kind",
			adapter.TypeDeclaration{AllowedKinds: []message.Kind{
				message.KindEvent, message.KindEvent,
			}},
			message.InstallTypeRegistryInvalid,
		},
		{
			"bad-terminal-convention",
			adapter.TypeDeclaration{
				AllowedKinds:       []message.Kind{message.KindEvent},
				TerminalConvention: "weird-mode",
			},
			message.InstallTypeRegistryInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTypeDeclaration("biz.x", tc.td)
			if err == nil {
				t.Fatalf("expected error")
			}
			var ie *InstallError
			if !errors.As(err, &ie) {
				t.Fatalf("expected *InstallError, got %T: %v", err, err)
			}
			if ie.Reason != tc.reason {
				t.Errorf("reason=%s want=%s (err=%v)", ie.Reason, tc.reason, err)
			}
		})
	}
}
