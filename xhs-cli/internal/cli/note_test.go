package cli

import (
	"errors"
	"strings"
	"testing"
)

// TestGetNoteRealMode_RequiresUrlOrXsecToken（fix-spec.md §Fix-T1.3）：
// real 模式下 get-note 至少需要 --url 或 --xsec-token 之一非空，
// 否则在调 daemon 之前就以 invalid_argument CLIError 返回。
func TestGetNoteRealMode_RequiresUrlOrXsecToken(t *testing.T) {
	t.Setenv("COAGENT_XHS_BACKEND", "real")

	cmd := newGetNoteCmd()
	cmd.SetArgs([]string{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error in real mode without --url/--xsec-token")
	}
	var ce *CLIError
	if !errors.As(err, &ce) || ce == nil || ce.Code != "invalid_argument" {
		t.Fatalf("expected invalid_argument CLIError, got %v", err)
	}
	if !strings.Contains(ce.Message, "--url") || !strings.Contains(ce.Message, "--xsec-token") {
		t.Fatalf("expected message to mention both --url and --xsec-token, got %q", ce.Message)
	}
}

// real 模式下提供 --note-id 但缺 --url/--xsec-token 仍应被拒：
// note_id 单独不足以对齐 extension 的页面定位。
func TestGetNoteRealMode_NoteIDAloneInsufficient(t *testing.T) {
	t.Setenv("COAGENT_XHS_BACKEND", "real")

	cmd := newGetNoteCmd()
	cmd.SetArgs([]string{"--note-id", "01HXYZ"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error: real mode rejects note-id alone")
	}
	var ce *CLIError
	if !errors.As(err, &ce) || ce.Code != "invalid_argument" {
		t.Fatalf("expected invalid_argument, got %v", err)
	}
}

// mock 模式仍然只要求 --note-id（不要求 --url/--xsec-token）。
func TestGetNoteMockMode_RequiresNoteID(t *testing.T) {
	t.Setenv("COAGENT_XHS_BACKEND", "")

	cmd := newGetNoteCmd()
	cmd.SetArgs([]string{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error in mock mode without --note-id")
	}
	var ce *CLIError
	if !errors.As(err, &ce) || ce.Code != "invalid_argument" {
		t.Fatalf("expected invalid_argument, got %v", err)
	}
	if !strings.Contains(ce.Message, "--note-id") {
		t.Fatalf("expected message to mention --note-id, got %q", ce.Message)
	}
}
