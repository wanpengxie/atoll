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

// TestGetNoteRealMode_XsecTokenAloneInsufficient（fix-spec.md §R4-T1 / round-3 codex#t61.1 / claude#t61.C1）：
// real 模式下提供 --xsec-token 但缺 --url/--note-id 仍应被拒：xsec_token 单独无 note_id
// 在 XHS API 上是 dead-end（无法构造 explore URL），与 daemon validator + extension 端期望对齐。
// 这是三层合约（CLI/daemon/extension）收紧后的正向锁定。
func TestGetNoteRealMode_XsecTokenAloneInsufficient(t *testing.T) {
	t.Setenv("COAGENT_XHS_BACKEND", "real")

	cmd := newGetNoteCmd()
	cmd.SetArgs([]string{"--xsec-token", "tk"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error: real mode rejects xsec-token alone")
	}
	var ce *CLIError
	if !errors.As(err, &ce) || ce.Code != "invalid_argument" {
		t.Fatalf("expected invalid_argument, got %v", err)
	}
	if !strings.Contains(ce.Message, "--note-id") {
		t.Fatalf("expected message to mention --note-id (tightened contract), got %q", ce.Message)
	}
}

// 注：(--note-id && --xsec-token) 与 --url 的正向通路由
// real_provider_test.go::TestRealProvider_GetNote_DispatchShape 的
// "all-three-given" / "note-id-and-token" / "url-only" cases 锁定，
// 此处不重复（CLI 校验通过后会进入 runWithProvider，os.Exit 会让单测复杂化）。

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
