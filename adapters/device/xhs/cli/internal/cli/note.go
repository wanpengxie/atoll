package cli

import (
	"context"
	"strings"

	"github.com/coagent-ai/coagent/adapters/device/xhs/cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newGetNoteCmd 注册 `coagent-xhs get-note`。
//
// flag：
//   - `--note-id`    mock 模式必填；real 模式 `--url` 缺失时与 `--xsec-token` 共同必填。
//   - `--url`        real 模式：直接给完整 URL，或与 (`--note-id` + `--xsec-token`) 二选一。
//   - `--xsec-token` real 模式 `--url` 缺失时与 `--note-id` 共同必填。
//
// 不同模式校验语义：
//   - mock：require `--note-id`。
//   - real：require `--url` OR (`--note-id` && `--xsec-token`)。
//     R4-T1 收紧：xsec_token 单独无 note_id 在 XHS API 上是 dead-end（无法构造 explore URL），
//     与 daemon validator + extension 端三层合约对齐。
func newGetNoteCmd() *cobra.Command {
	var (
		noteID    string
		noteURL   string
		xsecToken string
	)

	cmd := &cobra.Command{
		Use:   "get-note",
		Short: "拉笔记详情",
		RunE: func(cmd *cobra.Command, _ []string) error {
			noteID = strings.TrimSpace(noteID)
			noteURL = strings.TrimSpace(noteURL)
			xsecToken = strings.TrimSpace(xsecToken)

			// 按 backend 分支校验：避免 mock 用户被 real 的 url/xsec_token 要求误伤，
			// 也避免 real 用户用 mock 的 note-id fixture 假定。
			isReal := xhs.IsRealBackendFromEnv()
			if isReal {
				hasURL := noteURL != ""
				hasNoteID := noteID != ""
				hasToken := xsecToken != ""
				// R4-T1 (round-3 codex#t61.1 / claude#t61.C1) 收紧合约：
				// real 模式 get-note 必须 --url，或 (--note-id && --xsec-token)。
				// xsec_token 单独无 note_id 在 XHS API 上是 dead-end，CLI 提前拒绝
				// 与 daemon validator + extension 端期望对齐。
				if !hasURL && !(hasNoteID && hasToken) {
					return NewCLIError(
						"invalid_argument",
						"real mode requires --url, or both --note-id and --xsec-token",
					)
				}
			} else {
				if noteID == "" {
					return NewCLIError("invalid_argument", "--note-id is required")
				}
			}

			argsIn := xhs.GetNoteArgs{
				NoteID:    noteID,
				URL:       noteURL,
				XsecToken: xsecToken,
			}
			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				return p.GetNote(ctx, argsIn)
			})
			return nil
		},
	}

	cmd.Flags().StringVarP(&noteID, "note-id", "n", "", "笔记 ID（mock 模式必填；real 模式 --url 缺失时与 --xsec-token 共同必填）")
	cmd.Flags().StringVar(&noteURL, "url", "", "笔记 URL（real 模式：直接给 URL，或与 --note-id+--xsec-token 二选一）")
	cmd.Flags().StringVar(&xsecToken, "xsec-token", "", "xsec_token（real 模式 --url 缺失时与 --note-id 共同必填）")
	return cmd
}
