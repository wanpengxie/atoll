package cli

import (
	"context"
	"strings"

	"github.com/coagent-ai/xhs-cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newGetNoteCmd 注册 `coagent-xhs get-note`。
//
// flag：
//   - `--note-id` mock 模式必填，用于 fixture lookup；real 模式可选。
//   - `--url`        real 模式可选；与 --xsec-token 至少其一必须非空。
//   - `--xsec-token` real 模式可选；与 --url 至少其一必须非空。
//
// 不同模式校验语义：
//   - mock：require `--note-id`。
//   - real：require `--url` 或 `--xsec-token` 至少其一非空（NoteID 仍可作为 fallback 一并下发）。
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
				if noteURL == "" && xsecToken == "" {
					return NewCLIError("invalid_argument", "--url or --xsec-token is required in real mode")
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

	cmd.Flags().StringVarP(&noteID, "note-id", "n", "", "笔记 ID（mock 模式必填）")
	cmd.Flags().StringVar(&noteURL, "url", "", "笔记 URL（real 模式与 --xsec-token 至少其一）")
	cmd.Flags().StringVar(&xsecToken, "xsec-token", "", "xsec_token（real 模式与 --url 至少其一）")
	return cmd
}
