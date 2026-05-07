package cli

import (
	"context"
	"strings"

	"github.com/coagent-ai/xhs-cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newPublishStatusCmd 注册 `coagent-xhs publish-status --note-id <ID>`。
func newPublishStatusCmd() *cobra.Command {
	var noteID string

	cmd := &cobra.Command{
		Use:   "publish-status",
		Short: "查询某次发布的状态",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(noteID) == "" {
				return NewCLIError("invalid_argument", "--note-id is required")
			}
			argsIn := xhs.PublishStatusArgs{NoteID: noteID}
			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				return p.PublishStatus(ctx, argsIn)
			})
			return nil
		},
	}

	cmd.Flags().StringVarP(&noteID, "note-id", "n", "", "笔记 ID（必填）")
	return cmd
}
