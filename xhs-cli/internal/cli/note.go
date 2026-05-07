package cli

import (
	"context"
	"strings"

	"github.com/coagent-ai/xhs-cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newGetNoteCmd 注册 `coagent-xhs get-note --note-id <ID>`。
func newGetNoteCmd() *cobra.Command {
	var noteID string

	cmd := &cobra.Command{
		Use:   "get-note",
		Short: "拉笔记详情",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(noteID) == "" {
				return NewCLIError("invalid_argument", "--note-id is required")
			}
			argsIn := xhs.GetNoteArgs{NoteID: noteID}
			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				return p.GetNote(ctx, argsIn)
			})
			return nil
		},
	}

	cmd.Flags().StringVarP(&noteID, "note-id", "n", "", "笔记 ID（必填）")
	return cmd
}
