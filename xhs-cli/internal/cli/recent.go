package cli

import (
	"context"

	"github.com/coagent-ai/xhs-cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newGetMyRecentCmd 注册 `coagent-xhs get-my-recent [--limit N]`。
func newGetMyRecentCmd() *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "get-my-recent",
		Short: "获取当前账号最近发布列表",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			argsIn := xhs.GetMyRecentArgs{Limit: limit}
			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				return p.GetMyRecent(ctx, argsIn)
			})
			return nil
		},
	}

	cmd.Flags().IntVarP(&limit, "limit", "l", 10, "返回条数上限")
	return cmd
}
