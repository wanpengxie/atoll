package cli

import (
	"context"
	"strings"

	"github.com/wanpengxie/ActOS/adapters/device/xhs/cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newSearchCmd 注册 `coagent-xhs search <keyword> [--limit N]`。
func newSearchCmd() *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "search <keyword>",
		Short: "按关键词搜索笔记",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			keyword := strings.TrimSpace(args[0])
			if keyword == "" {
				return NewCLIError("invalid_argument", "keyword must not be empty")
			}
			searchArgs := xhs.SearchArgs{Keyword: keyword, Limit: limit}

			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				return p.Search(ctx, searchArgs)
			})
			return nil
		},
	}

	cmd.Flags().IntVarP(&limit, "limit", "l", 10, "返回条数上限")
	return cmd
}
