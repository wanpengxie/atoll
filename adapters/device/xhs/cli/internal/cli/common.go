package cli

import (
	"context"
	"os"

	"github.com/coagent-ai/coagent/adapters/device/xhs/cli/internal/xhs"
	"github.com/spf13/cobra"
)

// runWithProvider 是 5 子命令的共享 runner：
//
//  1. 按 env 构造 Provider；构造失败按 envelope 输出 + ExitRuntime 退出。
//  2. 调 fn(ctx, provider)；失败按 envelope 输出 + ExitRuntime 退出。
//  3. 成功把返回 data 写成成功 envelope。
//
// 命令实现只关心如何拼参数 + 选哪个 Provider 方法。
//
// 输出走 cmd.OutOrStdout() 而非直接 os.Stdout，便于测试中通过 root.SetOut 截获。
func runWithProvider(cmd *cobra.Command, fn func(ctx context.Context, p xhs.Provider) (any, error)) {
	out := cmd.OutOrStdout()

	provider, err := xhs.NewProviderFromEnv()
	if err != nil {
		_ = WriteErrFrom(out, err)
		os.Exit(ExitRuntime)
	}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	data, err := fn(ctx, provider)
	if err != nil {
		_ = WriteErrFrom(out, err)
		os.Exit(ExitRuntime)
	}

	if err := WriteOK(out, data); err != nil {
		// 写 stdout 都失败的话已经无能为力，仅以 ExitRuntime 退出。
		os.Exit(ExitRuntime)
	}
}
