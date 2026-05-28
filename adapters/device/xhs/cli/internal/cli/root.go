// Package cli 实现 coagent-xhs binary 的 cobra command 层。
//
// 5 个子命令在 `root.go` 注册，各自薄壳：解析 flag → 调 Provider → WriteOK/WriteErr。
// 输出统一 JSON envelope（见 output.go）。
package cli

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// NewRootCommand 构造 coagent-xhs root cobra command。
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "coagent-xhs",
		Short: "coagent xhs CLI",
		Long: fmt.Sprintf(`coagent-xhs：xhs 业务命令行入口（M1.3-T14 v4 重写）。

5 个子命令：publish / search / recent / get-note / sync-cookie

real 模式内部 spawn %s 子进程，按 L4 §2.3.2 把命令翻译成
"%s ask --type xhs.<op> --audience tool:xhs --payload ..."。
legacy 命令 publish-status 已废弃；改为新增 sync-cookie（对应 v4
xhs.cookie.sync type）。

provider 切换 env：%s（mock|real，默认 mock）
real 模式必填 env：%s / %s / %s（兼容 legacy %s / %s）`,
			"coagent", "coagent",
			"COAGENT_XHS_BACKEND",
			"DAEMON_URL", "COAGENT_AUTH_TOKEN", "COAGENT_CHANNEL_ID",
			"COAGENT_DAEMON_HTTP", "COAGENT_DAEMON_TOKEN"),
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(
		newPublishCmd(),
		newSearchCmd(),
		newRecentCmd(),
		newGetNoteCmd(),
		newSyncCookieCmd(),
	)

	return root
}

// RunCLI 是 coagent-xhs binary 的可测入口。
//
// 把 cobra 解析失败 / RunE 返回错误统一转成 stdout JSON envelope（永不再写 stderr）：
//   - RunE 返回 *CLIError → envelope 用其 Code/Message
//   - cobra parse / unknown command / 其他 plain error → envelope code="usage_error"
//   - 成功 → 返回 ExitOK
//
// 注意：runWithProvider 内部走 os.Exit；那条路径不会回到这里。本函数只负责入口/解析层。
func RunCLI(args []string, stdout io.Writer) int {
	root := NewRootCommand()
	root.SetOut(stdout)
	root.SetErr(stdout)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		var ce *CLIError
		if errors.As(err, &ce) && ce != nil {
			_ = WriteErr(stdout, ce.Code, ce.Message)
		} else {
			_ = WriteErr(stdout, "usage_error", err.Error())
		}
		return ExitUsageError
	}
	return ExitOK
}
