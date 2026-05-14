// coagent-xhs binary entrypoint.
//
// 提供 5 个 xhs 子命令（publish / search / get-my-recent / get-note /
// publish-status），按 COAGENT_XHS_BACKEND env 切换 mock/real provider。
//
// 入口逻辑全部委托给 cli.RunCLI：parse 错误、RunE 返回错误统一以 stdout JSON envelope
// 输出，stderr 永远为空，便于 agent 端单源解析。
package main

import (
	"os"

	"github.com/coagent-ai/coagent/adapters/device/xhs/cli/internal/cli"
)

func main() {
	os.Exit(cli.RunCLI(os.Args[1:], os.Stdout))
}
