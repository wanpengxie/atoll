---
archived_at: 2026-05-14
reason: M1.5-T9 — M1.3 期间过渡用 Go daemon 模块；M1.5 收口后由根 module 的 cmd/daemon 取代
replaced_by: cmd/daemon/（根 go.mod 模块下）+ runtime/* + kernel/*
safe_to_delete_after: M2.0 启动前
---

历史背景：

`lightcone/daemon-go/` 是 M1.3 期间为了配合 v4 protocol 验证而单独立起的
Go 模块（独立 go.mod / go-arch-lint / kernel-deps-check 等 CI 脚手架），
作为 lightcone Node daemon → 单栈 Go 的过渡载体。

M1.5 完成后：
- 根目录已有 `go.mod` + `kernel/` + `runtime/` + `cmd/daemon/`
- 顶层 `.go-arch-lint.yaml` 统一管理 6 大 ownership invariants
- `.github/workflows/go-ci.yml`（专为 daemon-go 设计的 workflow）随本归档一并删除

旧 daemon-go 保留以便参考 v4 protocol 实现 / sqlite store schema / actor harness 等
设计细节；M2.0 启动前可删。

注意：`.go-arch-lint.yaml` `scripts/kernel-deps-check.sh` `go.mod` `go.sum` 等历史
脚手架文件原样保留，不与根模块合并。
