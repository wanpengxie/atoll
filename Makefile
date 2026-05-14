# Top-level Makefile — M1.5 工程纪律入口
#
# Spec: .dalek/pm/m1.5-tickets.md §T8
# 顶层 target 与 §15.1 对齐：install / build / build-go / build-ui / build-ext /
# test / lint / migrate / dev / clean
#
# 容错策略：对未到位的目录 / 配置 / 命令做 graceful skip（打印 `[skip] …`
# 并继续）。M1.5 之后单栈：build-go / lint 只面向根 module
# （cmd/{server,daemon,worker,cli}），不存在第二个 go module。

SHELL := /usr/bin/env bash

.PHONY: install build build-go build-ui build-ext test lint migrate dev clean \
        lint-go lint-arch lint-banned-words lint-kernel-protocol lint-docs

# v5 Go 二进制（cmd/<bin>/main.go 由 T6/T7 落地）。
GO_BINARIES := server daemon worker cli

# ----------------------------------------------------------------------------
# install — 拉依赖、安装 lint / migrate 工具
# ----------------------------------------------------------------------------
install:
	@echo "[install] go mod download"
	@if [ -f go.mod ]; then go mod download; else echo "[skip] root go.mod absent (T2 pending)"; fi
	@echo "[install] pnpm install"
	pnpm install
	@echo "[install] go install lint / migrate tools (best-effort)"
	@command -v golangci-lint >/dev/null 2>&1 || \
	  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest || \
	  echo "[warn] golangci-lint install failed; install manually"
	@command -v go-arch-lint >/dev/null 2>&1 || \
	  go install github.com/fe3dback/go-arch-lint@latest || \
	  echo "[warn] go-arch-lint install failed; install manually"
	@command -v migrate >/dev/null 2>&1 || \
	  go install -tags 'sqlite3' github.com/golang-migrate/migrate/v4/cmd/migrate@latest || \
	  echo "[warn] golang-migrate install failed; install manually"

# ----------------------------------------------------------------------------
# build — Go binaries + UI + chrome extension
# ----------------------------------------------------------------------------
build: build-go build-ui build-ext

build-go:
	@mkdir -p bin
	@if [ -f go.mod ]; then \
	  for b in $(GO_BINARIES); do \
	    if [ -d "cmd/$$b" ]; then \
	      echo "[build-go] cmd/$$b -> bin/coagent-$$b"; \
	      go build -o bin/coagent-$$b ./cmd/$$b || exit 1; \
	    else \
	      echo "[skip] cmd/$$b not present (T6/T7 pending)"; \
	    fi; \
	  done; \
	else \
	  echo "[skip] root go.mod absent (T2 pending)"; \
	fi

build-ui:
	@if [ -f ui/package.json ]; then \
	  echo "[build-ui] pnpm --filter ui build"; \
	  pnpm --filter ui build; \
	else \
	  echo "[skip] ui/ not present (T7 pending)"; \
	fi

build-ext:
	@if [ -f adapters/device/xhs/extension/app/chrome-extension/package.json ]; then \
	  echo "[build-ext] pnpm --filter coagent-xhs-extension build"; \
	  pnpm --filter coagent-xhs-extension build; \
	else \
	  echo "[skip] xhs extension package not present"; \
	fi

# ----------------------------------------------------------------------------
# test — Go + JS
# ----------------------------------------------------------------------------
test:
	@if [ -f go.mod ]; then \
	  echo "[test] go test ./..."; \
	  go test ./...; \
	fi
	@echo "[test] pnpm -r --if-present test"
	@pnpm -r --if-present test

# ----------------------------------------------------------------------------
# lint — 5 类 lint 全跑（spec §T8）
#   1) golangci-lint        Go 代码风格 / 静态检查
#   2) go-arch-lint         component-level import 边界（T2 提供 .go-arch-lint.yaml）
#   3) banned-words         分层文本扫描（CODE_DIRS / ACTIVE_SPEC 严格 + 历史 grandfather）
#   4) 协议合规             kernel/ Go test（T1 提供 envelope_test / kind_test / reason_test / contract_test）
#   5) 文档 lint            .dalek/pm 文档交叉引用路径校验
# ----------------------------------------------------------------------------
lint: lint-go lint-arch lint-banned-words lint-kernel-protocol lint-docs

lint-go:
	@# 范围：lint-go 覆盖根模块（kernel/ runtime/ adapters/ server/ cmd/ pkg/）。
	@# M1.5 单栈：只有一个 go module，不会有第二个 go module。
	@if [ ! -f go.mod ]; then \
	  echo "[skip] lint-go: root go.mod absent (T2 pending)"; \
	elif ! command -v golangci-lint >/dev/null 2>&1; then \
	  echo "[skip] lint-go: golangci-lint not installed; run 'make install' or 'go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest'"; \
	else \
	  echo "[lint-go] golangci-lint run ./..."; \
	  golangci-lint run ./... || exit 1; \
	fi

lint-arch:
	@if [ ! -f .go-arch-lint.yaml ]; then \
	  echo "[skip] .go-arch-lint.yaml not present (T2 pending)"; \
	elif ! command -v go-arch-lint >/dev/null 2>&1; then \
	  echo "[skip] go-arch-lint not installed; run 'make install'"; \
	else \
	  echo "[lint-arch] go-arch-lint check"; \
	  go-arch-lint check --arch-file .go-arch-lint.yaml; \
	fi

lint-banned-words:
	@echo "[lint-banned-words] scripts/lint-banned-words.sh"
	@bash scripts/lint-banned-words.sh

lint-kernel-protocol:
	@if [ -d kernel ] && [ -f go.mod ]; then \
	  echo "[lint-kernel-protocol] go test ./kernel/..."; \
	  go test ./kernel/... || exit 1; \
	else \
	  echo "[skip] kernel/ Go module not present (T1 pending)"; \
	fi

lint-docs:
	@echo "[lint-docs] scripts/lint-docs.sh"
	@bash scripts/lint-docs.sh

# ----------------------------------------------------------------------------
# migrate — server-side sqlite schema（T6 落地 server/store/migrations/）
# ----------------------------------------------------------------------------
migrate:
	@if [ ! -d server/store/migrations ]; then \
	  echo "[skip] server/store/migrations/ not present (T6 pending)"; \
	elif ! command -v migrate >/dev/null 2>&1; then \
	  echo "[skip] migrate CLI not installed; run 'make install'"; \
	else \
	  mkdir -p data; \
	  migrate -path server/store/migrations -database "sqlite3://./data/server.db" up; \
	fi

# ----------------------------------------------------------------------------
# dev — 本地起 server + daemon + ui dev server（v5 二进制；缺位则提示）
# ----------------------------------------------------------------------------
dev:
	@echo "[dev] v5 dev stack (best-effort)"
	@if [ -x bin/coagent-server ]; then \
	  echo "[dev] starting bin/coagent-server &"; \
	  ./bin/coagent-server & \
	else \
	  echo "[skip] bin/coagent-server not built (run make build first)"; \
	fi
	@if [ -x bin/coagent-daemon ]; then \
	  echo "[dev] starting bin/coagent-daemon &"; \
	  ./bin/coagent-daemon --server-url ws://localhost:8080 --key dev & \
	else \
	  echo "[skip] bin/coagent-daemon not built (run make build first)"; \
	fi
	@if [ -f ui/package.json ]; then \
	  pnpm --filter ui dev; \
	else \
	  echo "[skip] ui/ not present (T7 pending); foreground process exits."; \
	fi

# ----------------------------------------------------------------------------
# clean — 删 build 产物（不动 .dalek / 用户数据）
# ----------------------------------------------------------------------------
clean:
	rm -rf bin/ dist/ ui/dist
	@pnpm -r --if-present clean 2>/dev/null || true
