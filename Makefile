SHELL := /usr/bin/env bash

.PHONY: install build build-go build-release test lint check-gateway-retired check-link-seam-retired dev dev-init dev-reopen dev-server clean e2e-loop

GO_BINARIES := server daemon

# ----------------------------------------------------------------------------
# install — 拉全部依赖
# ----------------------------------------------------------------------------
install:
	go mod download

# ----------------------------------------------------------------------------
# build
# ----------------------------------------------------------------------------
build: build-go

LDFLAGS_RELEASE := -s -w

build-go:
	@mkdir -p bin
	@for b in $(GO_BINARIES); do \
	  echo "[build] cmd/$$b -> bin/atoll-$$b"; \
	  go build -o bin/atoll-$$b ./cmd/$$b || exit 1; \
	done

build-release:
	@mkdir -p bin
	@for b in $(GO_BINARIES); do \
	  echo "[build-release] cmd/$$b -> bin/atoll-$$b (stripped)"; \
	  go build -ldflags="$(LDFLAGS_RELEASE)" -o bin/atoll-$$b ./cmd/$$b || exit 1; \
	done

# ----------------------------------------------------------------------------
# test / lint
# ----------------------------------------------------------------------------
# test — 日常档：-short 跳过打标的慢测试（真实等待窗/重交错类，每个 skip
# 自带原因），atolltestfast 关掉测试进程内 sqlite 的 fsync（测试不做崩溃
# 恢复；生产二进制无此 tag，见 runtime/internal/store/pragma_sync.go）。
test:
	go test -tags atolltestfast -short -race ./...

# test-full — 收口门（合 main 前 / 批次终审前）：一个不跳全量跑。
test-full:
	go test -tags atolltestfast -race ./...

# test-strict — 不带任何测试加速 tag 的全真档（怀疑加速档掩盖了
# durability 相关行为时用；常规收口用 test-full 即可）。
test-strict:
	go test -race ./...

# e2e-loop — server frame contract + canonical authenticated server/daemon journey.
# 裸 go test ./... 不受影响（ATOLL_E2E_BIN 空则 skip）。
e2e-loop: build-go
	@for test in TestGatewayFrames TestDaemonBinaryCanonicalControl; do \
		ATOLL_E2E_BIN=$(PWD)/bin go test ./e2e/ -list "^$$test$$" | grep -qx "$$test" || { echo "[e2e] missing $$test" >&2; exit 1; }; \
	done
	ATOLL_E2E_BIN=$(PWD)/bin go test ./e2e/ -run '^(TestGatewayFrames|TestDaemonBinaryCanonicalControl)$$' -v -timeout 600s

# lint — go vet + 架构约束（archtest：契约形状只许住 lib/introspect）
lint: check-gateway-retired check-link-seam-retired
	go vet ./...
	go test ./archtest/

# DoD-4 expected-diff gate: retirement comments are the sole allowlist; any live
# identifier/string/semantic hit fails mechanically.
check-gateway-retired:
	./scripts/check-gateway-retired-symbols.sh

check-link-seam-retired:
	./scripts/check-link-seam-retired-symbols.sh

# ----------------------------------------------------------------------------
# dev — 起 server(API-only)。web UI 在独立仓库 atoll-web:
#   make dev UI_DIST=../atoll-web/dist  连本地构建好的 UI
#   UI dev server 在 atoll-web 仓里 pnpm dev
# ----------------------------------------------------------------------------
dev: build-go dev-reopen

dev-init: build-go
	@mkdir -p /tmp/atoll-dev/channels
	@echo "[dev] initializing server on :8832"
	@bin/atoll-server \
	  --db /tmp/atoll-dev/app.db \
	  --channel-db-dir /tmp/atoll-dev/channels \
	  --addr :8832 \
	  --init \
	  --ui-dist "$(UI_DIST)" &

dev-reopen:
	@mkdir -p /tmp/atoll-dev/channels
	@echo "[dev] reopening server on :8832"
	@bin/atoll-server \
	  --db /tmp/atoll-dev/app.db \
	  --channel-db-dir /tmp/atoll-dev/channels \
	  --addr :8832 \
	  --ui-dist "$(UI_DIST)" &

dev-server: dev-reopen

# ----------------------------------------------------------------------------
# clean — 删 build 产物（不动用户数据）
# ----------------------------------------------------------------------------
clean:
	rm -rf bin/
