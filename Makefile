SHELL := /usr/bin/env bash

.PHONY: install build build-go build-release test lint dev dev-server clean e2e-loop

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
test:
	go test -race ./...

# e2e-loop — C1 最小闭环：真双进程六段旅程（e2e/ 黑盒 harness）。
# 裸 go test ./... 不受影响（ATOLL_E2E_BIN 空则 skip）。
e2e-loop: build-go
	ATOLL_E2E_BIN=$(PWD)/bin go test ./e2e/ -run TestLoop -v -timeout 300s

# lint — go vet + 架构约束（archtest：契约形状只许住 lib/introspect）
lint:
	go vet ./...
	go test ./archtest/

# ----------------------------------------------------------------------------
# dev — 起 server(API-only)。web UI 在独立仓库 atoll-web:
#   make dev UI_DIST=../atoll-web/dist  连本地构建好的 UI
#   UI dev server 在 atoll-web 仓里 pnpm dev
# ----------------------------------------------------------------------------
dev: build-go dev-server

dev-server:
	@mkdir -p /tmp/atoll-dev/channels
	@echo "[dev] starting server on :8832"
	@bin/atoll-server \
	  --db /tmp/atoll-dev/app.db \
	  --channel-db-dir /tmp/atoll-dev/channels \
	  --addr :8832 \
	  --ui-dist "$(UI_DIST)" &

# ----------------------------------------------------------------------------
# clean — 删 build 产物（不动用户数据）
# ----------------------------------------------------------------------------
clean:
	rm -rf bin/
