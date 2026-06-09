SHELL := /usr/bin/env bash

.PHONY: install build build-go build-ui test dev dev-ui dev-server clean

GO_BINARIES := server daemon

# ----------------------------------------------------------------------------
# install — 拉全部依赖
# ----------------------------------------------------------------------------
install:
	go mod download
	cd web/ui && pnpm install

# ----------------------------------------------------------------------------
# build
# ----------------------------------------------------------------------------
build: build-go build-ui

build-go:
	@mkdir -p bin
	@for b in $(GO_BINARIES); do \
	  echo "[build] cmd/$$b -> bin/coagent-$$b"; \
	  go build -o bin/coagent-$$b ./cmd/$$b || exit 1; \
	done

build-ui:
	cd web/ui && pnpm build

# ----------------------------------------------------------------------------
# test
# ----------------------------------------------------------------------------
test:
	go test ./...

# ----------------------------------------------------------------------------
# dev — 一键起全栈: server + UI dev server
#   make dev        起 server + UI
#   make dev-server 只起 server
#   make dev-ui     只起 UI dev server
# ----------------------------------------------------------------------------
dev: build-go dev-server dev-ui

dev-server:
	@mkdir -p /tmp/coagent-dev/channels
	@echo "[dev] starting server on :8832"
	@bin/coagent-server \
	  --db /tmp/coagent-dev/app.db \
	  --channel-db-dir /tmp/coagent-dev/channels \
	  --addr :8832 &

dev-ui:
	@echo "[dev] starting UI on :5173 (proxy -> :8832)"
	cd web/ui && pnpm dev

# ----------------------------------------------------------------------------
# clean — 删 build 产物（不动用户数据）
# ----------------------------------------------------------------------------
clean:
	rm -rf bin/
