SHELL := /usr/bin/env bash

.PHONY: install build build-go build-release build-ui test lint dev dev-ui dev-server clean

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

build-ui:
	cd web/ui && pnpm build

# ----------------------------------------------------------------------------
# test / lint
# ----------------------------------------------------------------------------
test:
	go test ./...

# lint — go vet + 架构约束（archtest：契约形状只许住 lib/introspect）
lint:
	go vet ./...
	go test ./archtest/

# ----------------------------------------------------------------------------
# dev — 一键起全栈: server + UI dev server
#   make dev        起 server + UI
#   make dev-server 只起 server
#   make dev-ui     只起 UI dev server
# ----------------------------------------------------------------------------
dev: build-go dev-server dev-ui

dev-server:
	@mkdir -p /tmp/atoll-dev/channels
	@echo "[dev] starting server on :8832"
	@bin/atoll-server \
	  --db /tmp/atoll-dev/app.db \
	  --channel-db-dir /tmp/atoll-dev/channels \
	  --addr :8832 &

dev-ui:
	@echo "[dev] starting UI on :5173 (proxy -> :8832)"
	cd web/ui && pnpm dev

# ----------------------------------------------------------------------------
# clean — 删 build 产物（不动用户数据）
# ----------------------------------------------------------------------------
clean:
	rm -rf bin/
