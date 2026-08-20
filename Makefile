SHELL := /usr/bin/env bash

.PHONY: deps install build build-go build-release test test-full test-strict lint check-activity-types check-data-plane-scope dev clean e2e-loop

# server/daemon ship namespaced (atoll-server / atoll-daemon); the entry
# command itself is plain `atoll` — its own name IS the namespace.
GO_BINARIES := server daemon atoll

# ----------------------------------------------------------------------------
# deps — 拉全部依赖（此前叫 install；那个名字现在归下面的装机向导，因为
# 「装一个 atoll」是别人敲 make install 时想要的东西，不是拉依赖）
# ----------------------------------------------------------------------------
deps:
	go mod download

# ----------------------------------------------------------------------------
# install — 装一个节点：编译，然后跑交互式装机向导。
# 向导自己会问 home / 地址 / steward / 密码；--yes 全取默认（CI / 脚本）：
#   make install ARGS=--yes
# ----------------------------------------------------------------------------
install: build-go
	./scripts/install.sh $(ARGS)

# ----------------------------------------------------------------------------
# build
# ----------------------------------------------------------------------------
build: build-go

LDFLAGS_RELEASE := -s -w

build-go:
	@mkdir -p bin
	@for b in $(GO_BINARIES); do \
	  out=$$([ "$$b" = atoll ] && echo atoll || echo atoll-$$b); \
	  echo "[build] cmd/$$b -> bin/$$out"; \
	  go build -o bin/$$out ./cmd/$$b || exit 1; \
	done

build-release:
	@mkdir -p bin
	@for b in $(GO_BINARIES); do \
	  out=$$([ "$$b" = atoll ] && echo atoll || echo atoll-$$b); \
	  echo "[build-release] cmd/$$b -> bin/$$out (stripped)"; \
	  go build -ldflags="$(LDFLAGS_RELEASE)" -o bin/$$out ./cmd/$$b || exit 1; \
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
test-full: check-data-plane-scope
	go test -tags atolltestfast -race ./...

# test-strict — 不带任何测试加速 tag 的全真档（怀疑加速档掩盖了
# durability 相关行为时用；常规收口用 test-full 即可）。
test-strict:
	go test -race ./...

# e2e-loop — the black-box acceptance suite (two real OS processes over the
# portal wire dialect). 裸 go test ./... 不受影响（harness 自建二进制）。
e2e-loop: build-go
	ATOLL_E2E_BIN=$(PWD)/bin go test -count=1 ./e2e/ -v -timeout 600s

# lint — go vet + 架构约束（archtest：契约形状只许住 lib/introspect）
lint: check-activity-types
	go vet ./...
	go test ./archtest/

check-activity-types:
	./scripts/check-activity-types.sh
	./scripts/check-activity-types.sh --self-test

check-data-plane-scope:
	./scripts/check-data-plane-scope.sh

# ----------------------------------------------------------------------------
# dev — 起 server(API-only) 于 /tmp/atoll-dev。首启自动安装（root 密码见日志），
# 重复运行恒重开同一个 home。web UI 在独立仓库 atoll-web（连同一个 API）。
# ----------------------------------------------------------------------------
dev: build-go
	@echo "[dev] server on :8832 (home: /tmp/atoll-dev/server)"
	@bin/atoll-server --home /tmp/atoll-dev/server --addr :8832 &

# ----------------------------------------------------------------------------
# clean — 删 build 产物（不动用户数据）
# ----------------------------------------------------------------------------
clean:
	rm -rf bin/
