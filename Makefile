SHELL := /usr/bin/env bash

.PHONY: deps install build build-go build-release web web-dev all package test test-full test-strict lint check-activity-types check-data-plane-scope dev clean e2e-loop

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

# ----------------------------------------------------------------------------
# version — 一个发行版只有一个对外版本号：atoll 自己的 tag。它配的前端是
# atoll-web 的哪个 tag，记在 WEB_VERSION 里；两者一起烙进二进制（atoll version
# 会打印），因为一个装完就是一个文件的东西，必须自己说得清自己是谁。
#
# 工作树里编出来的是 dev —— 那是实话，别拿它冒充发行版。
# ----------------------------------------------------------------------------
VERSION     ?= $(shell git describe --tags --exact-match 2>/dev/null || echo dev)
WEB_VERSION ?= $(shell cat WEB_VERSION 2>/dev/null || echo none)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null)
DATE        ?= $(shell date -u +%F)
BUILDINFO   := github.com/wanpengxie/atoll/cmd/internal/buildinfo
LDFLAGS_VERSION := -X $(BUILDINFO).Version=$(VERSION) -X $(BUILDINFO).WebVersion=$(WEB_VERSION) \
                   -X $(BUILDINFO).Commit=$(COMMIT) -X $(BUILDINFO).Date=$(DATE)

LDFLAGS_RELEASE := -s -w $(LDFLAGS_VERSION)

build-go:
	@mkdir -p bin
	@for b in $(GO_BINARIES); do \
	  out=$$([ "$$b" = atoll ] && echo atoll || echo atoll-$$b); \
	  echo "[build] cmd/$$b -> bin/$$out"; \
	  go build -ldflags="$(LDFLAGS_VERSION)" -o bin/$$out ./cmd/$$b || exit 1; \
	done

build-release:
	@mkdir -p bin
	@for b in $(GO_BINARIES); do \
	  out=$$([ "$$b" = atoll ] && echo atoll || echo atoll-$$b); \
	  echo "[build-release] cmd/$$b -> bin/$$out (stripped)"; \
	  go build -ldflags="$(LDFLAGS_RELEASE)" -o bin/$$out ./cmd/$$b || exit 1; \
	done

# ----------------------------------------------------------------------------
# web — 按 WEB_VERSION 取 atoll-web 的那个 tag，构建，铺进 web/dist。
# 之后编出来的 atoll 里就带着这版 UI。
#
# web/dist 只有 index.html 进库（占位页，没有它 //go:embed 编不过）；这条命令
# 会覆盖它，所以跑完 git 会显示它改了——那是构建产物，别 commit，
# 用 `git checkout web/dist/index.html` 复原。
# ----------------------------------------------------------------------------
WEB_REPO   ?= https://github.com/wanpengxie/atoll-web.git
WEB_SRC    ?= $(CURDIR)/.cache/atoll-web

web:
	@[ "$(WEB_VERSION)" != none ] || { echo "WEB_VERSION 没写；发行版必须指名配的是哪个 atoll-web tag"; exit 1; }
	@rm -rf "$(WEB_SRC)"
	@mkdir -p "$(dir $(WEB_SRC))"
	git clone --depth 1 --branch "$(WEB_VERSION)" "$(WEB_REPO)" "$(WEB_SRC)"
	cd "$(WEB_SRC)" && npm ci && npm run build
	@rm -rf web/dist && mkdir -p web/dist
	cp -R "$(WEB_SRC)/dist/." web/dist/
	@test -f web/dist/index.html || { echo "atoll-web $(WEB_VERSION) 没产出 index.html"; exit 1; }
	@echo "[web] web/dist = atoll-web $(WEB_VERSION)"

# ----------------------------------------------------------------------------
# web-dev / all — 开发用：拿开发机上那份 atoll-web 工作树的当前状态（当前
# 分支、连未提交的改动一起）构建，不 clone、不认 tag。发行走的恒是上面的
# `web`（按 WEB_VERSION 取 tag）——两条路不能混：这条编出来的东西自称
# 什么版本由 git describe 决定，工作树里就是 dev。
#
#   make all        前后端一起：web-dev + build-go
#   make all WEB_WORKTREE=/别的/路径
# ----------------------------------------------------------------------------
WEB_WORKTREE ?= $(CURDIR)/../atoll-web

web-dev:
	@test -f "$(WEB_WORKTREE)/package.json" || { echo "找不到 atoll-web 工作树：$(WEB_WORKTREE)（用 WEB_WORKTREE=<路径> 指一个）"; exit 1; }
	@echo "[web-dev] 源 = $(WEB_WORKTREE) @ $$(cd "$(WEB_WORKTREE)" && git rev-parse --abbrev-ref HEAD)$$(cd "$(WEB_WORKTREE)" && git diff --quiet || echo '+本地改动')"
	cd "$(WEB_WORKTREE)" && npm run build
	@rm -rf web/dist && mkdir -p web/dist
	cp -R "$(WEB_WORKTREE)/dist/." web/dist/
	@test -f web/dist/index.html || { echo "atoll-web 没产出 index.html"; exit 1; }
	@echo "[web-dev] web/dist = $(WEB_WORKTREE) 的当前状态"

all: web-dev
	@$(MAKE) build-go WEB_VERSION="$$(cd "$(WEB_WORKTREE)" && git describe --tags --exact-match 2>/dev/null || echo "$$(cd "$(WEB_WORKTREE)" && git rev-parse --abbrev-ref HEAD)-$$(cd "$(WEB_WORKTREE)" && git rev-parse --short HEAD)")"

# ----------------------------------------------------------------------------
# package — 出四个平台的发行包 + checksums 到 dist/。
# mac 分 arm64/amd64 是硬要求（Intel 机和 Apple Silicon 不通用）；用户不用挑，
# install.sh 按 uname 自己选。
#
# 纯 Go（modernc sqlite），所以 CGO_ENABLED=0 下一台 linux 就能出全部平台，
# 不需要 mac runner。
# ----------------------------------------------------------------------------
PLATFORMS ?= darwin/arm64 darwin/amd64 linux/amd64 linux/arm64

package:
	@rm -rf dist && mkdir -p dist
	@for p in $(PLATFORMS); do \
	  os=$${p%/*}; arch=$${p#*/}; \
	  name=atoll_$(VERSION)_$${os}_$${arch}; \
	  stage=dist/$$name; \
	  mkdir -p $$stage; \
	  echo "[package] $$os/$$arch -> $$name.tar.gz"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -ldflags="$(LDFLAGS_RELEASE)" -o $$stage/atoll ./cmd/atoll || exit 1; \
	  cp scripts/install.sh $$stage/install.sh; \
	  cp LICENSE $$stage/ 2>/dev/null || true; \
	  printf 'atoll  %s\nweb    %s\ncommit %s\nbuilt  %s\n' "$(VERSION)" "$(WEB_VERSION)" "$(COMMIT)" "$(DATE)" > $$stage/VERSION; \
	  tar -czf dist/$$name.tar.gz -C dist $$name || exit 1; \
	  rm -rf $$stage; \
	done
	@cd dist && { command -v sha256sum >/dev/null && sha256sum *.tar.gz || shasum -a 256 *.tar.gz; } > checksums.txt
	@echo; ls -lh dist/

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
# dev — 起 server 于 /tmp/atoll-dev。首启自动安装（root 密码见日志），
# 重复运行恒重开同一个 home。web UI 跟 API 同端口（编进二进制；没跑过 make web
# 就是张占位页，改前端时用 atoll-web 的 npm run dev 连这个 API）。
# ----------------------------------------------------------------------------
dev: build-go
	@echo "[dev] server on :8832 (home: /tmp/atoll-dev/server)"
	@bin/atoll-server --home /tmp/atoll-dev/server --addr :8832 &

# ----------------------------------------------------------------------------
# clean — 删 build 产物（不动用户数据）
# ----------------------------------------------------------------------------
clean:
	rm -rf bin/
