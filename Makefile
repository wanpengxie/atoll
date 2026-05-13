.PHONY: check-l0 install logrotate-config dev build deploy register doctor doctor-offline smoke test lint clean check-banned-mcp daemon-go-build daemon-go-test daemon-go-lint daemon-go-tidy

KNOWN_TARGETS := check-l0 install logrotate-config dev build deploy register doctor doctor-offline smoke test lint clean check-banned-mcp daemon-go-build daemon-go-test daemon-go-lint daemon-go-tidy
PASS_ARGS := $(strip $(ARGS) $(filter-out $(KNOWN_TARGETS),$(MAKECMDGOALS)))

# daemon-go (M1.3-T0+): Go re-implementation of lightcone/daemon. Node
# daemon stays online during the dual-stack window — both stacks build
# off the root Makefile.
DAEMON_GO_DIR := lightcone/daemon-go

%:
	@:

check-l0:
	@if command -v node >/dev/null; then node -e "const major=Number(process.versions.node.split('.')[0]); if (major < 20) { console.error('[missing] node '+process.versions.node+' (need 20+)'); process.exit(1); } console.log('[ok] node '+process.versions.node);"; else echo "[missing] node - install Node.js 20+." >&2; exit 1; fi
	@if command -v pnpm >/dev/null; then version=$$(pnpm -v); major=$${version%%.*}; if [ "$$major" -ge 9 ]; then echo "[ok] pnpm $$version"; else echo "[missing] pnpm $$version (need 9+)" >&2; exit 1; fi; else echo "[missing] pnpm - install pnpm 9+." >&2; exit 1; fi
	@if command -v pm2 >/dev/null; then echo "[ok] pm2 $$(pm2 --version | head -n 1)"; else echo "[missing] pm2 - install with: npm i -g pm2"; fi
	@if command -v claude >/dev/null; then echo "[ok] claude $$(claude --version | head -n 1)"; else echo "[missing] claude - install and authenticate Claude Code"; fi
	@if command -v mysql >/dev/null; then echo "[ok] mysql $$(mysql --version | head -n 1)"; else echo "[missing] mysql - install mysql-client/mysql-client-core"; fi

install: check-l0
	pnpm install
	pm2 install pm2-logrotate >/dev/null 2>&1 || true

logrotate-config: check-l0
	pm2 set pm2-logrotate:max_size 100M
	pm2 set pm2-logrotate:retain 10
	pm2 set pm2-logrotate:compress true

dev:
	pnpm -r --parallel dev

build: check-l0 daemon-go-build
	pnpm --filter @coagent/agent-binary build
	pnpm --filter @coagent/cli build
	pnpm --filter lightcone build
	pnpm --filter @lightcone-ai/daemon build
	pnpm --filter feishu-bridge build

deploy: install build

register: check-l0
	node ops/register-machine.mjs $(PASS_ARGS)

doctor: check-l0
	node ops/doctor.mjs $(PASS_ARGS)

doctor-offline: check-l0
	node ops/doctor.mjs --offline

smoke: check-l0
	node lightcone/daemon/scripts/smoke-channel-runtime.mjs $(PASS_ARGS)

test: check-l0 check-banned-mcp daemon-go-test
	node --test ops/*.test.mjs
	pnpm -r test

lint: daemon-go-lint
	pnpm -r lint

# --- daemon-go (Go) targets --------------------------------------------------

daemon-go-tidy:
	cd $(DAEMON_GO_DIR) && go mod tidy

daemon-go-build:
	cd $(DAEMON_GO_DIR) && go build ./...

daemon-go-test:
	cd $(DAEMON_GO_DIR) && go test ./...

daemon-go-lint:
	@if command -v golangci-lint >/dev/null; then \
		cd $(DAEMON_GO_DIR) && golangci-lint run --config=../../.golangci.yml ./... ; \
	else \
		echo "[skip] golangci-lint not installed; run \"go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest\"" >&2 ; \
	fi

check-banned-mcp:
	bash scripts/check-banned-mcp.sh

clean:
	pnpm -r clean
	rm -rf node_modules
