.PHONY: check-l0 install dev build deploy register doctor smoke test lint clean

KNOWN_TARGETS := check-l0 install dev build deploy register doctor smoke test lint clean
PASS_ARGS := $(strip $(ARGS) $(filter-out $(KNOWN_TARGETS),$(MAKECMDGOALS)))

%:
	@:

check-l0:
	@command -v node >/dev/null || (echo "Missing node. Install Node.js 20+." >&2; exit 1)
	@node -e "const major=Number(process.versions.node.split('.')[0]); if (major < 20) { console.error('Node.js 20+ required, found '+process.versions.node); process.exit(1); }"
	@command -v pnpm >/dev/null || (echo "Missing pnpm. Install pnpm 9+." >&2; exit 1)
	@pnpm -v | node -e "let v=''; process.stdin.on('data', c=>v+=c); process.stdin.on('end',()=>{const major=Number(v.trim().split('.')[0]); if (major < 9) { console.error('pnpm 9+ required, found '+v.trim()); process.exit(1); }})"
	@command -v pm2 >/dev/null || (echo "Missing pm2. Install with: npm i -g pm2" >&2; exit 1)
	@command -v claude >/dev/null || (echo "Missing claude CLI. Install and authenticate Claude Code." >&2; exit 1)
	@command -v mysql >/dev/null || (echo "Missing mysql client. Install mysql-client/mysql-client-core." >&2; exit 1)

install: check-l0
	pnpm install

dev:
	pnpm -r --parallel dev

build:
	pnpm --filter @coagent/agent-binary build
	pnpm --filter @coagent/cli build
	pnpm --filter lightcone build
	pnpm --filter @lightcone-ai/daemon build
	pnpm --filter feishu-bridge build
	pnpm --filter mysql-mcp-server build
	pnpm --filter platform-actions-mcp-server build
	pnpm --filter publisher-mcp-server build

deploy: install build

register:
	node ops/register-machine.mjs $(PASS_ARGS)

doctor:
	node ops/doctor.mjs $(PASS_ARGS)

smoke:
	node lightcone/daemon/scripts/smoke-channel-runtime.mjs $(PASS_ARGS)

test:
	pnpm -r test

lint:
	pnpm -r lint

clean:
	pnpm -r clean
	rm -rf node_modules
