.PHONY: dev build test lint clean

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

test:
	pnpm -r test

lint:
	pnpm -r lint

clean:
	pnpm -r clean
	rm -rf node_modules
