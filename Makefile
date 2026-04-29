.PHONY: dev build test lint clean

dev:
	pnpm -r --parallel dev

build:
	pnpm -r build

test:
	pnpm -r test

lint:
	pnpm -r lint

clean:
	pnpm -r clean
	rm -rf node_modules
