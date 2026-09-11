# Pinned Pi bridge asset

`bridge.mjs` is a checked-in, self-contained phase-one runtime asset embedded
by `bridge.go`. Actor startup never runs npm and never downloads `latest`.

- source repository: `badlogic/pi-mono`
- source commit: `9767ba275f3e9a5ee0f5c5342249b629ab1b2282`
- packages: `@earendil-works/pi-ai` and `@earendil-works/pi-agent-core` 0.85.1
- package lock: the `package-lock.json` at that commit
- bundler: esbuild 0.28.1
- output SHA-256: `4e71911461dec2ee9c75a676a374e2dfe2e38c8e92de0258c61633a993bbb87c`
- runtime prerequisite: Node.js >= 22.19.0; startup verifies the version
- Atoll bridge revision: v10 (v9 behavior retained; Windows helper lookup treats environment names case-insensitively)
- upstream license: MIT; see `PI_LICENSE.txt`

The auditable source boundary is `bridge.ts`. From a clean pinned Pi checkout
whose workspace packages have been built, regenerate with:

```sh
NODE_PATH="$PWD/node_modules" npx esbuild \
  ../atoll/drivers/tools/pibridge/bridge.ts \
  --bundle --platform=node --format=esm --target=node22 \
  --outfile=../atoll/drivers/tools/pibridge/bridge.mjs \
  --alias:@atoll-pi-coding/grep="$PWD/packages/coding-agent/src/core/tools/grep.ts" \
  --alias:@atoll-pi-coding/find="$PWD/packages/coding-agent/src/core/tools/find.ts" \
  --alias:@atoll-pi-coding/ls="$PWD/packages/coding-agent/src/core/tools/ls.ts" \
  --alias:@atoll-pi-coding/powershell="$PWD/packages/coding-agent/src/core/tools/powershell.ts" \
  '--banner:js=import { createRequire as __atollCreateRequire } from "node:module"; const require = __atollCreateRequire(import.meta.url);'
```

The four `@atoll-pi-coding/*` names are build-only aliases. Keep them pointed
at the individual pinned tool modules: importing the `pi-coding-agent` package
root also bundles its session, UI, and extension entry points, which are not
part of this bridge.

Then compare the digest and run `go test ./drivers/tools/pibridge -count=1`.

Offline converter contracts can be run with
`node --test drivers/tools/pibridge/provider_contract.test.mjs` from the Atoll
repository with the pinned sibling Pi checkout built. These use `onPayload`
capture before any network request. HTTP error capture uses the pinned
Anthropic/OpenAI adapters' per-request fetch; Google adapters reject custom
fetch and currently return conservative unknown errors if Pi erases their
structured status. Unknown errors are not retried by string matching.
Changing the source commit, dependency graph, bridge protocol, or bundle
requires a new `Version` in `bridge.go` and a new recorded digest.
