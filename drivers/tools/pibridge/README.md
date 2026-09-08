# Pinned Pi bridge asset

`bridge.mjs` is a checked-in, self-contained phase-one runtime asset embedded
by `bridge.go`. Actor startup never runs npm and never downloads `latest`.

- source repository: `badlogic/pi-mono`
- source commit: `9767ba275f3e9a5ee0f5c5342249b629ab1b2282`
- packages: `@earendil-works/pi-ai` and `@earendil-works/pi-agent-core` 0.85.1
- package lock: the `package-lock.json` at that commit
- bundler: esbuild 0.28.1
- output SHA-256: `2b177ee75dcafcf9362754d04b969e9a2906f4be059e0ffcad78776f80d81cb5`
- runtime prerequisite: Node.js >= 22.19.0; startup verifies the version
- Atoll bridge revision: v5 (provider/runtime environment policies, deterministic faux tool-call fixture, Pi-native tool argument validation, payload-error classification, and per-Provider-Actor API-key injection)
- upstream license: MIT; see `PI_LICENSE.txt`

The auditable source boundary is `bridge.ts`. From a clean pinned Pi checkout
whose workspace packages have been built, regenerate with:

```sh
NODE_PATH="$PWD/node_modules" npx esbuild \
  ../atoll/drivers/tools/pibridge/bridge.ts \
  --bundle --platform=node --format=esm --target=node22 \
  --outfile=../atoll/drivers/tools/pibridge/bridge.mjs \
  '--banner:js=import { createRequire as __atollCreateRequire } from "node:module"; const require = __atollCreateRequire(import.meta.url);'
```

Then compare the digest and run `go test ./drivers/tools/pibridge -count=1`.
Changing the source commit, dependency graph, bridge protocol, or bundle
requires a new `Version` in `bridge.go` and a new recorded digest.
