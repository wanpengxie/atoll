# cli

`xhs` is the coagent business-layer CLI for XiaoHongShu actions. **Mock-only** in M1.0.

## Build

```bash
pnpm --filter @coagent/cli build
```

Build output:
- `cli/dist/index.js`
- `cli/bin/xhs`

## Usage

```bash
./cli/bin/xhs publish --title "测试标题" --content /tmp/note.md --images /tmp/a.jpg,/tmp/b.jpg
./cli/bin/xhs search 奶茶
./cli/bin/xhs get-my-recent --limit 3
./cli/bin/xhs get-note --note-id 01HXYZ
./cli/bin/xhs publish-status --note-id 01HXYZ
```

Mock fixtures live in `cli/src/mock/fixtures/`.

## Status

- Mock backend implemented with fixed CLI contract (input args + JSON output envelope).
- Real backend (live xhs.com integration) is **out of scope for M1.0** and will be re-evaluated post-dogfood.
