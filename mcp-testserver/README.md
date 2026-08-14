# MCP v2 reference test server

This is a deterministic, in-memory MCP reference fixture implemented with the
official Python SDK pinned to `mcp==2.0.0`. It serves only protocol revision
`2026-07-28`; it has no authentication or persistence and creates no protocol
sessions.

## Install and run

From this directory:

```sh
python3 -m venv .venv
.venv/bin/pip install -e .
```

Run over stdio (the connection string is the executable plus arguments shown):

```sh
.venv/bin/python -m server.main --transport stdio
```

Run Streamable HTTP on its POST-only MCP endpoint:

```sh
.venv/bin/python -m server.main --transport http --port 8000
```

The HTTP connection URL is `http://127.0.0.1:8000/mcp`. Both commands build the
same server object and register the same tools, resources, prompts, extensions,
cache hints, and request-state security policy.

Run the conformance suite with the same official SDK environment:

```sh
.venv/bin/python -m unittest discover -s tests/conformance -v
```

## Tools

- `echo` — returns one input string unchanged.
- `add` — adds two numeric inputs.
- `create_order` — exercises nested customer and item-array input objects.
- `set_priority` — exercises a three-value string enum.
- `search` — combines a required query, defaulted limit, and optional tags.
- `describe_shape` — exercises `$ref` and `oneOf` with circle/rectangle variants.
- `structured_report` — advertises `outputSchema` and returns `structuredContent`.
- `fail_tool_error` — returns a business/tool failure with `isError: true`.
- `fail_protocol_error` — returns JSON-RPC Invalid Params (`-32602`).
- `slow_task` — emits periodic request-scoped progress notifications.
- `never_returns` — stays pending until the client times out or cancels.
- `log_when_asked` — attempts one log notification; the SDK sends it only for a request that opted in with `logLevel`.
- `book_ticket` — performs a form-elicitation MRTR confirmation with protected `requestState`.
- `toggle_extra_tool` — adds/removes `extra_tool` and publishes subscribed catalog/resource change events.
- `long_job` — returns synchronously unless the caller opted into the Tasks extension, in which case it returns a task handle.
- `extra_tool` — dynamic tool present only while enabled by `toggle_extra_tool`.

## Resources

- `test://text` — deterministic UTF-8 reference text.
- `test://binary` — deterministic arbitrary binary bytes (base64 on the wire).
- `test://counter` — changing in-memory counter updated by `toggle_extra_tool`.
- `test://greeting/{name}` — URI template that renders a greeting for `name`.

## Prompts

- `welcome` — no-argument welcome message.
- `review_topic` — one-argument prompt asking for a careful topic review.

The server also advertises `io.modelcontextprotocol/tasks` and implements
`tasks/get`, `tasks/update`, and `tasks/cancel`. Change notifications use
`subscriptions/listen`; the removed `resources/subscribe`,
`resources/unsubscribe`, HTTP GET stream, session ID, initialize handshake,
and resumable SSE mechanisms are not part of the served 2026 surface.
