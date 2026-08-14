"""Command-line entry point for both required transports."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from typing import Any

from mcp.server.caching import CacheHint
from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.mcpserver import MCPServer
from mcp.server.request_state import RequestStateSecurity
from mcp.shared.exceptions import MCPError
from mcp_types import (
    UNSUPPORTED_PROTOCOL_VERSION,
    DiscoverResult,
    LoggingCapability,
    UnsupportedProtocolVersionErrorData,
)

from . import PROTOCOL_VERSION, SERVER_NAME, SERVER_VERSION
from . import prompts, resources, tools_basic, tools_failure, tools_interactive, tools_schema


class ProtocolPolicy:
    """Pin the only served revision and make the remaining logging surface honest."""

    async def __call__(
        self,
        ctx: ServerRequestContext[Any, Any],
        call_next: CallNext,
    ) -> HandlerResult:
        if ctx.protocol_version != PROTOCOL_VERSION:
            data = UnsupportedProtocolVersionErrorData(
                supported=[PROTOCOL_VERSION], requested=ctx.protocol_version
            ).model_dump(by_alias=True, mode="json")
            raise MCPError(
                UNSUPPORTED_PROTOCOL_VERSION,
                "Unsupported protocol version",
                data=data,
            )
        result = await call_next(ctx)
        if isinstance(result, DiscoverResult):
            # The fixture emits request-scoped notifications/message, so the
            # specification requires it to advertise logging even though
            # logging/setLevel is intentionally not registered.
            capabilities = result.capabilities.model_copy(update={"logging": LoggingCapability()})
            return result.model_copy(update={"capabilities": capabilities})
        if ctx.method == "server/discover" and isinstance(result, Mapping):
            wire = dict(result)
            capabilities = dict(wire.get("capabilities") or {})
            capabilities["logging"] = {}
            wire["capabilities"] = capabilities
            return wire
        return result


def build_server() -> MCPServer:
    task_extension = tools_interactive.TasksExtension()
    cache_hints = {
        "server/discover": CacheHint(ttl_ms=60_000, scope="public"),
        "tools/list": CacheHint(ttl_ms=0, scope="public"),
        "prompts/list": CacheHint(ttl_ms=60_000, scope="public"),
        "resources/list": CacheHint(ttl_ms=0, scope="public"),
        "resources/templates/list": CacheHint(ttl_ms=60_000, scope="public"),
        "resources/read": CacheHint(ttl_ms=0, scope="private"),
    }
    server = MCPServer(
        SERVER_NAME,
        version=SERVER_VERSION,
        instructions="Deterministic MCP 2026-07-28 protocol and tool conformance fixture.",
        extensions=[task_extension],
        cache_hints=cache_hints,
        request_state_security=RequestStateSecurity.ephemeral(ttl=300, audience=SERVER_NAME),
        middleware=[ProtocolPolicy()],
    )
    resource_state = resources.ResourceState()
    tools_basic.register(server)
    tools_schema.register(server)
    tools_failure.register(server)
    resources.register(server, resource_state)
    prompts.register(server)
    tools_interactive.register(server, resource_state)
    return server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=("stdio", "http"), default="stdio")
    parser.add_argument("--port", type=int, default=8000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = build_server()
    if args.transport == "stdio":
        server.run("stdio")
    else:
        server.run(
            "streamable-http",
            host="127.0.0.1",
            port=args.port,
            streamable_http_path="/mcp",
            stateless_http=True,
            json_response=False,
        )


if __name__ == "__main__":
    main()
