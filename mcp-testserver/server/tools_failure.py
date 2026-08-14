"""Business failure, protocol failure, progress, cancellation, and logging tools."""

from __future__ import annotations

import warnings

import anyio
from mcp.server.mcpserver import Context, MCPServer
from mcp.shared.exceptions import MCPDeprecationWarning, MCPError
from mcp_types import INVALID_PARAMS, CallToolResult, TextContent
from pydantic import Field
from typing_extensions import Annotated


def register(server: MCPServer) -> None:
    @server.tool(description="Return a normal tool result marked isError=true.", structured_output=False)
    def fail_tool_error() -> CallToolResult:
        return CallToolResult(
            content=[TextContent(type="text", text="intentional tool execution failure")],
            is_error=True,
        )

    @server.tool(description="Raise a JSON-RPC Invalid Params protocol error.", structured_output=False)
    def fail_protocol_error() -> str:
        raise MCPError(code=INVALID_PARAMS, message="intentional invalid parameters")

    @server.tool(description="Wait for a bounded interval while reporting periodic progress.", structured_output=False)
    async def slow_task(
        seconds: Annotated[float, Field(ge=0.06, le=5.0)],
        ctx: Context,
    ) -> str:
        for step in range(1, 4):
            await anyio.sleep(seconds / 3)
            await ctx.report_progress(step, 3, f"step {step} of 3")
        return "slow task complete"

    @server.tool(description="Never complete unless the client cancels or disconnects.", structured_output=False)
    async def never_returns() -> str:
        await anyio.sleep_forever()
        raise AssertionError("unreachable")

    @server.tool(
        description="Emit one request-scoped log message only when the request opts in with logLevel.",
        structured_output=False,
    )
    async def log_when_asked(ctx: Context) -> str:
        # Logging is deprecated, but the build fixture deliberately covers its
        # remaining 2026 request-scoped compatibility rule.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", MCPDeprecationWarning)
            await ctx.info({"event": "log_when_asked"}, logger_name="mcp-testserver")
        return "log attempt complete"
