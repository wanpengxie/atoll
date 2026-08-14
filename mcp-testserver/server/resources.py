"""Static, binary, changing, and templated resources."""

from __future__ import annotations

from dataclasses import dataclass

from mcp.server.mcpserver import MCPServer

TEXT_URI = "test://text"
BINARY_URI = "test://binary"
CHANGING_URI = "test://counter"
TEMPLATE_URI = "test://greeting/{name}"


@dataclass
class ResourceState:
    counter: int = 0


def register(server: MCPServer, state: ResourceState) -> None:
    @server.resource(
        TEXT_URI,
        name="reference-text",
        description="A deterministic UTF-8 text resource.",
        mime_type="text/plain",
    )
    def reference_text() -> str:
        return "MCP 2026-07-28 reference text"

    @server.resource(
        BINARY_URI,
        name="reference-binary",
        description="A deterministic binary resource.",
        mime_type="application/octet-stream",
    )
    def reference_binary() -> bytes:
        return b"\x00MCP-v2\xff"

    @server.resource(
        CHANGING_URI,
        name="changing-counter",
        description="An in-memory counter changed by toggle_extra_tool.",
        mime_type="text/plain",
    )
    def changing_counter() -> str:
        return str(state.counter)

    @server.resource(
        TEMPLATE_URI,
        name="greeting-template",
        description="Render a greeting for the URI path parameter.",
        mime_type="text/plain",
    )
    def greeting(name: str) -> str:
        return f"Hello, {name}!"
