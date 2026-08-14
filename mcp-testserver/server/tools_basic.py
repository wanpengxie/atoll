"""Baseline tools."""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer


def register(server: MCPServer) -> None:
    @server.tool(description="Return the supplied string unchanged.", structured_output=False)
    def echo(text: str) -> str:
        return text

    @server.tool(description="Add two numbers and return their sum.", structured_output=False)
    def add(a: float, b: float) -> float:
        return a + b
