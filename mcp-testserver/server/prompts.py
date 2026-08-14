"""Prompt fixtures."""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer


def register(server: MCPServer) -> None:
    @server.prompt(name="welcome", description="A no-argument welcome prompt.")
    def welcome() -> str:
        return "Welcome to the MCP v2 reference test server."

    @server.prompt(name="review_topic", description="Ask the model to review a caller-supplied topic.")
    def review_topic(topic: str) -> str:
        return f"Review the following topic carefully: {topic}"
