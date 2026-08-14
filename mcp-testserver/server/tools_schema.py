"""Tools covering representative JSON Schema shapes."""

from __future__ import annotations

from typing import Annotated, Literal

from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import TypedDict


class Customer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    email: Annotated[str, Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: str
    quantity: int = Field(ge=1)
    unit_price: float = Field(alias="unitPrice", ge=0)


class Circle(BaseModel):
    kind: Literal["circle"]
    radius: float = Field(gt=0)


class Rectangle(BaseModel):
    kind: Literal["rectangle"]
    width: float = Field(gt=0)
    height: float = Field(gt=0)


Shape = Annotated[Circle | Rectangle, Field(discriminator="kind")]


class Report(TypedDict):
    title: str
    item_count: int
    labels: list[str]


def register(server: MCPServer) -> None:
    @server.tool(description="Create an order from a nested customer object and item array.", structured_output=False)
    def create_order(customer: Customer, items: list[OrderItem]) -> str:
        total = sum(item.quantity * item.unit_price for item in items)
        return f"order:{customer.email}:{len(items)}:{total:.2f}"

    @server.tool(description="Accept one of the low, normal, or urgent priority values.", structured_output=False)
    def set_priority(level: Literal["low", "normal", "urgent"]) -> str:
        return f"priority:{level}"

    @server.tool(description="Search with a required query, defaulted limit, and optional tag list.", structured_output=False)
    def search(query: str, limit: int = 10, tags: list[str] | None = None) -> str:
        return f"search:{query}:limit={limit}:tags={','.join(tags or [])}"

    @server.tool(description="Describe a circle or rectangle using $ref-backed oneOf schema branches.", structured_output=False)
    def describe_shape(shape: Shape) -> str:
        if isinstance(shape, Circle):
            return f"circle:{shape.radius}"
        return f"rectangle:{shape.width}x{shape.height}"

    @server.tool(description="Return a structured report validated against the advertised outputSchema.")
    def structured_report(title: str, labels: list[str]) -> Report:
        return Report(title=title, item_count=len(labels), labels=labels)
