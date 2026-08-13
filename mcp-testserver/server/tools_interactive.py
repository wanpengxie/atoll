"""MRTR, dynamic catalog, and Tasks extension fixtures."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.extension import Extension, MethodBinding
from mcp.server.mcpserver import Context, MCPServer
from mcp.shared.exceptions import MCPError
from mcp_types import (
    INVALID_PARAMS,
    MISSING_REQUIRED_CLIENT_CAPABILITY,
    CallToolRequestParams,
    CallToolResult,
    ElicitRequest,
    ElicitRequestFormParams,
    ElicitResult,
    InputRequiredResult,
    InputResponses,
    Request,
    RequestParams,
    Result,
    TextContent,
)

from .resources import CHANGING_URI, ResourceState

TASKS_EXTENSION_ID = "io.modelcontextprotocol/tasks"


class CreateTaskResult(Result):
    result_type: Literal["task"] = "task"
    task_id: str
    status: Literal["working", "input_required", "completed", "failed", "cancelled"]
    status_message: str | None = None
    created_at: str
    last_updated_at: str
    ttl_ms: int | None
    poll_interval_ms: int | None = None


class GetTaskParams(RequestParams):
    task_id: str


class GetTaskRequest(Request[GetTaskParams, Literal["tasks/get"]]):
    method: Literal["tasks/get"] = "tasks/get"
    params: GetTaskParams
    name_param = "taskId"


class UpdateTaskParams(RequestParams):
    task_id: str
    input_responses: InputResponses


class UpdateTaskRequest(Request[UpdateTaskParams, Literal["tasks/update"]]):
    method: Literal["tasks/update"] = "tasks/update"
    params: UpdateTaskParams
    name_param = "taskId"


class CancelTaskRequest(Request[GetTaskParams, Literal["tasks/cancel"]]):
    method: Literal["tasks/cancel"] = "tasks/cancel"
    params: GetTaskParams
    name_param = "taskId"


class GetTaskResult(Result):
    result_type: Literal["complete"] = "complete"
    task_id: str
    status: Literal["working", "input_required", "completed", "failed", "cancelled"]
    status_message: str | None = None
    created_at: str
    last_updated_at: str
    ttl_ms: int | None
    poll_interval_ms: int | None = None
    input_requests: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


class UpdateTaskResult(Result):
    result_type: Literal["complete"] = "complete"


class CancelTaskResult(Result):
    result_type: Literal["complete"] = "complete"


@dataclass
class _TaskRecord:
    task_id: str
    created_at: str
    last_updated_at: str
    created_monotonic: float
    duration: float
    status: Literal["working", "completed", "cancelled"] = "working"


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class TasksExtension(Extension):
    """Small in-memory implementation of the official Tasks extension draft."""

    identifier = TASKS_EXTENSION_ID

    def __init__(self) -> None:
        self._tasks: dict[str, _TaskRecord] = {}

    def methods(self) -> Sequence[MethodBinding]:
        modern = frozenset({"2026-07-28"})
        return (
            MethodBinding("tasks/get", GetTaskParams, self._get, modern),
            MethodBinding("tasks/update", UpdateTaskParams, self._update, modern),
            MethodBinding("tasks/cancel", GetTaskParams, self._cancel, modern),
        )

    async def intercept_tool_call(
        self,
        params: CallToolRequestParams,
        ctx: ServerRequestContext[Any, Any],
        call_next: CallNext,
    ) -> HandlerResult:
        if params.name != "long_job":
            return await call_next(ctx)

        extensions = ctx.session.client_capabilities.extensions if ctx.session.client_capabilities else None
        if not extensions or TASKS_EXTENSION_ID not in extensions:
            # The non-opted-in path is deliberately a normal synchronous tool result.
            return await call_next(ctx)

        seconds = float((params.arguments or {}).get("seconds", 0.25))
        task_id = str(uuid.uuid4())
        now = _now()
        self._tasks[task_id] = _TaskRecord(
            task_id=task_id,
            created_at=now,
            last_updated_at=now,
            created_monotonic=time.monotonic(),
            duration=seconds,
        )
        return CreateTaskResult(
            task_id=task_id,
            status="working",
            status_message="Long job accepted",
            created_at=now,
            last_updated_at=now,
            ttl_ms=60_000,
            poll_interval_ms=50,
        )

    def _lookup(self, task_id: str) -> _TaskRecord:
        task = self._tasks.get(task_id)
        if task is None:
            raise MCPError(INVALID_PARAMS, "Unknown task", data={"taskId": task_id})
        if task.status == "working" and time.monotonic() - task.created_monotonic >= task.duration:
            task.status = "completed"
            task.last_updated_at = _now()
        return task

    async def _get(self, ctx: ServerRequestContext[Any, Any], params: GetTaskParams) -> GetTaskResult:
        task = self._lookup(params.task_id)
        final = None
        if task.status == "completed":
            final = CallToolResult(
                content=[TextContent(type="text", text="long job completed asynchronously")]
            ).model_dump(by_alias=True, mode="json", exclude_none=True)
        return GetTaskResult(
            task_id=task.task_id,
            status=task.status,
            status_message=f"Task is {task.status}",
            created_at=task.created_at,
            last_updated_at=task.last_updated_at,
            ttl_ms=60_000,
            poll_interval_ms=50,
            result=final,
        )

    async def _update(self, ctx: ServerRequestContext[Any, Any], params: UpdateTaskParams) -> UpdateTaskResult:
        # long_job has no outstanding input requests; the extension specification
        # says unknown/already-satisfied response keys should be ignored.
        self._lookup(params.task_id)
        return UpdateTaskResult()

    async def _cancel(self, ctx: ServerRequestContext[Any, Any], params: GetTaskParams) -> CancelTaskResult:
        task = self._lookup(params.task_id)
        if task.status == "working":
            task.status = "cancelled"
            task.last_updated_at = _now()
        return CancelTaskResult()


def register(server: MCPServer, resource_state: ResourceState) -> None:
    @server.tool(
        description="Book a ticket after a form-mode MRTR confirmation round.",
        structured_output=False,
    )
    async def book_ticket(destination: str, ctx: Context) -> CallToolResult | InputRequiredResult:
        if ctx.request_state is None:
            capabilities = ctx.client_capabilities
            if capabilities is None or capabilities.elicitation is None:
                raise MCPError(
                    MISSING_REQUIRED_CLIENT_CAPABILITY,
                    "Client must declare elicitation support",
                    data={"requiredCapabilities": {"elicitation": {"form": {}}}},
                )
            state = json.dumps(
                {"destination": destination, "initialRequestId": ctx.request_id},
                sort_keys=True,
                separators=(",", ":"),
            )
            return InputRequiredResult(
                input_requests={
                    "confirm_booking": ElicitRequest(
                        params=ElicitRequestFormParams(
                            message=f"Confirm booking to {destination}?",
                            requested_schema={
                                "type": "object",
                                "properties": {"confirm": {"type": "boolean"}},
                                "required": ["confirm"],
                            },
                        )
                    )
                },
                request_state=state,
            )

        state = json.loads(ctx.request_state)
        response = (ctx.input_responses or {}).get("confirm_booking")
        if not isinstance(response, ElicitResult):
            return InputRequiredResult(
                input_requests={
                    "confirm_booking": ElicitRequest(
                        params=ElicitRequestFormParams(
                            message=f"Confirm booking to {destination}?",
                            requested_schema={
                                "type": "object",
                                "properties": {"confirm": {"type": "boolean"}},
                                "required": ["confirm"],
                            },
                        )
                    )
                },
                request_state=ctx.request_state,
            )
        if response.action != "accept" or not response.content or response.content.get("confirm") is not True:
            return CallToolResult(content=[TextContent(type="text", text="booking declined")], is_error=True)
        if str(state["initialRequestId"]) == ctx.request_id:
            raise MCPError(INVALID_PARAMS, "MRTR retry must use a new JSON-RPC id")
        payload = {
            "ticket": f"TICKET-{destination.upper()}",
            "initialRequestId": str(state["initialRequestId"]),
            "retryRequestId": ctx.request_id,
        }
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(payload, sort_keys=True, separators=(",", ":")))]
        )

    def extra_tool() -> str:
        """A dynamically installed tool used only to prove catalog changes."""
        return "extra tool enabled"

    extra_enabled = False

    @server.tool(
        description="Toggle extra_tool, mutate the changing resource, and publish subscribed change events.",
        structured_output=False,
    )
    async def toggle_extra_tool(ctx: Context) -> str:
        nonlocal extra_enabled
        if extra_enabled:
            server.remove_tool("extra_tool")
        else:
            server.add_tool(
                extra_tool,
                name="extra_tool",
                description="A dynamic tool present only after toggle_extra_tool enables it.",
                structured_output=False,
            )
        extra_enabled = not extra_enabled
        resource_state.counter += 1
        await ctx.notify_tools_changed()
        await ctx.notify_resources_changed()
        await ctx.notify_resource_updated(CHANGING_URI)
        return "extra_tool enabled" if extra_enabled else "extra_tool disabled"

    @server.tool(
        description="Complete synchronously unless the client opts into the Tasks extension.",
        structured_output=False,
    )
    def long_job(seconds: float = 0.25) -> str:
        return f"long job completed synchronously after logical delay {seconds}"
