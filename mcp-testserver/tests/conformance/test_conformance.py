from __future__ import annotations

import asyncio
import importlib.metadata
import json
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any

import anyio
from mcp.client import Client
from mcp.client.extension import ClientExtension, ResultClaim
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.subscriptions import ToolsListChanged
from mcp.client.subscriptions import ResourceUpdated, ResourcesListChanged
from mcp.shared.exceptions import MCPError
from mcp.shared.message import ClientMessageMetadata
from mcp_types import (
    HEADER_MISMATCH,
    INVALID_PARAMS,
    LOG_LEVEL_META_KEY,
    UNSUPPORTED_PROTOCOL_VERSION,
    CallToolResult,
    ElicitRequestFormParams,
    ElicitResult,
    InputRequiredResult,
    TextContent,
)

from server import PROTOCOL_VERSION, SERVER_NAME
from server.resources import BINARY_URI, CHANGING_URI, TEMPLATE_URI, TEXT_URI
from server.tools_interactive import (
    TASKS_EXTENSION_ID,
    CancelTaskRequest,
    CancelTaskResult,
    CreateTaskResult,
    GetTaskParams,
    GetTaskRequest,
    GetTaskResult,
    TasksExtension,
    UpdateTaskParams,
    UpdateTaskRequest,
    UpdateTaskResult,
)

PROJECT = Path(__file__).resolve().parents[2]
BASE_TOOLS = {
    "echo",
    "add",
    "create_order",
    "set_priority",
    "search",
    "describe_shape",
    "structured_report",
    "fail_tool_error",
    "fail_protocol_error",
    "slow_task",
    "never_returns",
    "log_when_asked",
    "book_ticket",
    "toggle_extra_tool",
    "long_job",
}


class OfficialStdioTransport(AbstractAsyncContextManager):
    """Transport adapter that delegates every byte to the SDK stdio client."""

    def __init__(self, errlog: Any):
        self._context = stdio_client(
            StdioServerParameters(
                command=sys.executable,
                args=["-m", "server.main", "--transport", "stdio"],
                cwd=PROJECT,
            ),
            errlog=errlog,
        )

    async def __aenter__(self):
        return await self._context.__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        return await self._context.__aexit__(exc_type, exc, tb)


async def _unused_task_resolver(result: CreateTaskResult, context: Any) -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=result.task_id)])


class TasksClientExtension(ClientExtension):
    identifier = TASKS_EXTENSION_ID

    def claims(self):
        return (
            ResultClaim(
                result_type="task",
                model=CreateTaskResult,
                resolve=_unused_task_resolver,
                protocol_versions=frozenset({PROTOCOL_VERSION}),
            ),
        )


class ConformanceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with socket.socket() as candidate:
            candidate.bind(("127.0.0.1", 0))
            cls.port = candidate.getsockname()[1]
        cls.url = f"http://127.0.0.1:{cls.port}/mcp"
        cls.http_log = tempfile.TemporaryFile(mode="w+")
        cls.http_process = subprocess.Popen(
            [sys.executable, "-m", "server.main", "--transport", "http", "--port", str(cls.port)],
            cwd=PROJECT,
            stdout=cls.http_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if cls.http_process.poll() is not None:
                cls.http_log.seek(0)
                raise RuntimeError(f"HTTP server exited early:\n{cls.http_log.read()}")
            try:
                with socket.create_connection(("127.0.0.1", cls.port), timeout=0.1):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            raise TimeoutError("HTTP server did not start")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.http_process.terminate()
        try:
            cls.http_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.http_process.kill()
            cls.http_process.wait(timeout=5)
        cls.http_log.close()

    def http_client(self, **kwargs: Any) -> Client:
        return Client(self.url, mode="auto", cache=None, read_timeout_seconds=3, **kwargs)

    def stdio_client(self, **kwargs: Any) -> tuple[Client, Any]:
        errlog = tempfile.TemporaryFile(mode="w+")
        return (
            Client(
                OfficialStdioTransport(errlog),
                mode="auto",
                cache=None,
                read_timeout_seconds=3,
                **kwargs,
            ),
            errlog,
        )

    async def test_dod_01_dependency_is_exactly_pinned(self) -> None:
        text = (PROJECT / "pyproject.toml").read_text()
        self.assertIn('"mcp==2.0.0"', text)
        self.assertNotIn('"mcp>=', text)

    async def test_dod_02_both_transports_negotiate_2026_07_28(self) -> None:
        async with self.http_client() as http:
            self.assertEqual(http.protocol_version, PROTOCOL_VERSION)
            self.assertNotEqual(http.protocol_version, "2025-03-26")
        stdio, errlog = self.stdio_client()
        try:
            async with stdio:
                self.assertEqual(stdio.protocol_version, PROTOCOL_VERSION)
                self.assertNotEqual(stdio.protocol_version, "2025-03-26")
        finally:
            errlog.close()

    async def test_dod_03_transports_return_byte_identical_echo_results(self) -> None:
        async with self.http_client() as http:
            http_result = await http.call_tool("echo", {"text": "transport-equivalence"})
        stdio, errlog = self.stdio_client()
        try:
            async with stdio:
                stdio_result = await stdio.call_tool("echo", {"text": "transport-equivalence"})
        finally:
            errlog.close()
        dump = lambda value: value.model_dump_json(by_alias=True, exclude_none=True)
        self.assertEqual(dump(http_result), dump(stdio_result))

    async def test_dod_04_tools_list_is_stable_and_cacheable(self) -> None:
        async with self.http_client() as client:
            first = await client.session.list_tools()
            second = await client.session.list_tools()
        dump = lambda value: value.model_dump_json(by_alias=True, exclude_none=True)
        self.assertEqual(dump(first), dump(second))
        self.assertEqual([tool.name for tool in first.tools], [tool.name for tool in second.tools])
        self.assertGreaterEqual(first.ttl_ms, 0)
        self.assertIn(first.cache_scope, ("public", "private"))

    async def test_dod_05_resource_and_prompt_lists_have_cache_hints(self) -> None:
        async with self.http_client() as client:
            results = [
                await client.session.list_resources(),
                await client.session.read_resource(TEXT_URI),
                await client.session.list_resource_templates(),
                await client.session.list_prompts(),
            ]
        for result in results:
            wire = result.model_dump(by_alias=True, mode="json", exclude_none=True)
            self.assertIn("ttlMs", wire)
            self.assertIn("cacheScope", wire)

    async def test_dod_06_missing_resource_is_invalid_params(self) -> None:
        async with self.http_client() as client:
            with self.assertRaises(MCPError) as raised:
                await client.session.read_resource("test://does-not-exist")
        self.assertEqual(raised.exception.code, INVALID_PARAMS)

    async def test_dod_07_logging_requires_per_request_opt_in(self) -> None:
        opted_in: list[Any] = []

        async def collect(params):
            opted_in.append(params)

        async with self.http_client(logging_callback=collect) as client:
            await client.call_tool("log_when_asked", meta={LOG_LEVEL_META_KEY: "info"})
        self.assertEqual(len(opted_in), 1)
        self.assertEqual(opted_in[0].data, {"event": "log_when_asked"})

        not_opted_in: list[Any] = []

        async def reject_collection(params):
            not_opted_in.append(params)

        async with self.http_client(logging_callback=reject_collection) as client:
            await client.call_tool("log_when_asked")
            await anyio.sleep(0.05)
        self.assertEqual(not_opted_in, [])

    async def test_dod_08_slow_task_emits_at_least_two_progress_updates(self) -> None:
        progress: list[tuple[float, float | None, str | None]] = []

        async def collect(current, total, message):
            progress.append((current, total, message))

        async with self.http_client() as client:
            result = await client.call_tool("slow_task", {"seconds": 0.12}, progress_callback=collect)
        self.assertFalse(result.is_error)
        self.assertGreaterEqual(len(progress), 2)
        self.assertEqual(progress, sorted(progress, key=lambda item: item[0]))

    async def test_dod_09_mrtr_complete_flow_and_distinct_request_ids(self) -> None:
        async def elicitation(context, params):
            return ElicitResult(action="accept", content={"confirm": True})

        async with self.http_client(elicitation_callback=elicitation) as client:
            first = await client.session.call_tool(
                "book_ticket", {"destination": "Singapore"}, allow_input_required=True
            )
            self.assertIsInstance(first, InputRequiredResult)
            self.assertIn("confirm_booking", first.input_requests or {})
            self.assertIsNotNone(first.request_state)
            embedded = (first.input_requests or {})["confirm_booking"]
            self.assertIsInstance(embedded.params, ElicitRequestFormParams)
            final = await client.session.call_tool(
                "book_ticket",
                {"destination": "Singapore"},
                input_responses={"confirm_booking": ElicitResult(action="accept", content={"confirm": True})},
                request_state=first.request_state,
                allow_input_required=True,
            )
        self.assertIsInstance(final, CallToolResult)
        payload = json.loads(final.content[0].text)
        self.assertNotEqual(payload["initialRequestId"], payload["retryRequestId"])
        self.assertEqual(payload["ticket"], "TICKET-SINGAPORE")

    async def test_dod_10_tampered_request_state_is_rejected(self) -> None:
        async def elicitation(context, params):
            return ElicitResult(action="accept", content={"confirm": True})

        async with self.http_client(elicitation_callback=elicitation) as client:
            first = await client.session.call_tool(
                "book_ticket", {"destination": "Tokyo"}, allow_input_required=True
            )
            self.assertIsInstance(first, InputRequiredResult)
            state = first.request_state or ""
            replacement = "A" if state[-1:] != "A" else "B"
            tampered = state[:-1] + replacement
            with self.assertRaises(MCPError) as raised:
                await client.session.call_tool(
                    "book_ticket",
                    {"destination": "Tokyo"},
                    input_responses={"confirm_booking": ElicitResult(action="accept", content={"confirm": True})},
                    request_state=tampered,
                    allow_input_required=True,
                )
        self.assertEqual(raised.exception.code, INVALID_PARAMS)

    async def test_dod_11_toggle_changes_catalog_and_notifies_subscriber(self) -> None:
        async with self.http_client() as client:
            async with client.listen(tools_list_changed=True) as subscription:
                before = await client.session.list_tools()
                await client.call_tool("toggle_extra_tool")
                with anyio.fail_after(2):
                    event = await anext(subscription)
                after = await client.session.list_tools()
        self.assertIsInstance(event, ToolsListChanged)
        self.assertNotEqual([tool.name for tool in before.tools], [tool.name for tool in after.tools])

    async def test_dod_12_tasks_negative_without_capability_is_not_task(self) -> None:
        async with self.http_client() as client:
            result = await client.session.call_tool("long_job", {"seconds": 0.1}, allow_claimed=True)
        self.assertNotEqual(result.result_type, "task")
        self.assertIsInstance(result, CallToolResult)

    async def test_dod_13_http_header_body_mismatch_is_minus_32020(self) -> None:
        async with self.http_client() as client:
            original_stamp = client.session._stamp

            def mismatching_stamp(data, options):
                original_stamp(data, options)
                options.setdefault("headers", {})["mcp-method"] = "resources/list"

            client.session._stamp = mismatching_stamp
            try:
                with self.assertRaises(MCPError) as raised:
                    await client.session.list_tools()
            finally:
                client.session._stamp = original_stamp
        self.assertEqual(raised.exception.code, HEADER_MISMATCH)

    async def test_dod_14_readme_lists_every_fixture(self) -> None:
        readme = (PROJECT / "README.md").read_text()
        for name in BASE_TOOLS | {"extra_tool", TEXT_URI, BINARY_URI, CHANGING_URI, TEMPLATE_URI, "welcome", "review_topic"}:
            self.assertIn(name, readme)

    async def test_dod_15_suite_uses_official_sdk_client_2_0_0(self) -> None:
        self.assertEqual(importlib.metadata.version("mcp"), "2.0.0")
        self.assertEqual(Client.__module__, "mcp.client.client")

    async def test_protocol_surface_schemas_resources_prompts_and_errors(self) -> None:
        async with self.http_client() as client:
            discover = client.session.discover_result
            self.assertIsNotNone(discover)
            self.assertEqual(discover.meta["io.modelcontextprotocol/serverInfo"]["name"], SERVER_NAME)
            self.assertIn(TASKS_EXTENSION_ID, discover.capabilities.extensions or {})
            self.assertIsNotNone(discover.capabilities.logging)

            listing = await client.session.list_tools()
            self.assertEqual(
                listing.meta["io.modelcontextprotocol/serverInfo"]["name"], SERVER_NAME
            )
            by_name = {tool.name: tool for tool in listing.tools}
            self.assertTrue(BASE_TOOLS.issubset(by_name))
            customer = by_name["create_order"].input_schema["$defs"]["Customer"]
            self.assertEqual(customer["properties"]["name"]["type"], "string")
            self.assertEqual(by_name["set_priority"].input_schema["properties"]["level"]["enum"], ["low", "normal", "urgent"])
            self.assertEqual(by_name["search"].input_schema["properties"]["limit"]["default"], 10)
            shape_schema = by_name["describe_shape"].input_schema
            self.assertIn("$defs", shape_schema)
            self.assertIn("oneOf", shape_schema["properties"]["shape"])
            self.assertIsNotNone(by_name["structured_report"].output_schema)

            structured = await client.call_tool("structured_report", {"title": "r", "labels": ["a", "b"]})
            self.assertEqual(
                structured.meta["io.modelcontextprotocol/serverInfo"]["name"], SERVER_NAME
            )
            self.assertEqual(structured.structured_content["item_count"], 2)
            tool_error = await client.call_tool("fail_tool_error")
            self.assertTrue(tool_error.is_error)
            with self.assertRaises(MCPError) as protocol_error:
                await client.call_tool("fail_protocol_error")
            self.assertEqual(protocol_error.exception.code, INVALID_PARAMS)

            binary = await client.session.read_resource(BINARY_URI)
            self.assertEqual(binary.meta["io.modelcontextprotocol/serverInfo"]["name"], SERVER_NAME)
            self.assertTrue(binary.contents[0].blob)
            greeting = await client.session.read_resource("test://greeting/Ada")
            self.assertEqual(greeting.contents[0].text, "Hello, Ada!")
            prompts = await client.session.list_prompts()
            self.assertEqual(prompts.meta["io.modelcontextprotocol/serverInfo"]["name"], SERVER_NAME)
            self.assertEqual({prompt.name for prompt in prompts.prompts}, {"welcome", "review_topic"})
            welcome = await client.session.get_prompt("welcome")
            review = await client.session.get_prompt("review_topic", {"topic": "MCP"})
            self.assertIn("Welcome", welcome.messages[0].content.text)
            self.assertIn("MCP", review.messages[0].content.text)

            with self.assertRaises(MCPError) as unsupported:
                await client.session.send_discover("2099-01-01")
            self.assertEqual(unsupported.exception.code, UNSUPPORTED_PROTOCOL_VERSION)
            self.assertEqual(unsupported.exception.error.data["supported"], [PROTOCOL_VERSION])

    async def test_all_three_subscription_filter_families_deliver(self) -> None:
        async with self.http_client() as client:
            async with client.listen(
                tools_list_changed=True,
                resources_list_changed=True,
                resource_subscriptions=[CHANGING_URI],
            ) as subscription:
                await client.call_tool("toggle_extra_tool")
                received = []
                with anyio.fail_after(2):
                    while len(received) < 3:
                        received.append(await anext(subscription))
        self.assertEqual(
            {type(event) for event in received},
            {ToolsListChanged, ResourcesListChanged, ResourceUpdated},
        )
        updated = next(event for event in received if isinstance(event, ResourceUpdated))
        self.assertEqual(updated.uri, CHANGING_URI)

    async def test_legacy_stdio_initialize_is_rejected(self) -> None:
        errlog = tempfile.TemporaryFile(mode="w+")
        legacy = Client(
            OfficialStdioTransport(errlog),
            mode="legacy",
            cache=None,
            read_timeout_seconds=3,
        )
        try:
            with self.assertRaises(BaseExceptionGroup) as rejected:
                async with legacy:
                    pass
            pending = list(rejected.exception.exceptions)
            errors: list[MCPError] = []
            while pending:
                error = pending.pop()
                if isinstance(error, BaseExceptionGroup):
                    pending.extend(error.exceptions)
                elif isinstance(error, MCPError):
                    errors.append(error)
            self.assertEqual([error.code for error in errors], [UNSUPPORTED_PROTOCOL_VERSION])
        finally:
            errlog.close()

    async def test_tasks_positive_get_update_and_cancel(self) -> None:
        async with self.http_client(extensions=[TasksClientExtension()]) as client:
            created = await client.session.call_tool(
                "long_job", {"seconds": 2.0}, allow_claimed=True
            )
            self.assertIsInstance(created, CreateTaskResult)
            current = await client.session.send_request(
                GetTaskRequest(params=GetTaskParams(task_id=created.task_id)), GetTaskResult
            )
            self.assertEqual(current.status, "working")
            updated = await client.session.send_request(
                UpdateTaskRequest(params=UpdateTaskParams(task_id=created.task_id, input_responses={})),
                UpdateTaskResult,
            )
            self.assertEqual(updated.result_type, "complete")
            cancelled = await client.session.send_request(
                CancelTaskRequest(params=GetTaskParams(task_id=created.task_id)), CancelTaskResult
            )
            self.assertEqual(cancelled.result_type, "complete")
            final = await client.session.send_request(
                GetTaskRequest(params=GetTaskParams(task_id=created.task_id)), GetTaskResult
            )
            self.assertEqual(final.status, "cancelled")

    async def test_task_ids_expire_across_server_restart(self) -> None:
        first, first_log = self.stdio_client(extensions=[TasksClientExtension()])
        try:
            async with first:
                created = await first.session.call_tool(
                    "long_job", {"seconds": 2.0}, allow_claimed=True
                )
                self.assertIsInstance(created, CreateTaskResult)
                task_id = created.task_id
        finally:
            first_log.close()

        restarted, restarted_log = self.stdio_client(extensions=[TasksClientExtension()])
        try:
            async with restarted:
                with self.assertRaises(MCPError) as missing:
                    await restarted.session.send_request(
                        GetTaskRequest(params=GetTaskParams(task_id=task_id)), GetTaskResult
                    )
                self.assertEqual(missing.exception.code, INVALID_PARAMS)
        finally:
            restarted_log.close()

    async def test_never_returns_times_out_and_connection_remains_usable(self) -> None:
        async with self.http_client() as client:
            with self.assertRaises(MCPError):
                await client.call_tool("never_returns", read_timeout_seconds=0.15)
            echoed = await client.call_tool("echo", {"text": "still alive"})
            self.assertEqual(echoed.content[0].text, "still alive")


if __name__ == "__main__":
    unittest.main()
