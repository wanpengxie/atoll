#!/usr/bin/env python3
"""Atoll code-mode runtime for Python: a minimal MCP client over stdio.

The host (the Go coderunner actor) is the MCP server; the declared actors'
words are its tools. This file is the second implementation of that contract
after runner.mjs — same wire, different language. No dependencies.

Program convention:  async def run(atoll, args) -> JSON value
"""
import asyncio
import importlib.util
import json
import os
import sys
import traceback

PROTOCOL_VERSION = "2025-06-18"
TOOL_CONTEXT = "atoll_context"
TOOL_RETURN = "atoll_return"
TOOL_FAIL = "atoll_fail"


class AtollError(Exception):
    def __init__(self, code, detail=""):
        super().__init__(detail or code)
        self.code = code
        self.detail = detail or ""


def _text_content(result):
    if not isinstance(result, dict):
        return None
    for block in result.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            return block.get("text")
    return None


def _unwrap(result):
    if isinstance(result, dict) and result.get("isError"):
        structured = result.get("structuredContent") or {}
        raise AtollError(structured.get("error_code", "call_failed"), structured.get("detail") or _text_content(result) or "")
    if isinstance(result, dict) and "structuredContent" in result:
        return result["structuredContent"]
    text = _text_content(result)
    if text is None:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return text


def _call_arguments(value):
    if isinstance(value, dict):
        return value
    return {"$input": value}


class _LogWriter:
    """Replaces sys.stdout / sys.stderr for the program: every line becomes
    a notifications/message, so the real stdout stays a clean protocol pipe."""

    def __init__(self, runtime, level):
        self.runtime = runtime
        self.level = level
        self.buffer = ""

    def write(self, text):
        self.buffer += str(text)
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            self.runtime.log_message(self.level, "console", line)
        return len(text)

    def flush(self):
        if self.buffer:
            self.runtime.log_message(self.level, "console", self.buffer)
            self.buffer = ""


class Runtime:
    def __init__(self):
        self.out = sys.stdout
        self.pending = {}
        self.next_id = 1
        self.tool_names = {}
        self.cancelled = asyncio.Event()
        self.progress_count = 0
        self.request_id = None

    # --- wire ---------------------------------------------------------
    def send(self, msg):
        payload = {"jsonrpc": "2.0"}
        payload.update(msg)
        self.out.write(json.dumps(payload) + "\n")
        self.out.flush()

    def request(self, method, params):
        rid = self.next_id
        self.next_id += 1
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        self.send({"id": rid, "method": method, "params": params})
        return fut

    def notify(self, method, params):
        self.send({"method": method, "params": params})

    def log_message(self, level, logger, data):
        self.notify("notifications/message", {"level": level, "logger": logger, "data": data})

    async def call_tool(self, name, arguments, meta=None):
        params = {"name": name, "arguments": arguments}
        if meta:
            params["_meta"] = meta
        return _unwrap(await self.request("tools/call", params))

    def receive(self, msg):
        if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
            return
        if "method" not in msg and "id" in msg:
            fut = self.pending.pop(msg["id"], None)
            if fut is None or fut.done():
                return
            if "error" in msg and msg["error"] is not None:
                err = msg["error"]
                data = err.get("data") or {}
                fut.set_exception(AtollError(data.get("code", "rpc_error"), data.get("detail") or err.get("message", "")))
            else:
                fut.set_result(msg.get("result"))
            return
        if "method" in msg and "id" in msg:
            if msg["method"] == "ping":
                self.send({"id": msg["id"], "result": {}})
            else:
                self.send({"id": msg["id"], "error": {"code": -32601, "message": "method not found: " + msg["method"]}})

    async def read_stdin(self):
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        while True:
            line = await reader.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                self.receive(json.loads(line))
            except ValueError:
                pass
        # The host ended the session: abort the program and every wait.
        self.cancelled.set()
        for rid, fut in list(self.pending.items()):
            self.pending.pop(rid, None)
            if not fut.done():
                fut.set_exception(AtollError("cancelled", "execution cancelled"))

    # --- program-facing object -----------------------------------------
    def make_atoll(self, context):
        runtime = self

        class Atoll:
            self_id = context.get("self")
            channel = context.get("channel")
            request_id = context.get("request_id")
            actors = dict(context.get("actors") or {})
            signal = runtime.cancelled

            async def call(self, target=None, type=None, input=None, deadline_ms=None):
                if runtime.cancelled.is_set():
                    raise AtollError("cancelled", "execution cancelled")
                name = runtime.tool_names.get((target, type))
                if not name:
                    raise AtollError("undeclared_capability", f"{target} {type} is not in requires")
                meta = {"atoll/deadline_ms": int(deadline_ms)} if deadline_ms and deadline_ms > 0 else None
                return await runtime.call_tool(name, _call_arguments(input), meta)

            async def all(self, thunks, max_concurrency=8):
                width = max(1, min(8, int(max_concurrency)))
                semaphore = asyncio.Semaphore(width)

                async def one(thunk):
                    async with semaphore:
                        return await thunk()

                return await asyncio.gather(*(one(thunk) for thunk in thunks))

            def log(self, *values):
                runtime.log_message("info", "atoll", " ".join(str(v) if isinstance(v, str) else json.dumps(v, default=str) for v in values))

            def progress(self, status, value=None):
                runtime.progress_count += 1
                runtime.notify("notifications/progress", {
                    "progressToken": runtime.request_id, "progress": runtime.progress_count,
                    "message": status, "value": value,
                })

        return Atoll()

    async def fail(self, kind, message, stack=None):
        try:
            await self.call_tool(TOOL_FAIL, {"kind": kind, "message": message, "stack": stack})
        except Exception:
            pass
        self.out.flush()
        os._exit(1)

    async def main(self):
        reader = asyncio.ensure_future(self.read_stdin())
        try:
            await self.request("initialize", {
                "protocolVersion": PROTOCOL_VERSION, "capabilities": {},
                "clientInfo": {"name": "atoll-coderunner-python", "version": "1"},
            })
            self.notify("notifications/initialized", {})
            listed = await self.request("tools/list", {})
            for tool in (listed or {}).get("tools") or []:
                meta = tool.get("_meta") or {}
                target, word = meta.get("atoll/target"), meta.get("atoll/word")
                if not target or not word:
                    continue
                self.tool_names[(target, word)] = tool["name"]
                if meta.get("atoll/actor"):
                    self.tool_names[(meta["atoll/actor"], word)] = tool["name"]
            context = await self.call_tool(TOOL_CONTEXT, {})
            self.request_id = context.get("request_id")
            atoll = self.make_atoll(context)

            # The program owns sys.stdout/sys.stderr as log streams from here.
            sys.stdout = _LogWriter(self, "info")
            sys.stderr = _LogWriter(self, "error")

            path = os.environ.get("ATOLL_PROGRAM")
            try:
                if not path:
                    raise RuntimeError("ATOLL_PROGRAM is not set")
                spec = importlib.util.spec_from_file_location("atoll_program", path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
            except Exception as error:  # noqa: BLE001 — any load failure is a syntax-class failure
                return await self.fail("syntax", str(error), traceback.format_exc())
            run = getattr(module, "run", None)
            if not callable(run):
                return await self.fail("invalid_output", "program does not export run")
            try:
                value = run(atoll, context.get("args"))
                if asyncio.iscoroutine(value):
                    value = await value
            except Exception as error:  # noqa: BLE001 — the program threw
                return await self.fail("exception", str(error), traceback.format_exc())
            try:
                encoded = json.dumps(value)
            except (TypeError, ValueError) as error:
                return await self.fail("invalid_output", "run result is not JSON serializable: " + str(error))
            sys.stdout.flush()
            sys.stderr.flush()
            await self.call_tool(TOOL_RETURN, {"value": json.loads(encoded)})
            self.out.flush()
            os._exit(0)
        finally:
            reader.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(Runtime().main())
    except SystemExit:
        raise
    except BaseException as error:  # noqa: BLE001 — last resort
        sys.__stdout__.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/message",
                                         "params": {"level": "error", "logger": "console", "data": str(error)}}) + "\n")
        sys.__stdout__.flush()
        os._exit(1)
