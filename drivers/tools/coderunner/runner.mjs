// Atoll code-mode runtime for Node: an MCP client over stdio. The host (the Go
// coderunner actor) is the MCP server; the declared actors' words are its
// tools. This file is one implementation of that contract — any MCP client in
// any language can stand here.
import { pathToFileURL } from "node:url";

const PROTOCOL_VERSION = "2025-06-18";
const TOOL_CONTEXT = "atoll_context";
const TOOL_RETURN = "atoll_return";
const TOOL_FAIL = "atoll_fail";

const pending = new Map();
const controller = new AbortController();
let cancelled = false;
let nextID = 1;
let inputBuffer = "";
let progressCount = 0;
let requestID = null;
// (target or actor id) + " " + word → tool name, from tools/list _meta.
const toolNames = new Map();

function send(msg) {
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", ...msg }) + "\n");
}

function request(method, params) {
  const id = nextID++;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    send({ id, method, params });
  });
}

function notify(method, params) {
  send({ method, params });
}

function textOf(values) {
  return values
    .map((value) => {
      if (typeof value === "string") return value;
      try {
        const encoded = JSON.stringify(value);
        return encoded === undefined ? String(value) : encoded;
      } catch {
        return String(value);
      }
    })
    .join(" ");
}

function logMessage(level, logger, values) {
  notify("notifications/message", { level, logger, data: textOf(values) });
}

console.log = (...values) => logMessage("info", "console", values);
console.info = (...values) => logMessage("info", "console", values);
console.debug = (...values) => logMessage("debug", "console", values);
console.warn = (...values) => logMessage("warning", "console", values);
console.error = (...values) => logMessage("error", "console", values);

class AtollError extends Error {
  constructor(code, detail) {
    super(detail || code);
    this.name = "AtollError";
    this.code = code;
    this.detail = detail || "";
  }
}

function callArguments(input) {
  if (input !== null && typeof input === "object" && !Array.isArray(input)) return input;
  return { $input: input === undefined ? null : input };
}

function textContent(result) {
  if (!result || !Array.isArray(result.content)) return undefined;
  const block = result.content.find((c) => c && c.type === "text");
  return block ? block.text : undefined;
}

function unwrapResult(result) {
  if (result && result.isError) {
    const structured = result.structuredContent || {};
    throw new AtollError(structured.error_code || "call_failed", structured.detail || textContent(result) || "");
  }
  if (result && result.structuredContent !== undefined) return result.structuredContent;
  const text = textContent(result);
  if (text === undefined) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function callTool(name, args, meta) {
  const params = { name, arguments: args };
  if (meta) params._meta = meta;
  return unwrapResult(await request("tools/call", params));
}

function makeAtoll(context) {
  const atoll = {
    actors: Object.freeze({ ...(context.actors || {}) }),
    signal: controller.signal,
    self: context.self,
    channel: context.channel,
    requestId: context.request_id,
    call({ target, type, input, deadlineMs } = {}) {
      if (cancelled) return Promise.reject(new AtollError("cancelled", "execution cancelled"));
      const name = toolNames.get(`${target} ${type}`);
      if (!name) return Promise.reject(new AtollError("undeclared_capability", `${target} ${type} is not in requires`));
      const meta = Number.isFinite(deadlineMs) && deadlineMs > 0 ? { "atoll/deadline_ms": Math.floor(deadlineMs) } : undefined;
      return callTool(name, callArguments(input), meta);
    },
    async all(thunks, options = {}) {
      if (!Array.isArray(thunks)) throw new TypeError("atoll.all expects an array of thunks");
      const requested = Number(options.maxConcurrency);
      const width = Math.max(1, Math.min(8, Number.isFinite(requested) ? Math.floor(requested) : 8));
      const values = new Array(thunks.length);
      let cursor = 0;
      async function worker() {
        while (true) {
          const index = cursor++;
          if (index >= thunks.length) return;
          values[index] = await thunks[index]();
        }
      }
      await Promise.all(Array.from({ length: Math.min(width, thunks.length) }, worker));
      return values;
    },
    log(...values) {
      logMessage("info", "atoll", values);
    },
    progress(status, value) {
      progressCount += 1;
      notify("notifications/progress", {
        progressToken: requestID,
        progress: progressCount,
        message: status,
        value: value === undefined ? null : value,
      });
    },
  };
  return Object.freeze(atoll);
}

async function fail(kind, error, fallback) {
  const message = error instanceof Error ? error.message : String(error ?? fallback);
  const stack = error instanceof Error && error.stack ? error.stack : undefined;
  try {
    await callTool(TOOL_FAIL, { kind, message, stack });
  } catch {
    // The host is gone or already finished; exiting is all that is left.
  }
  process.exit(1);
}

async function main() {
  await request("initialize", {
    protocolVersion: PROTOCOL_VERSION,
    capabilities: {},
    clientInfo: { name: "atoll-coderunner-node", version: "1" },
  });
  notify("notifications/initialized", {});
  const listed = await request("tools/list", {});
  for (const tool of listed.tools || []) {
    const meta = tool._meta || {};
    const target = meta["atoll/target"];
    const word = meta["atoll/word"];
    if (!target || !word) continue;
    toolNames.set(`${target} ${word}`, tool.name);
    if (meta["atoll/actor"]) toolNames.set(`${meta["atoll/actor"]} ${word}`, tool.name);
  }
  const context = await callTool(TOOL_CONTEXT, {});
  requestID = context.request_id;
  const atoll = makeAtoll(context);

  const programPath = process.env.ATOLL_PROGRAM;
  let mod;
  try {
    if (!programPath) throw new Error("ATOLL_PROGRAM is not set");
    mod = await import(pathToFileURL(programPath).href);
  } catch (error) {
    return fail("syntax", error, "program import failed");
  }
  if (typeof mod.run !== "function") {
    return fail("invalid_output", null, "program does not export run");
  }
  let value;
  try {
    value = await mod.run({ atoll, args: context.args });
  } catch (error) {
    return fail("exception", error, "program threw");
  }
  let encoded;
  try {
    encoded = JSON.stringify(value);
    if (encoded === undefined) throw new TypeError("run returned undefined");
  } catch (error) {
    return fail("invalid_output", error, "run result is not JSON serializable");
  }
  await callTool(TOOL_RETURN, { value: JSON.parse(encoded) });
  process.exit(0);
}

function receive(msg) {
  if (!msg || msg.jsonrpc !== "2.0") return;
  if (msg.method === undefined && msg.id !== undefined) {
    const item = pending.get(msg.id);
    if (!item) return;
    pending.delete(msg.id);
    if (msg.error) {
      const data = msg.error.data || {};
      item.reject(new AtollError(data.code || "rpc_error", data.detail || msg.error.message || ""));
    } else {
      item.resolve(msg.result);
    }
    return;
  }
  if (msg.method !== undefined && msg.id !== undefined) {
    if (msg.method === "ping") send({ id: msg.id, result: {} });
    else send({ id: msg.id, error: { code: -32601, message: `method not found: ${msg.method}` } });
  }
  // Notifications from the host: none defined; ignore.
}

process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  inputBuffer += chunk;
  while (true) {
    const newline = inputBuffer.indexOf("\n");
    if (newline < 0) return;
    const line = inputBuffer.slice(0, newline);
    inputBuffer = inputBuffer.slice(newline + 1);
    if (!line.trim()) continue;
    try {
      receive(JSON.parse(line));
    } catch {
      // A malformed host line is not the program's fault; drop it.
    }
  }
});
// The host ends the session by closing our stdin: abort the program.
process.stdin.on("end", () => {
  cancelled = true;
  controller.abort();
  for (const [id, item] of pending) {
    pending.delete(id);
    item.reject(new AtollError("cancelled", "execution cancelled"));
  }
});

process.on("uncaughtException", (error) => {
  void fail("exception", error, "uncaught exception");
});
process.on("unhandledRejection", (error) => {
  void fail("exception", error, "unhandled rejection");
});

void main().catch((error) => fail("exception", error, "runtime failed"));
