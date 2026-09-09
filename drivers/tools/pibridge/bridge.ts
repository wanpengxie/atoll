import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import { fauxAssistantMessage, fauxProvider, fauxToolCall, validateToolArguments } from "@earendil-works/pi-ai";
import {
  createBashTool,
  createEditTool,
  createReadTool,
  createWriteTool,
  TODO_CONTEXT,
  withAbortSignal,
} from "@earendil-works/pi-agent-core";
import { NodeExecutionEnv } from "@earendil-works/pi-agent-core/node";
// These narrow, build-time aliases point at the pinned pi-coding-agent tool
// modules. They intentionally avoid importing its session, UI, and extension
// system; see README.md for the reproducible esbuild mapping.
import { createFindTool } from "@atoll-pi-coding/find";
import { createGrepTool } from "@atoll-pi-coding/grep";
import { createLsTool } from "@atoll-pi-coding/ls";
import { createPowerShellTool } from "@atoll-pi-coding/powershell";
import { accessSync, constants as fsConstants } from "node:fs";
import path from "node:path";
import { createInterface } from "node:readline";

const models = builtinModels();
const faux = fauxProvider();
models.setProvider(faux.provider);
const active = new Map<string, AbortController>();
const envs = new Map<string, NodeExecutionEnv>();
const harnessTools = new Map([
  ["read", createReadTool()],
  ["write", createWriteTool()],
  ["edit", createEditTool()],
  ["bash", createBashTool()],
]);

function executableAvailable(name: string): boolean {
  const extensions = process.platform === "win32"
    ? (process.env.PATHEXT || ".EXE;.CMD;.BAT;.COM").split(";")
    : [""];
  for (const dir of (process.env.PATH || "").split(path.delimiter)) {
    for (const extension of extensions) {
      try {
        accessSync(path.join(dir, name + extension), fsConstants.X_OK);
        return true;
      } catch {}
    }
  }
  return false;
}

function codingTools(cwd: string) {
  const tools = new Map([
    ["grep", createGrepTool(cwd)],
    ["find", createFindTool(cwd)],
    ["ls", createLsTool(cwd)],
  ]);
  if (executableAvailable("pwsh") || executableAvailable("powershell")) {
    tools.set("powershell", createPowerShellTool(cwd));
  }
  return tools;
}

function send(frame: unknown) {
  process.stdout.write(JSON.stringify(frame) + "\n");
}

class BridgeValidationError extends Error {}

async function errorCode(response: Response): Promise<string | undefined> {
  const reader = response.clone().body?.getReader();
  if (!reader) return undefined;
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      length += part.value.byteLength;
      if (length > 65536) { void reader.cancel(); return undefined; }
      chunks.push(part.value);
    }
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    const value = body?.error?.code ?? body?.error?.type ?? body?.code;
    return typeof value === "string" && /^[A-Za-z0-9_.-]{1,80}$/.test(value) ? value : undefined;
  } catch { return undefined; }
}

function failure(id: string, op: string, error: unknown) {
  const e = error instanceof Error ? error : new Error(String(error));
  let code = op.startsWith("workspace.") ? "tool_failed" : "provider_error";
  if (e.name === "AbortError") code = "cancelled";
  else if (e.message.startsWith("unknown or ambiguous model")) code = "model_not_found";
  else if (e.message.startsWith("Validation failed for tool")) code = "invalid_args";
  if (op === "llm.generate") {
    const info = error as any;
    const status = Number.isInteger(info?.status) ? info.status : undefined;
    if (code === "provider_error") {
      code = status === 429 ? "rate_limited" : status === 401 ? "auth" : status === 403 ? "permission" :
        status === 404 ? "model_not_found" : status === 400 || status === 422 ? "invalid_args" :
        status === 408 || status === 409 || (status && status >= 500) ? "transient_provider" : "unknown_provider_error";
      if (info?.code === "transport_error") code = "transport_error";
      if (info?.code === "cancelled") code = "cancelled";
    }
    if (error instanceof BridgeValidationError) code = "invalid_args";
    const providerCode = typeof info?.code === "string" && /^[A-Za-z0-9_.-]{1,80}$/.test(info.code) ? info.code : undefined;
    send({ id, kind: "error", code, status, provider_code: providerCode, retry_after_ms: info?.retryAfterMS,
      detail: error instanceof BridgeValidationError ? error.message : `Provider request failed (${code}); raw provider text is not exposed` });
  } else send({ id, kind: "error", code, detail: e.message });
}

function resolveModel(provider: unknown, modelId: unknown) {
  let p = typeof provider === "string" ? provider : "";
  let m = typeof modelId === "string" ? modelId : "";
  if (!p && m.includes("/")) [p, m] = [m.slice(0, m.indexOf("/")), m.slice(m.indexOf("/") + 1)];
  if (p && m) return models.getModel(p, m);
  if (m) {
    const candidates = models.getModels().filter((value) => value.id === m);
    if (candidates.length === 1) return candidates[0];
  }
  return undefined;
}

async function generate(id: string, args: any, controller: AbortController) {
  const model = resolveModel(args.provider, args.model);
  if (!model) throw new Error(`unknown or ambiguous model ${args.provider || ""}/${args.model || ""}`);
  const images = (args.messages || []).flatMap((message: any) => Array.isArray(message?.content) ? message.content : [])
    .filter((block: any) => block?.type === "image");
  if (images.length > 0 && (!model.input?.includes("image") || args.options?.faux_supports_images === false)) {
    throw new BridgeValidationError("model does not support image input");
  }
  for (const image of images) {
    if (typeof image.data !== "string" || image.data.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(image.data) ||
        Buffer.from(image.data, "base64").byteLength > 10 * 1024 * 1024 ||
        typeof image.mimeType !== "string" || !["image/png", "image/jpeg", "image/gif", "image/webp"].includes(image.mimeType)) {
      throw new BridgeValidationError("tool image is invalid, unsupported, or exceeds the 10 MiB decoded limit");
    }
  }
  if (model.provider === "faux") {
    const scriptedTool = args.options?.faux_tool_call;
    if (scriptedTool && typeof scriptedTool.name === "string") {
      faux.appendResponses([fauxAssistantMessage(
        fauxToolCall(scriptedTool.name, scriptedTool.arguments || {}, { id: scriptedTool.id }),
        { stopReason: "toolUse" },
      )]);
    } else {
      faux.appendResponses([fauxAssistantMessage(args.options?.faux_response || "faux response")]);
    }
  }
  const context = { systemPrompt: args.system_prompt || "", messages: args.messages || [], tools: args.tools || [] };
  const options: any = { ...(args.options || {}), signal: controller.signal, maxRetries: 0 };
  let failureInfo: any;
  // These pinned adapters support a per-invocation fetch. Capture HTTP facts
  // before Pi converts exceptions into a final error AssistantMessage.
  if (["anthropic-messages", "openai-completions", "openai-responses"].includes(model.api)) {
    options.fetch = async (...params: Parameters<typeof fetch>) => {
      try {
        const response = await fetch(...params);
        if (!response.ok) {
          const milliseconds = response.headers.get("retry-after-ms");
          const after = response.headers.get("retry-after");
          let delay = milliseconds ? Number(milliseconds) : after ? (Number.isFinite(Number(after)) ? Number(after) * 1000 : Date.parse(after) - Date.now()) : undefined;
          if (delay !== undefined && (!Number.isFinite(delay) || delay < 0)) delay = undefined;
          failureInfo = { status: response.status, retryAfterMS: delay === undefined ? undefined : Math.ceil(delay), code: await errorCode(response) };
        }
        return response;
      } catch (error) {
        failureInfo = { code: controller.signal.aborted ? "cancelled" : "transport_error" };
        throw error;
      }
    };
  }
  if (typeof args.api_key === "string" && args.api_key) options.apiKey = args.api_key;
  delete options.faux_response;
  delete options.faux_tool_call;
  delete options.faux_supports_images;
  const stream = models.streamSimple(model, context, options);
  send({ id, kind: "progress", event: { phase: "provider_wait" } });
  for await (const _event of stream) { /* Consume signatures and final content; never retain token event history. */ }
  const message = await stream.result();
  if (message.stopReason === "error" || message.stopReason === "aborted") {
    throw Object.assign(new Error("provider did not deliver a successful assistant"), failureInfo || {},
      message.stopReason === "aborted" ? { code: "cancelled" } : {});
  }
  send({ id, kind: "result", value: { provider: model.provider, model: model.id, message } });
}

function executionEnv(cwd: string) {
  let env = envs.get(cwd);
  if (!env) {
    env = new NodeExecutionEnv({ cwd });
    envs.set(cwd, env);
  }
  return env;
}

async function workspace(id: string, name: string, args: any, cwd: string, controller: AbortController) {
  if (name === "read") {
    if (args.offset !== undefined && (!Number.isInteger(args.offset) || args.offset < 1)) throw new Error("Validation failed for tool read: offset must be a positive integer");
    if (args.limit !== undefined && (!Number.isInteger(args.limit) || args.limit < 1)) throw new Error("Validation failed for tool read: limit must be a positive integer");
  }
  const harnessTool = harnessTools.get(name);
  let result: unknown;
  if (harnessTool) {
    const prepared = harnessTool.prepareArguments ? harnessTool.prepareArguments(args) : args;
    const validated = validateToolArguments(harnessTool, { type: "toolCall", id, name, arguments: prepared });
    const context = withAbortSignal(controller.signal, TODO_CONTEXT);
    const invocation = {
      invocationId: id,
      operationId: id,
      turnId: id,
      async getMemo(_name: string) { return undefined; },
      async setMemo(_name: string, _value: any) {},
    };
    result = await harnessTool.execute(
      id,
      validated,
      (update: unknown) => send({ id, kind: "progress", event: update }),
      { env: executionEnv(cwd) },
      invocation,
      context,
    );
  } else {
    if (name === "grep" && !executableAvailable("rg")) throw new Error("ripgrep (rg) is not available");
    if (name === "find" && !executableAvailable("fd")) throw new Error("fd is not available");
    const tool = codingTools(cwd).get(name);
    if (!tool) throw new Error(`unknown or unavailable Pi workspace tool ${name}`);
    const prepared = tool.prepareArguments ? tool.prepareArguments(args) : args;
    const validated = validateToolArguments(tool, { type: "toolCall", id, name, arguments: prepared });
    result = await tool.execute(
      id,
      validated,
      controller.signal,
      (update: unknown) => send({ id, kind: "progress", event: update }),
    );
  }
  send({ id, kind: "result", value: result });
}

async function handle(frame: any) {
  const id = typeof frame?.id === "string" ? frame.id : "";
  if (!id) return;
  if (frame.kind === "cancel") {
    active.get(id)?.abort();
    return;
  }
  if (frame.kind !== "request" || active.has(id)) return;
  const controller = new AbortController();
  active.set(id, controller);
  try {
    if (frame.op === "llm.generate") await generate(id, frame.args || {}, controller);
    else if (frame.op === "llm.models") {
      const provider = typeof frame.args?.provider === "string" ? frame.args.provider : undefined;
      const values = models.getModels(provider).map((m) => ({
        id: m.id, name: m.name, provider: m.provider, api: m.api,
        reasoning: m.reasoning, input: m.input, contextWindow: m.contextWindow, maxTokens: m.maxTokens,
      }));
      send({ id, kind: "result", value: { models: values } });
    } else if (typeof frame.op === "string" && frame.op.startsWith("workspace.")) {
      await workspace(id, frame.op.slice("workspace.".length), frame.args || {}, frame.cwd || process.cwd(), controller);
    } else throw new Error(`unsupported bridge operation ${frame.op}`);
  } catch (error) {
    failure(id, typeof frame.op === "string" ? frame.op : "", error);
  } finally {
    active.delete(id);
  }
}

const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", (line) => {
  try { void handle(JSON.parse(line)); }
  catch (error) { send({ kind: "protocol_error", detail: error instanceof Error ? error.message : String(error) }); }
});
input.on("close", () => {
  for (const controller of active.values()) controller.abort();
  void Promise.all([...envs.values()].map((env) => env.cleanup(TODO_CONTEXT))).finally(() => process.exit(0));
});
send({ kind: "ready", protocol: 1, node: process.version, pi: "0.85.1" });
