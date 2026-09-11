import { builtinModels } from "@earendil-works/pi-ai/providers/all";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
  isContextOverflow,
  isRecoverableLength,
  isRetryableAssistantError,
  validateToolArguments,
} from "@earendil-works/pi-ai";
import { calculateContextTokens } from "@earendil-works/pi-ai/utils/estimate";
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
const harnessTools = new Map([
  ["read", createReadTool()],
  ["write", createWriteTool()],
  ["edit", createEditTool()],
]);

function configuredHarnessTool(name: string, shellEnv: Record<string, string>) {
  if (name !== "bash") return harnessTools.get(name);
  return createBashTool({
    prepare(execution) {
      // The Looper supplied a complete branch snapshot. Do not merge the
      // Bridge process environment back in: that would make unset ineffective
      // and create a second source of branch execution truth.
      execution.env = { ...shellEnv };
      execution.inheritEnv = false;
    },
  });
}

function environmentValue(shellEnv: Record<string, string>, name: string): string | undefined {
  if (process.platform !== "win32") return shellEnv[name];
  const found = Object.keys(shellEnv).find((key) => key.toUpperCase() === name.toUpperCase());
  return found === undefined ? undefined : shellEnv[found];
}

function normalizeEnvironmentNames(shellEnv: Record<string, string>): Record<string, string> {
  if (process.platform !== "win32") return shellEnv;
  const seen = new Set<string>();
  for (const key of Object.keys(shellEnv)) {
    const folded = key.toUpperCase();
    if (seen.has(folded)) throw new BridgeValidationError(`duplicate Windows environment name ${key}`);
    seen.add(folded);
  }
  return shellEnv;
}

function executableAvailable(name: string, shellEnv: Record<string, string> = {}): boolean {
  const extensions = process.platform === "win32"
    ? (environmentValue(shellEnv, "PATHEXT") || ".EXE;.CMD;.BAT;.COM").split(";")
    : [""];
  for (const dir of (environmentValue(shellEnv, "PATH") || "").split(path.delimiter)) {
    for (const extension of extensions) {
      try {
        accessSync(path.join(dir, name + extension), fsConstants.X_OK);
        return true;
      } catch {}
    }
  }
  return false;
}

// The pinned coding-agent grep/find implementations resolve helper binaries
// through process.env and do not accept an env option. A Workspace Bridge
// belongs to one branch, so serialize its calls and project the Holder's full
// snapshot into that compatibility seam only for the duration of one call.
// Restoring it afterwards keeps the Bridge from becoming environment state.
let workspaceTail: Promise<void> = Promise.resolve();
async function withWorkspaceEnvironment<T>(shellEnv: Record<string, string>, run: () => Promise<T>): Promise<T> {
  const previous = workspaceTail;
  let release!: () => void;
  workspaceTail = new Promise<void>((resolve) => { release = resolve; });
  await previous;
  const saved = { ...process.env };
  try {
    for (const key of Object.keys(process.env)) delete process.env[key];
    for (const [key, value] of Object.entries(shellEnv)) process.env[key] = value;
    return await run();
  } finally {
    for (const key of Object.keys(process.env)) delete process.env[key];
    for (const [key, value] of Object.entries(saved)) {
      if (value !== undefined) process.env[key] = value;
    }
    release();
  }
}

function codingTools(cwd: string, shellEnv: Record<string, string>) {
  const tools = new Map([
    ["grep", createGrepTool(cwd)],
    ["find", createFindTool(cwd)],
    ["ls", createLsTool(cwd)],
  ]);
  if (executableAvailable("pwsh", shellEnv) || executableAvailable("powershell", shellEnv)) {
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
	const usage = {
		model: model.id,
		provider: model.provider,
		effort: typeof args.options?.thinkingLevel === "string" ? args.options.thinkingLevel : undefined,
		input: message.usage?.input || 0,
		output: message.usage?.output || 0,
		cache_read: message.usage?.cacheRead || 0,
		cache_write: message.usage?.cacheWrite || 0,
		total: message.usage ? calculateContextTokens(message.usage) : 0,
		context_tokens: (message.usage?.input || 0) + (message.usage?.cacheRead || 0) + (message.usage?.cacheWrite || 0),
		context_window: model.contextWindow || 0,
		cost: {
			input: message.usage?.cost?.input || 0,
			output: message.usage?.cost?.output || 0,
			cache_read: message.usage?.cost?.cacheRead || 0,
			cache_write: message.usage?.cost?.cacheWrite || 0,
			total: message.usage?.cost?.total || 0,
		},
	};
	const overflow = isContextOverflow(message, model.contextWindow);
	const recoverableLength = isRecoverableLength(message, model.maxTokens);
  if (message.stopReason === "error" || message.stopReason === "aborted" || overflow || recoverableLength) {
	const code = message.stopReason === "aborted" ? "cancelled" : overflow ? "context_overflow" :
		recoverableLength ? "length_recoverable" :
		failureInfo?.status === 429 ? "rate_limited" : failureInfo?.status === 401 ? "auth" : failureInfo?.status === 403 ? "permission" :
		failureInfo?.status === 404 ? "model_not_found" : failureInfo?.status === 400 || failureInfo?.status === 422 ? "invalid_args" :
		failureInfo?.status === 408 || failureInfo?.status === 409 || failureInfo?.status >= 500 ? "transient_provider" :
		failureInfo?.code === "transport_error" ? "transport_error" : "unknown_provider_error";
	const retryable = overflow || recoverableLength || isRetryableAssistantError(message) || failureInfo?.status === 408 || failureInfo?.status === 409 || failureInfo?.status === 429 || failureInfo?.status >= 500;
	const safeMessage = { ...message, content: code === "context_overflow" ? message.content : [], errorMessage: `Provider request failed (${code})` };
	send({ id, kind: "result", value: { provider: model.provider, model: model.id, message: safeMessage, usage, error_code: code, retryable,
		provider_status: failureInfo?.status, provider_code: failureInfo?.code, retry_after_ms: failureInfo?.retryAfterMS } });
	return;
  }
  send({ id, kind: "result", value: { provider: model.provider, model: model.id, message, usage } });
}

async function workspace(id: string, name: string, args: any, cwd: string, shellEnv: Record<string, string>, controller: AbortController) {
  if (name === "read") {
    if (args.offset !== undefined && (!Number.isInteger(args.offset) || args.offset < 1)) throw new Error("Validation failed for tool read: offset must be a positive integer");
    if (args.limit !== undefined && (!Number.isInteger(args.limit) || args.limit < 1)) throw new Error("Validation failed for tool read: limit must be a positive integer");
  }
  const harnessTool = configuredHarnessTool(name, shellEnv);
	let result: unknown;
	if (harnessTool) {
		const env = new NodeExecutionEnv({ cwd, shellEnv });
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
		try {
			result = await harnessTool.execute(
				id,
				validated,
				(update: unknown) => send({ id, kind: "progress", event: update }),
				{ env },
				invocation,
				context,
			);
		} finally {
			await env.cleanup(TODO_CONTEXT);
		}
  } else {
    if (name === "grep" && !executableAvailable("rg", shellEnv)) throw new Error("ripgrep (rg) is not available");
    if (name === "find" && !executableAvailable("fd", shellEnv)) throw new Error("fd is not available");
    const tool = codingTools(cwd, shellEnv).get(name);
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
		thinking_levels: Object.entries(m.thinkingLevelMap || {}).filter(([, value]) => value !== null).map(([level]) => level),
      }));
      send({ id, kind: "result", value: { models: values } });
    } else if (typeof frame.op === "string" && frame.op.startsWith("workspace.")) {
      const shellEnv: Record<string, string> = {};
      if (frame.env && typeof frame.env === "object" && !Array.isArray(frame.env)) {
        for (const [key, value] of Object.entries(frame.env)) {
          if (typeof value !== "string") throw new BridgeValidationError("workspace env values must be strings");
          shellEnv[key] = value;
        }
      }
      const normalizedEnv = normalizeEnvironmentNames(shellEnv);
      await withWorkspaceEnvironment(normalizedEnv, () => workspace(id, frame.op.slice("workspace.".length), frame.args || {}, frame.cwd || process.cwd(), normalizedEnv, controller));
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
  process.exit(0);
});
send({ kind: "ready", protocol: 1, node: process.version, pi: "0.85.1" });
