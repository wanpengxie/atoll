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
import { createInterface } from "node:readline";

const models = builtinModels();
const faux = fauxProvider();
models.setProvider(faux.provider);
const active = new Map<string, AbortController>();
const envs = new Map<string, NodeExecutionEnv>();
const tools = new Map([
  ["read", createReadTool()],
  ["write", createWriteTool()],
  ["edit", createEditTool()],
  ["bash", createBashTool()],
]);

function send(frame: unknown) {
  process.stdout.write(JSON.stringify(frame) + "\n");
}

function failure(id: string, op: string, error: unknown) {
  const e = error instanceof Error ? error : new Error(String(error));
  let code = op.startsWith("workspace.") ? "tool_failed" : "provider_error";
  if (e.name === "AbortError") code = "cancelled";
  else if (e.message.startsWith("unknown or ambiguous model")) code = "model_not_found";
  else if (e.message.startsWith("Validation failed for tool")) code = "invalid_args";
  send({ id, kind: "error", code, detail: e.message });
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
  const options = { ...(args.options || {}), signal: controller.signal };
  if (typeof args.api_key === "string" && args.api_key) options.apiKey = args.api_key;
  delete options.faux_response;
  delete options.faux_tool_call;
  const stream = models.streamSimple(model, context, options);
  const events: unknown[] = [];
  for await (const event of stream) {
    events.push(event);
    send({ id, kind: "progress", event });
  }
  const message = await stream.result();
  send({ id, kind: "result", value: { provider: model.provider, model: model.id, message, events } });
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
  const tool = tools.get(name);
  if (!tool) throw new Error(`unknown Pi workspace tool ${name}`);
  const prepared = tool.prepareArguments ? tool.prepareArguments(args) : args;
  const validated = validateToolArguments(tool, { type: "toolCall", id, name, arguments: prepared });
  const context = withAbortSignal(controller.signal, TODO_CONTEXT);
  const invocation = {
    invocationId: id,
    operationId: id,
    turnId: id,
    async getMemo(_name: string) { return undefined; },
    async setMemo(_name: string, _value: any) {},
  };
  const result = await tool.execute(
    id,
    validated,
    (update: unknown) => send({ id, kind: "progress", event: update }),
    { env: executionEnv(cwd) },
    invocation,
    context,
  );
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
