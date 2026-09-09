// Offline contract fixtures for the pinned Pi converters. onPayload throws
// before networking; these tests never need real credentials or paid services.
import assert from "node:assert/strict";
import { test } from "node:test";
import { streamSimple } from "../../../../pi-mono/packages/ai/dist/compat.js";

const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
function model(api, compat = {}) {
  return { api, provider: api === "openai-responses" ? "openai" : "fixture", id: api.startsWith("google") ? "gemini-3-pro" : "fixture-model",
    name: "fixture", baseUrl: "http://127.0.0.1:9", reasoning: true, input: ["text", "image"],
    cost: usage.cost, contextWindow: 128000, maxTokens: 1024, compat };
}
function assistant(m, content) { return { role: "assistant", provider: m.provider, api: m.api, model: m.id,
  content, usage, stopReason: "toolUse", timestamp: 1 }; }
const tool = (id = "call_1") => ({ type: "toolCall", id, name: "read", arguments: {} });
const result = (id = "call_1") => ({ role: "toolResult", toolCallId: id, toolName: "read", isError: false,
  content: [{ type: "text", text: "ok" }], timestamp: 2 });
async function capture(m, messages) {
  let payload;
  const original = JSON.stringify(messages);
  const stream = streamSimple(m, { messages }, { apiKey: "fixture-not-a-real-key", maxRetries: 0,
    onPayload(value) { payload = structuredClone(value); throw new Error("offline payload capture"); } });
  const final = await stream.result();
  assert.equal(JSON.stringify(messages), original, "converter mutated retained source history");
  assert.ok(payload, `payload was not captured: ${final.errorMessage}`);
  return payload;
}

test("F1 Anthropic signed/empty/redacted thinking and multi-tool aggregation", async () => {
  const m = model("anthropic-messages");
  const p = await capture(m, [assistant(m, [
    { type: "thinking", thinking: "", thinkingSignature: "signed" },
    { type: "thinking", thinking: "", thinkingSignature: "opaque", redacted: true }, tool("a"), tool("b")]), result("a"), result("b")]);
  const content = p.messages.find(x => x.role === "assistant").content;
  assert.ok(content.some(x => x.type === "thinking" && x.signature === "signed" && x.thinking === ""));
  assert.ok(content.some(x => x.type === "redacted_thinking" && x.data === "opaque"));
  assert.equal(p.messages.at(-1).content.filter(x => x.type === "tool_result").length, 2);
});
test("F1 Anthropic empty signature default versus explicit compat", async () => {
  for (const allow of [false, true]) {
    const m = model("anthropic-messages", { allowEmptySignature: allow });
    const p = await capture(m, [assistant(m, [{ type: "thinking", thinking: "reasoning", thinkingSignature: "" }])]);
    const b = p.messages.find(x => x.role === "assistant").content[0];
    assert.equal(b.type, allow ? "thinking" : "text");
  }
});
test("F2 Responses encrypted reasoning and call/item identity", async () => {
  const m = model("openai-responses");
  const item = { type: "reasoning", id: "rs_one", encrypted_content: "opaque", summary: [] };
  const p = await capture(m, [assistant(m, [{ type: "thinking", thinking: "", thinkingSignature: JSON.stringify(item) }, tool("call_1|fc_one")]), result("call_1|fc_one")]);
  assert.deepEqual(p.input.find(x => x.type === "reasoning"), item);
  const call = p.input.find(x => x.type === "function_call");
  const output = p.input.find(x => x.type === "function_call_output");
  assert.equal(call.call_id, output.call_id); assert.equal(call.id, "fc_one");
  const foreign = assistant({ ...m, id: "another-model" }, [tool("call_1|fc_one")]);
  const changed = await capture(m, [foreign, result("call_1|fc_one")]);
  assert.equal(changed.input.find(x => x.type === "function_call").id, undefined);
});
test("F3 Completions reasoning fields and thinking-as-text remain converter decisions", async () => {
  for (const asText of [false, true]) {
    const m = model("openai-completions", { requiresThinkingAsText: asText, requiresReasoningContentOnAssistantMessages: true });
    const p = await capture(m, [assistant(m, [{ type: "thinking", thinking: "reasoning", thinkingSignature: "reasoning_content" }, tool()]), result()]);
    const a = p.messages.find(x => x.role === "assistant");
    if (asText) assert.ok(JSON.stringify(a.content).includes("reasoning"));
    else assert.equal(a.reasoning_content, "reasoning");
    assert.equal(a.tool_calls[0].id, p.messages.find(x => x.role === "tool").tool_call_id);
  }
});
test("F4 Gemini part signatures and parallel tool responses", async () => {
  for (const api of ["google-generative-ai", "google-vertex"]) {
  const m = model(api);
  const signature = Buffer.from("opaque signature").toString("base64");
  const p = await capture(m, [assistant(m, [{ type: "text", text: "", textSignature: signature },
    { type: "thinking", thinking: "", thinkingSignature: signature }, { ...tool("a"), thoughtSignature: signature }, tool("b")]), result("a"), result("b")]);
  const parts = p.contents.find(x => x.role === "model").parts;
  assert.equal(parts.filter(x => x.thoughtSignature === signature).length, 3);
  assert.equal(p.contents.at(-1).parts.filter(x => x.functionResponse).length, 2);
  const changed = await capture(m, [assistant({ ...m, id: "old-gemini-model" }, [{ type: "thinking", thinking: "visible-reason", thinkingSignature: signature }, { ...tool(), thoughtSignature: signature }]), result()]);
  assert.ok(!JSON.stringify(changed).includes(signature));
  }
});
test("F5 cross-provider redacted block does not become plaintext; IDs still pair", async () => {
  const apis = ["anthropic-messages", "openai-completions", "openai-responses", "google-generative-ai"];
  for (const sourceAPI of apis) {
  const source = model(sourceAPI);
  for (const api of apis.filter(x => x !== sourceAPI)) {
    const m = model(api);
    const p = await capture(m, [assistant(source, [{ type: "thinking", thinking: "hidden-redacted-text", thinkingSignature: "opaque", redacted: true }, tool("foreign|id")]), result("foreign|id")]);
    assert.ok(!JSON.stringify(p).includes("hidden-redacted-text"));
    assert.ok(!JSON.stringify(p).includes("opaque"));
    if (api === "openai-responses") assert.equal(p.input.find(x => x.type === "function_call").call_id, p.input.find(x => x.type === "function_call_output").call_id);
    if (api === "openai-completions") assert.equal(p.messages.find(x => x.role === "assistant").tool_calls[0].id, p.messages.find(x => x.role === "tool").tool_call_id);
    if (api === "google-generative-ai") assert.equal(p.contents.find(x => x.role === "model").parts.find(x => x.functionCall).functionCall.name, p.contents.at(-1).parts[0].functionResponse.name);
    if (api === "anthropic-messages") assert.equal(p.messages.find(x => x.role === "assistant").content.find(x => x.type === "tool_use").id, p.messages.at(-1).content.find(x => x.type === "tool_result").tool_use_id);
  }
  }
});
test("F2 malformed Responses signature fails before sending a request", async () => {
  const m = model("openai-responses");
  let payloads = 0;
  const stream = streamSimple(m, { messages: [assistant(m, [{ type: "thinking", thinking: "x", thinkingSignature: "invalid-json" }])] },
    { apiKey: "fixture", maxRetries: 0, onPayload() { payloads++; throw new Error("offline"); } });
  const result = await stream.result();
  assert.equal(result.stopReason, "error"); assert.equal(payloads, 0);
});
test("R1 pinned Anthropic/OpenAI SDKs issue one HTTP request when maxRetries is zero", async () => {
  for (const api of ["anthropic-messages", "openai-completions", "openai-responses"]) {
    for (const status of [400, 401, 429, 503]) {
      const m = model(api);
      let calls = 0;
      const stream = streamSimple(m, { messages: [{ role: "user", content: "fixture", timestamp: 1 }] }, {
        apiKey: "fixture", maxRetries: 0,
        fetch: async () => { calls++; return new Response(JSON.stringify({ error: { type: "fixture_error", message: "fixture" } }),
          { status, headers: { "content-type": "application/json", "retry-after": "0" } }); },
      });
      const final = await stream.result();
      assert.equal(final.stopReason, "error");
      assert.equal(calls, 1, `${api} ${status} multiplied the attempt count`);
    }
  }
});
test("F3 Completions encrypted reasoning_details remains opaque", async () => {
  const m = model("openai-completions");
  const details = [{ type: "reasoning.encrypted", id: "opaque-id", data: "opaque-data" }];
  const p = await capture(m, [assistant(m, [{ type: "thinking", thinking: "", thinkingSignature: JSON.stringify(details) }, tool()]), result()]);
  assert.deepEqual(p.messages.find(x => x.role === "assistant").reasoning_details, details);
});
