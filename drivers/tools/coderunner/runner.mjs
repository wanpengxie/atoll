const pending = new Map();
const controller = new AbortController();
let cancelled = false;
let nextCallID = 1;
let started = false;
let inputBuffer = "";

function send(frame) {
  process.stdout.write(JSON.stringify(frame) + "\n");
}

function textOf(values) {
  return values.map((value) => {
    if (typeof value === "string") return value;
    try {
      const encoded = JSON.stringify(value);
      return encoded === undefined ? String(value) : encoded;
    } catch {
      return String(value);
    }
  }).join(" ");
}

function log(stream, values) {
  send({ op: "log", stream, text: textOf(values) });
}

console.log = (...values) => log("stdout", values);
console.info = (...values) => log("stdout", values);
console.warn = (...values) => log("stderr", values);
console.error = (...values) => log("stderr", values);

class AtollError extends Error {
  constructor(code, detail) {
    super(detail || code);
    this.name = "AtollError";
    this.code = code;
    this.detail = detail || "";
  }
}

function answer(frame) {
  const item = pending.get(frame.id);
  if (!item) return;
  pending.delete(frame.id);
  if (frame.ok) item.resolve(frame.payload);
  else item.reject(new AtollError(frame.error?.code || "call_failed", frame.error?.detail || ""));
}

function makeAtoll(start) {
  const atoll = {
    actors: Object.freeze({ ...start.actors }),
    signal: controller.signal,
    self: start.self,
    channel: start.channel,
    requestId: start.request_id,
    call({ target, type, input, deadlineMs } = {}) {
      if (cancelled) return Promise.reject(new AtollError("cancelled", "execution cancelled"));
      const id = nextCallID++;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        send({ op: "call", id, target, type, input: input === undefined ? null : input, deadline_ms: deadlineMs });
      });
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
      log("log", values);
    },
    progress(status, value) {
      send({ op: "progress", status, value: value === undefined ? null : value });
    },
  };
  return Object.freeze(atoll);
}

function errorFrame(kind, error, fallback, exit = false) {
  const message = error instanceof Error ? error.message : String(error ?? fallback);
  const stack = error instanceof Error && error.stack ? error.stack : undefined;
  const line = JSON.stringify({ op: "error", kind, message, stack }) + "\n";
  if (exit) process.stdout.write(line, () => process.exit(1));
  else process.stdout.write(line);
}

async function execute(start) {
  const atoll = makeAtoll(start);
  let mod;
  try {
    mod = await import(start.program);
  } catch (error) {
    errorFrame("syntax", error, "program import failed", true);
    return;
  }
  if (typeof mod.run !== "function") {
    errorFrame("invalid_output", null, "program does not export run", true);
    return;
  }
  let value;
  try {
    value = await mod.run({ atoll, args: start.args });
  } catch (error) {
    errorFrame("exception", error, "program threw", true);
    return;
  }
  let encoded;
  try {
    encoded = JSON.stringify(value);
    if (encoded === undefined) throw new TypeError("run returned undefined");
  } catch (error) {
    errorFrame("invalid_output", error, "run result is not JSON serializable", true);
    return;
  }
  process.stdout.write(`{"op":"result","value":${encoded}}\n`, () => process.exit(0));
}

function receive(frame) {
  if (frame.op === "start" && !started) {
    started = true;
    void execute(frame).catch((error) => {
      errorFrame("exception", error, "runner failed", true);
    });
    return;
  }
  if (frame.op === "answer") {
    answer(frame);
    return;
  }
  if (frame.op === "cancel") {
    cancelled = true;
    controller.abort();
  }
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
    } catch (error) {
      errorFrame("invalid_output", error, "invalid runner input");
    }
  }
});

process.on("uncaughtException", (error) => {
  errorFrame("exception", error, "uncaught exception", true);
});
process.on("unhandledRejection", (error) => {
  errorFrame("exception", error, "unhandled rejection", true);
});
