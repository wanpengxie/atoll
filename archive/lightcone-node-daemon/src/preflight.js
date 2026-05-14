import { spawn } from 'child_process';

// Actually spawn the target CLI with the chosen model. We watch for invalid-model
// / auth / unknown-model style errors in stderr+stdout and abort early, otherwise
// wait up to PROBE_MS for the process to exit on its own.
const PROBE_MS = 15000;

// Patterns that mean "this config is definitively broken" — abort fast without
// waiting for the full timeout.
const FATAL_PATTERNS = [
  /not supported/i,
  /unknown model/i,
  /invalid[_ -]?model/i,
  /model.*not found/i,
  /invalid[_ -]?request/i,
  /unauthorized|unauthenticated|not authenticated/i,
  /LLM is not set/i,
  /no such model/i,
];

const PROBES = {
  // `claude --print` runs a one-shot completion, invalid model returns non-zero
  // with the provider's error text.
  claude: (model) => ({ args: ['--model', model, '--print', 'ping'] }),

  // `codex exec -m MODEL "ping"` — same idea.
  codex: (model) => ({ args: ['exec', '-m', model, 'ping'] }),

  // `kimi --model MODEL info` exits fast and validates model parsing.
  kimi: (model) => ({ args: ['--model', model, 'info'] }),
};

export async function preflight({ runtime, model }) {
  if (!model) return { ok: true };                 // no model → CLI default
  const probe = PROBES[runtime];
  if (!probe) return { ok: true };                 // unknown runtime → skip

  const { args } = probe(model);
  const env = { ...process.env, FORCE_COLOR: '0', NO_COLOR: '1' };

  return new Promise((resolve) => {
    let stderr = '';
    let stdout = '';
    let settled = false;
    let proc;

    const done = (result) => {
      if (settled) return;
      settled = true;
      try { proc?.kill('SIGTERM'); } catch {}
      setTimeout(() => { try { proc?.kill('SIGKILL'); } catch {} }, 500);
      resolve(result);
    };

    const scanFatal = () => {
      const combined = stderr + '\n' + stdout;
      for (const re of FATAL_PATTERNS) {
        if (re.test(combined)) {
          done({ ok: false, error: combined.trim().slice(0, 800) });
          return true;
        }
      }
      return false;
    };

    try {
      proc = spawn(runtime, args, { env, stdio: ['pipe', 'pipe', 'pipe'] });
    } catch (e) {
      return resolve({ ok: false, error: `spawn failed: ${e.message}` });
    }

    proc.stderr.on('data', (d) => { stderr += d.toString(); scanFatal(); });
    proc.stdout.on('data', (d) => { stdout += d.toString(); scanFatal(); });
    proc.on('error', (e) => done({ ok: false, error: `spawn error: ${e.message}` }));
    proc.on('exit', (code) => {
      if (code === 0) return done({ ok: true });
      const text = (stderr + '\n' + stdout).trim().slice(0, 800);
      done({ ok: false, error: text || `${runtime} exited with code ${code}` });
    });

    try { proc.stdin.end(''); } catch {}
    setTimeout(() => done({ ok: true }), PROBE_MS);
  });
}
