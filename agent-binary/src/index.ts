import { parseEnv } from './bootstrap/env.js';
import { ensureSessionState } from './bootstrap/session-resume.js';
import { initializeWorkspace } from './bootstrap/workspace-init.js';
import { runPostTurn } from './hooks/post-turn.js';
import { appendTrace } from './hooks/trace.js';
import { startStdinReader } from './ipc/stdin-reader.js';
import { setStdoutContext, writeActivity, writeSession, writeStatus } from './ipc/stdout-writer.js';
import { buildSystemPrompt, buildUserTurn } from './prompt/system-prompt.js';
import { runAgentTurn } from './sdk/agent.js';
import { emitAgentProgress } from './sdk/daemon-rpc.js';
import type { AgentEvent, StdinEnvelope } from './types/ipc.js';
import { logError, logInfo } from './util/logger.js';
import { CliError } from './util/simple-error.js';
import { nowIso } from './util/time.js';

function normalizeEvent(event: AgentEvent): AgentEvent {
  return {
    ...event,
    createdAt: event.createdAt ?? event.created_at ?? nowIso(),
  };
}

async function main(): Promise<void> {
  const env = parseEnv();
  initializeWorkspace(env);
  const session = ensureSessionState(env);
  const systemPrompt = buildSystemPrompt(env);

  setStdoutContext({ channelId: env.channelId, agentPid: process.pid, sessionId: session.sessionId });
  writeSession(session.sessionId);
  writeStatus('ready', session.existed ? 'session_restored' : 'session_initialized');
  appendTrace(env.workdir, env.agentName, session.sessionId, {
    kind: 'agent.boot',
    channel_id: env.channelId,
    session_id: session.sessionId,
    restored: session.existed,
  });

  const queue: AgentEvent[] = [];
  let processing = false;

  const processNext = async () => {
    if (processing) return;
    const next = queue.shift();
    if (!next) return;

    processing = true;
    writeActivity('processing', next.type, { correlation_id: next.requestId ?? next.id });
    appendTrace(env.workdir, env.agentName, session.sessionId, {
      kind: 'event.received',
      event: next,
    });

    try {
      const turn = await runAgentTurn({
        cwd: env.workdir,
        sessionId: session.sessionId,
        systemPrompt,
        userPrompt: buildUserTurn(next, env),
        onEvent: (event) => {
          appendTrace(env.workdir, env.agentName, session.sessionId, {
            kind: 'claude.event',
            event,
          });

          if (event.type === 'assistant') {
            const message = (event as { message?: { content?: unknown } }).message;
            const blocks = Array.isArray(message?.content) ? message.content : [];
            for (const block of blocks) {
              if (
                block
                && typeof block === 'object'
                && (block as { type?: unknown }).type === 'text'
                && typeof (block as { text?: unknown }).text === 'string'
              ) {
                const text = String((block as { text: string }).text).trim();
                if (!text) continue;
                emitAgentProgress(env, text).catch((err: unknown) => {
                  appendTrace(env.workdir, env.agentName, session.sessionId, {
                    kind: 'agent.progress.emit_failed',
                    message: err instanceof Error ? err.message : String(err),
                  });
                });
              }
            }
          }
        },
        onStderr: (line) => {
          appendTrace(env.workdir, env.agentName, session.sessionId, {
            kind: 'claude.stderr',
            line,
          });
        },
      });

      const postTurn = runPostTurn(
        env.workdir,
        env.agentName,
        [
          `# Current State`,
          `- last_event: ${next.type}`,
          `- last_mode: ${turn.mode}`,
          `- session_id: ${session.sessionId}`,
          `- updated_at: ${nowIso()}`,
          turn.result ? `- last_result: ${turn.result}` : `- last_result: <empty>`,
        ].join('\n'),
      );

      appendTrace(env.workdir, env.agentName, session.sessionId, {
        kind: 'turn.completed',
        event_type: next.type,
        mode: turn.mode,
        result: turn.result,
        archived_artifacts: postTurn.archivedArtifacts,
      });
      writeActivity('turn.completed', '', { event_type: next.type });
    } catch (error) {
      const cliError = error instanceof CliError
        ? error
        : new CliError('agent_turn_failed', error instanceof Error ? error.message : 'Unknown agent turn failure');
      writeStatus('error', `${cliError.code}: ${cliError.message}`);
      writeActivity('turn.failed', cliError.code, { event_type: next.type, message: cliError.message });
      appendTrace(env.workdir, env.agentName, session.sessionId, {
        kind: 'turn.failed',
        event_type: next.type,
        code: cliError.code,
        message: cliError.message,
      });
      logError(cliError.message, { code: cliError.code, eventType: next.type });
    } finally {
      processing = false;
      writeActivity('idle', '');
      void processNext();
    }
  };

  startStdinReader((envelope: StdinEnvelope) => {
    if (envelope.type !== 'event' || !envelope.event?.type) {
      return;
    }
    queue.push(normalizeEvent(envelope.event));
    void processNext();
  });

  process.on('SIGTERM', () => {
    writeStatus('stopping', 'SIGTERM');
    appendTrace(env.workdir, env.agentName, session.sessionId, {
      kind: 'agent.stop',
      signal: 'SIGTERM',
    });
    process.exit(0);
  });

  process.on('SIGINT', () => {
    writeStatus('stopping', 'SIGINT');
    appendTrace(env.workdir, env.agentName, session.sessionId, {
      kind: 'agent.stop',
      signal: 'SIGINT',
    });
    process.exit(0);
  });

  logInfo('ready', { channelId: env.channelId, sessionId: session.sessionId });
}

main().catch((error) => {
  const cliError = error instanceof CliError
    ? error
    : new CliError('agent_boot_failed', error instanceof Error ? error.message : 'Unknown startup failure');
  writeStatus('error', `${cliError.code}: ${cliError.message}`);
  logError(cliError.message, { code: cliError.code });
  process.exit(1);
});
