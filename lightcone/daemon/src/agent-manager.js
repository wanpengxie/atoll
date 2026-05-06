import { spawn } from 'child_process';
import { mkdirSync, statSync, writeFileSync, readdirSync, unlinkSync } from 'fs';
import { homedir } from 'os';
import path from 'path';
import { buildSystemPrompt } from './drivers/claude.js';
import { buildCodexSpawn, buildCodexSystemPrompt, parseCodexLine } from './drivers/codex.js';
import { buildKimiSpawn, buildKimiInitMessages, parseKimiLine, encodeKimiStdin } from './drivers/kimi.js';
import { buildCoagentSpawn } from './drivers/coagent.js';
import { startSession, stopSession, stopAllSessions } from './browser-login.js';

export class AgentManager {
  constructor({ serverUrl, machineApiKey, daemonSocketPath = process.env.COAGENT_DAEMON_SOCKET, daemonHttpUrl = process.env.COAGENT_DAEMON_HTTP, daemonToken = process.env.COAGENT_DAEMON_TOKEN }) {
    this.serverUrl = serverUrl;
    this.machineApiKey = machineApiKey;
    this.daemonSocketPath = daemonSocketPath ?? '';
    this.daemonHttpUrl = daemonHttpUrl ?? '';
    this.daemonToken = daemonToken ?? '';
    // key: `teamId:agentId` → { config, teamId, agentId, sessionId, proc }
    this.agents = new Map();
    // key → true (spawn in progress)
    this.starting = new Set();
  }

  _key(agentId, teamId) {
    return `${teamId ?? ''}:${agentId}`;
  }

  handle(msg, connection) {
    switch (msg.type) {
      case 'agent:start':       return this._startAgent(msg, connection);
      case 'agent:stop':        return this._stopAgent(msg.agentId, msg.teamId, connection);
      case 'agent:deliver':     return this._deliverMessage(msg, connection);
      case 'runtime:preflight': return this._preflight(msg, connection);
      case 'browser:start_login': return this._startBrowserLogin(msg, connection);
      case 'browser:stop_login':  return this._stopBrowserLogin(msg);
      case 'ping':              return connection.send({ type: 'pong' });
      default:
        console.error(`[AgentManager] Unhandled: ${msg.type}`);
    }
  }

  async _preflight(msg, connection) {
    const { preflight } = await import('./preflight.js');
    const { runtime, model, requestId } = msg;
    console.error(`[Preflight] runtime=${runtime} model=${model ?? '(default)'}`);
    const result = await preflight({ runtime, model });
    if (result.ok) console.error(`[Preflight] OK`);
    else           console.error(`[Preflight] FAIL: ${result.error?.slice(0, 200)}`);
    connection.send({ type: 'runtime:preflight:result', requestId, ok: result.ok, error: result.error ?? null });
  }

  async stopAll() {
    for (const [, agent] of this.agents) {
      if (agent.proc) agent.proc.kill();
    }
    this.agents.clear();
    await stopAllSessions();
  }

  // ── private ───────────────────────────────────────────────────────────────

  _teamWorkspaceDir(teamId) {
    const dir = path.join(homedir(), '.lightcone', 'workspace', teamId ?? '_global');
    mkdirSync(path.join(dir, 'artifacts'), { recursive: true });
    mkdirSync(path.join(dir, 'notes'), { recursive: true });
    mkdirSync(path.join(dir, 'tmp'), { recursive: true });
    return dir;
  }

  _workspaceDir(agentId, teamId) {
    const dir = path.join(homedir(), '.lightcone', 'workspace', teamId ?? '_global', agentId);
    mkdirSync(dir, { recursive: true });
    mkdirSync(path.join(dir, 'tmp'), { recursive: true });
    return dir;
  }

  _materializeSkills(workspaceDir, skills) {
    const skillsDir = path.join(workspaceDir, '.skills');
    mkdirSync(skillsDir, { recursive: true });
    // Remove stale skill files
    try {
      for (const f of readdirSync(skillsDir)) {
        unlinkSync(path.join(skillsDir, f));
      }
    } catch { /* ignore */ }
    // Write each skill with content
    for (const skill of skills) {
      if (!skill.content) continue;
      const filename = skill.name.replace(/[^a-zA-Z0-9_-]/g, '-') + '.md';
      writeFileSync(path.join(skillsDir, filename), `# ${skill.name}\n\n${skill.content}\n`);
    }
  }

  _formatDeliveryText(message) {
    return `New message in ${message.team_type === 'dm' ? 'dm from' : `#${message.team_name} from`} ${message.sender_name}: ${message.content}`;
  }

  _takePendingMessage(key) {
    if (!this._pendingMessages) return null;
    const pending = this._pendingMessages.get(key);
    if (!pending?.length) return null;
    const msg = pending.shift();
    if (pending.length === 0) this._pendingMessages.delete(key);
    return msg;
  }

  async _startAgent({ agentId, teamId, teamName, config }, connection) {
    const key = this._key(agentId, teamId);
    if (this.agents.has(key) || this.starting.has(key)) {
      console.error(`[AgentManager] Agent ${config?.displayName ?? agentId} in team ${teamName ?? teamId} already registered`);
      return;
    }
    this.starting.add(key);

    const runtime = config.runtime ?? 'coagent';
    this._teamWorkspaceDir(teamId);
    const workspaceDir = this._workspaceDir(agentId, teamId);
    const startupMsg = runtime === 'codex' ? this._takePendingMessage(key) : null;

    const skills = [];

    // Historical runtimes can still read static skill files if provided locally.
    this._materializeSkills(workspaceDir, skills);

    let proc;

    if (runtime === 'coagent') {
      const sessionIdPath = path.join(workspaceDir, 'session.id');
      const coagentSpawn = buildCoagentSpawn({
        channelId: config.channelId ?? config.channel_id ?? teamId ?? agentId,
        channelName: teamName ?? config.channelName ?? config.channel_name ?? agentId,
        channelType: config.channelType ?? config.channel_type ?? config.type ?? 'echo',
        workspaceId: config.workspaceId ?? config.workspace_id ?? '',
        workdir: workspaceDir,
        capabilitySet: config.capabilitySet ?? config.capability_set ?? { cli_binaries: ['coagent', 'coagent-kernel'] },
        daemonSocketPath: config.daemonSocketPath ?? config.daemon_socket_path ?? this.daemonSocketPath,
        daemonHttpUrl: config.daemonHttpUrl ?? config.daemon_http_url ?? this.daemonHttpUrl,
        daemonToken: config.daemonToken ?? config.daemon_token ?? this.daemonToken,
        sessionIdPath,
        agentName: config.agentName ?? config.agent_name ?? agentId,
      });

      console.error(`[AgentManager] Spawning coagent for ${config.displayName ?? agentId} team=${teamName ?? teamId ?? 'none'} (session=${coagentSpawn.sessionId})`);

      proc = spawn(coagentSpawn.command, coagentSpawn.args, {
        cwd: workspaceDir,
        env: coagentSpawn.env,
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      this.agents.set(key, {
        config, teamId, agentId, sessionId: coagentSpawn.sessionId, proc,
        runtime: 'coagent',
      });
      this.starting.delete(key);

      proc.stdout.on('data', (chunk) => {
        const text = chunk.toString();
        if (text.trim()) process.stdout.write(text);
      });
    } else if (runtime === 'kimi') {
      // ── Kimi CLI ──────────────────────────────────────────────────────────
      const kimiSpawn = buildKimiSpawn({
        config, agentId, workspaceDir,
      });

      console.error(`[AgentManager] Spawning kimi for ${config.displayName ?? agentId} team=${teamName ?? teamId ?? 'none'} (session=${kimiSpawn.sessionId})`);

      proc = spawn('kimi', kimiSpawn.args, {
        cwd: workspaceDir,
        env: kimiSpawn.env,
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      // Send initialize + prompt via stdin
      const initMsgs = buildKimiInitMessages(kimiSpawn);
      for (const msg of initMsgs) {
        proc.stdin.write(msg + '\n');
      }

      // Track Kimi-specific state for parsing
      const kimiState = { sessionId: kimiSpawn.sessionId, sessionAnnounced: false };

      this.agents.set(key, {
        config, teamId, agentId, sessionId: kimiSpawn.sessionId, proc,
        runtime: 'kimi', kimiState, kimiIdle: false,
      });
      this.starting.delete(key);

      let buffer = '';
      proc.stdout.on('data', (chunk) => {
        buffer += chunk.toString();
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (!line.trim()) continue;
          this._parseKimiLine(key, agentId, teamId, line, connection);
        }
      });
    } else if (runtime === 'codex') {
      const startupPrompt = startupMsg
        ? `${buildCodexSystemPrompt(config, agentId)}\n\nNew message received:\n\n${this._formatDeliveryText(startupMsg.message)}\n\nRespond as appropriate. Complete all required work before stopping.`
        : buildCodexSystemPrompt(config, agentId);

      const codexSpawn = buildCodexSpawn({
        config,
        agentId,
        workspaceDir,
        prompt: startupPrompt,
      });

      console.error(`[AgentManager] Spawning codex for ${config.displayName ?? agentId} team=${teamName ?? teamId ?? 'none'} (session=${config.sessionId ?? 'new'})`);

      proc = spawn('codex', codexSpawn.args, {
        cwd: workspaceDir,
        env: codexSpawn.env,
        stdio: ['ignore', 'pipe', 'pipe'],
      });

      this.agents.set(key, {
        config, teamId, agentId, sessionId: config.sessionId ?? null, proc,
        runtime: 'codex',
      });
      this.starting.delete(key);

      let buffer = '';
      proc.stdout.on('data', (chunk) => {
        buffer += chunk.toString();
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (!line.trim()) continue;
          this._parseCodexLine(key, agentId, teamId, line, connection);
        }
      });
    } else {
      // ── Claude CLI (default) ──────────────────────────────────────────────
      const args = [
        '--print',
        '--allow-dangerously-skip-permissions',
        '--dangerously-skip-permissions',
        '--verbose',
        '--output-format', 'stream-json',
        '--input-format', 'stream-json',
        '--system-prompt', buildSystemPrompt(config, agentId, skills),
        '--disallowed-tools', 'EnterPlanMode,ExitPlanMode',
      ];

      if (config.sessionId) {
        // Only resume if the session file exists locally
        const projectSlug = workspaceDir.replace(/[\/\.]/g, '-');
        const sessionFile = path.join(homedir(), '.claude', 'projects', projectSlug, `${config.sessionId}.jsonl`);
        try {
          statSync(sessionFile);
          args.push('--resume', config.sessionId);
        } catch {
          console.error(`[AgentManager] Session ${config.sessionId} not found locally, starting fresh`);
        }
      }

      const spawnEnv = { ...process.env, FORCE_COLOR: '0', ...(config.envVars ?? {}) };
      delete spawnEnv.CLAUDECODE;

      console.error(`[AgentManager] Spawning claude for ${config.displayName ?? agentId} team=${teamName ?? teamId ?? 'none'} (session=${config.sessionId ?? 'new'})`);

      proc = spawn('claude', args, {
        cwd: workspaceDir,
        env: spawnEnv,
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      this.agents.set(key, { config, teamId, agentId, sessionId: config.sessionId ?? null, proc, runtime: 'claude' });
      this.starting.delete(key);

      // Parse stdout stream for session ID and activity updates
      let buffer = '';
      proc.stdout.on('data', (chunk) => {
        buffer += chunk.toString();
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (!line.trim()) continue;
          this._parseLine(key, agentId, teamId, line, connection);
        }
      });
    }

    proc.stderr.on('data', (data) => {
      const text = data.toString().trim();
      if (text) console.error(`[AgentManager][${config.displayName ?? agentId}] stderr: ${text.slice(0, 500)}`);
    });

    proc.on('exit', (code) => {
      const agent = this.agents.get(key);
      console.error(`[AgentManager] Agent ${config.displayName ?? agentId} team=${teamName ?? teamId ?? 'none'} exited (code=${code})`);
      this.agents.delete(key);

      if (code === 0 && runtime === 'codex' && this._pendingMessages?.get(key)?.length) {
        const restartConfig = { ...config, sessionId: agent?.sessionId ?? config.sessionId ?? null };
        this._startAgent({ agentId, teamId, config: restartConfig }, connection);
        return;
      }

      // If exited immediately with an error and had a session, retry without session (session file may not exist on this machine)
      if (code !== 0 && config.sessionId && !this._retried?.has(key)) {
        if (!this._retried) this._retried = new Set();
        this._retried.add(key);
        console.error(`[AgentManager] Retrying ${agentId} team=${teamId} without session (session may not exist locally)`);
        const retryConfig = { ...config, sessionId: null };
        this._startAgent({ agentId, teamId, config: retryConfig }, connection);
        return;
      }

      connection.send({ type: 'agent:status', agentId, teamId, status: 'inactive' });
      connection.send({ type: 'agent:activity', agentId, teamId, activity: 'offline', detail: '', entries: [] });
    });

    // Send startup prompt
    if (runtime === 'kimi' || runtime === 'codex' || runtime === 'coagent') {
      // These runtimes receive turn events through their own stdin protocol.
    } else {
      this._write(key, 'You have just started. Follow your startup sequence: first call read_memory with path="MEMORY.md" to load your memory index, then call check_messages.');
    }

    console.error(`[AgentManager] Agent ${config.displayName ?? agentId} is now active (team=${teamName ?? teamId ?? 'none'})`);
    connection.send({ type: 'agent:status', agentId, teamId, status: 'active' });
    connection.send({ type: 'agent:activity', agentId, teamId, activity: 'online', detail: '', entries: [] });

    // Flush any messages that arrived while the agent was starting
    this._flushPending(key, connection);
  }

  async _startBrowserLogin(msg, connection) {
    const platform = msg.platform ?? 'xhs';
    console.error(`[AgentManager] Starting browser login for platform=${platform}`);
    try {
      await startSession(platform, connection, msg.userId ?? 'default');
    } catch (err) {
      console.error(`[AgentManager] Browser login start failed (${platform}):`, err.message);
      connection.send({ type: 'browser:login_error', platform, error: err.message });
    }
  }

  _stopBrowserLogin(msg) {
    const platform = msg.platform ?? 'xhs';
    console.error(`[AgentManager] Stopping browser login for platform=${platform}`);
    return stopSession(platform);
  }

  _stopAgent(agentId, teamId, connection) {
    const key = this._key(agentId, teamId);
    const agent = this.agents.get(key);
    if (!agent) return;
    console.error(`[AgentManager] Stopping agent ${agentId} team=${teamId ?? 'none'}`);
    agent.proc?.kill();
    // exit handler will report status
  }

  _deliverMessage(msg, connection) {
    const { agentId, teamId, seq, message } = msg;
    const key = this._key(agentId, teamId);
    connection.send({ type: 'agent:deliver:ack', agentId, teamId, seq });

    if (!this.agents.has(key) && !this.starting.has(key)) {
      // Agent not running — queue the message and request config to spawn it
      console.error(`[AgentManager] Agent ${agentId.slice(0,8)} team=${message.team_name ?? teamId} not running, requesting start for seq=${seq}`);
      if (!this._pendingMessages) this._pendingMessages = new Map();
      const pending = this._pendingMessages.get(key) ?? [];
      pending.push(msg);
      this._pendingMessages.set(key, pending);
      connection.send({ type: 'agent:request_start', agentId, teamId });
      return;
    }

    if (this.starting.has(key)) {
      // Spawn in progress — queue the message for delivery after start
      console.error(`[AgentManager] Agent ${agentId.slice(0,8)} team=${message.team_name ?? teamId} still starting, queuing seq=${seq}`);
      if (!this._pendingMessages) this._pendingMessages = new Map();
      const pending = this._pendingMessages.get(key) ?? [];
      pending.push(msg);
      this._pendingMessages.set(key, pending);
      return;
    }

    const text = this._formatDeliveryText(message);
    const agent = this.agents.get(key);
    console.error(`[AgentManager] Delivering seq=${seq} to agent ${agent?.config?.displayName ?? agentId.slice(0,8)} team=${message.team_name ?? teamId}`);
    if (agent?.runtime === 'codex') {
      if (!this._pendingMessages) this._pendingMessages = new Map();
      const pending = this._pendingMessages.get(key) ?? [];
      pending.push(msg);
      this._pendingMessages.set(key, pending);
      return;
    }
    this._write(key, text);
  }

  _flushPending(key, connection) {
    if (!this._pendingMessages) return;
    if (this.agents.get(key)?.runtime === 'codex') return;
    const pending = this._pendingMessages.get(key);
    if (!pending || pending.length === 0) return;
    this._pendingMessages.delete(key);
    for (const msg of pending) {
      const { agentId, teamId, seq, message } = msg;
      const text = this._formatDeliveryText(message);
      const agent = this.agents.get(key);
      console.error(`[AgentManager] Flushing queued seq=${seq} to agent ${agent?.config?.displayName ?? agentId.slice(0,8)} team=${message.team_name ?? teamId}`);
      this._write(key, text);
    }
  }

  // Write a user message to the running agent process via stdin
  _write(key, text) {
    const agent = this.agents.get(key);
    if (!agent?.proc) return;

    let line;
    if (agent.runtime === 'kimi') {
      // Kimi uses JSON-RPC: prompt (idle) or steer (busy)
      const mode = agent.kimiIdle ? 'idle' : 'busy';
      agent.kimiIdle = false; // will be set back to true on TurnEnd
      line = encodeKimiStdin(text, mode) + '\n';
    } else if (agent.runtime === 'codex') {
      return;
    } else if (agent.runtime === 'coagent') {
      line = JSON.stringify({
        type: 'event',
        event: {
          type: 'user.message.posted',
          source: 'agent-manager',
          payload: {
            message: {
              channelId: agent.teamId ?? '',
              content: text,
              senderKind: 'human',
              payload: { type: 'user.text', body: { text } },
            },
          },
        },
      }) + '\n';
    } else {
      // Claude uses stream-json
      line = JSON.stringify({
        type: 'user',
        message: { role: 'user', content: [{ type: 'text', text }] },
      }) + '\n';
    }

    try {
      agent.proc.stdin.write(line);
    } catch (err) {
      console.error(`[AgentManager] stdin write error for ${key}:`, err.message);
    }
  }

  _parseKimiLine(key, agentId, teamId, line, connection) {
    const agent = this.agents.get(key);
    if (!agent) return;
    const events = parseKimiLine(line, agent.kimiState);
    for (const evt of events) {
      switch (evt.kind) {
        case 'session_init':
          if (agent.sessionId !== evt.sessionId) {
            agent.sessionId = evt.sessionId;
            connection.send({ type: 'agent:session', agentId, teamId, sessionId: evt.sessionId });
          }
          break;
        case 'thinking':
          connection.send({ type: 'agent:activity', agentId, teamId, activity: 'thinking', detail: '', entries: [] });
          break;
        case 'tool_call':
          connection.send({ type: 'agent:activity', agentId, teamId, activity: 'working', detail: evt.name, entries: [] });
          break;
        case 'turn_end':
          agent.kimiIdle = true;
          connection.send({ type: 'agent:activity', agentId, teamId, activity: 'online', detail: '', entries: [] });
          break;
        case 'error':
          console.error(`[AgentManager][kimi][${agentId}] Error: ${evt.message}`);
          break;
      }
    }
  }

  _parseCodexLine(key, agentId, teamId, line, connection) {
    const agent = this.agents.get(key);
    if (!agent) return;
    const events = parseCodexLine(line);
    for (const evt of events) {
      switch (evt.kind) {
        case 'session_init':
          if (agent.sessionId !== evt.sessionId) {
            agent.sessionId = evt.sessionId;
            connection.send({ type: 'agent:session', agentId, teamId, sessionId: evt.sessionId });
          }
          break;
        case 'thinking':
          connection.send({ type: 'agent:activity', agentId, teamId, activity: 'thinking', detail: '', entries: [] });
          break;
        case 'tool_call':
          connection.send({ type: 'agent:activity', agentId, teamId, activity: 'working', detail: evt.name, entries: [] });
          break;
        case 'turn_end':
          connection.send({ type: 'agent:activity', agentId, teamId, activity: 'online', detail: '', entries: [] });
          break;
        case 'error':
          console.error(`[AgentManager][codex][${agentId}] Error: ${evt.message}`);
          break;
      }
    }
  }

  _parseLine(key, agentId, teamId, line, connection) {
    let event;
    try { event = JSON.parse(line); }
    catch { return; }

    // Capture and sync session ID
    if (event.session_id) {
      const agent = this.agents.get(key);
      if (agent && agent.sessionId !== event.session_id) {
        agent.sessionId = event.session_id;
        connection.send({ type: 'agent:session', agentId, teamId, sessionId: event.session_id });
      }
    }

    // Activity state machine
    const displayName = this.agents.get(key)?.config?.displayName ?? agentId.slice(0, 8);
    if (event.type === 'assistant') {
      const content = event.message?.content ?? [];
      for (const block of content) {
        if (block.type === 'thinking') {
          console.error(`[AgentManager][${displayName}] <thinking> ${block.thinking?.slice(0, 500)}`);
        } else if (block.type === 'text') {
          console.error(`[AgentManager][${displayName}] <text> ${block.text?.slice(0, 500)}`);
        } else if (block.type === 'tool_use') {
          console.error(`[AgentManager][${displayName}] <tool_use> ${block.name} params=${JSON.stringify(block.input ?? {}).slice(0, 500)}`);
          connection.send({ type: 'agent:activity', agentId, teamId, activity: 'working', detail: block.name, entries: [] });
        }
      }
      if (!content.some(c => c.type === 'tool_use')) {
        connection.send({ type: 'agent:activity', agentId, teamId, activity: 'thinking', detail: '', entries: [] });
      }
    } else if (event.type === 'tool') {
      const content = event.content;
      const resultStr = Array.isArray(content)
        ? content.map(c => c.text ?? c.content ?? JSON.stringify(c)).join('').slice(0, 1000)
        : JSON.stringify(content ?? event).slice(0, 1000);
      console.error(`[AgentManager][${displayName}] <tool_result> id=${event.tool_use_id ?? '?'} → ${resultStr}`);
    } else if (event.type === 'user') {
      // Tool results come back as user-turn messages in stream-json
      const toolResults = (event.message?.content ?? []).filter(c => c.type === 'tool_result');
      for (const tr of toolResults) {
        const resultStr = Array.isArray(tr.content)
          ? tr.content.map(c => c.text ?? c.content ?? JSON.stringify(c)).join('').slice(0, 1000)
          : JSON.stringify(tr.content ?? tr).slice(0, 1000);
        console.error(`[AgentManager][${displayName}] <tool_result> id=${tr.tool_use_id ?? '?'} → ${resultStr}`);
      }
    } else if (event.type === 'result') {
      console.error(`[AgentManager][${displayName}] turn done (stop_reason=${event.stop_reason ?? '?'})`);
      connection.send({ type: 'agent:activity', agentId, teamId, activity: 'online', detail: '', entries: [] });
    }
  }
}
