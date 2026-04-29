#!/usr/bin/env node
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { readFileSync } from 'fs';
import path, { extname } from 'path';

const cliArgs = process.argv.slice(2);
function getArg(name) {
  const idx = cliArgs.indexOf(name);
  return idx !== -1 && cliArgs[idx + 1] ? cliArgs[idx + 1] : '';
}

const SERVER_URL      = process.env.SERVER_URL || getArg('--server-url') || 'http://localhost:8777';
const MACHINE_API_KEY = process.env.MACHINE_API_KEY || getArg('--auth-token') || '';
const AGENT_ID        = process.env.AGENT_ID || getArg('--agent-id') || '';
const TEAM_ID      = process.env.TEAM_ID || getArg('--team-id') || '';  // injected per-team at spawn time
const WORKSPACE_DIR = path.resolve(process.env.WORKSPACE_DIR || getArg('--workspace-dir') || process.cwd());
const TEAM_WORKSPACE_DIR = path.dirname(WORKSPACE_DIR);

// Current active teamId for memory isolation (defaults to spawn-time TEAM_ID)
let currentTeamId = TEAM_ID;

const WORKSPACE_BINARY_MIME = {
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.webp': 'image/webp',
  '.gif': 'image/gif',
  '.pdf': 'application/pdf',
};

function dataUrlSummary(content) {
  if (typeof content !== 'string' || !content.startsWith('data:')) return null;
  const commaIdx = content.indexOf(',');
  if (commaIdx === -1) return null;
  const header = content.slice(5, commaIdx);
  const mime = header.split(';')[0] || 'application/octet-stream';
  const isBase64 = header.split(';').includes('base64');
  let bytes = null;
  if (isBase64) {
    const encoded = content.slice(commaIdx + 1).replace(/\s/g, '');
    const padding = encoded.endsWith('==') ? 2 : encoded.endsWith('=') ? 1 : 0;
    bytes = Math.floor(encoded.length * 3 / 4) - padding;
  }
  return { mime, bytes };
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return 'unknown size';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function isInsideDir(filePath, dir) {
  const rel = path.relative(dir, filePath);
  return rel === '' || (!!rel && !rel.startsWith('..') && !path.isAbsolute(rel));
}

function resolveLocalWorkspaceFile(filePath) {
  const resolved = path.resolve(WORKSPACE_DIR, filePath);
  if (isInsideDir(resolved, WORKSPACE_DIR)) return resolved;

  const allowedTeamRoots = ['artifacts', 'notes', 'tmp'].map(dir => path.join(TEAM_WORKSPACE_DIR, dir));
  if (allowedTeamRoots.some(root => isInsideDir(resolved, root))) return resolved;

  throw new Error(`Local file must be inside the agent workspace or team shared artifacts/notes/tmp directories. Got: ${filePath}`);
}

async function api(method, path, body) {
  const url = `${SERVER_URL}/internal/agent/${AGENT_ID}${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${MACHINE_API_KEY}`,
    },
    body: body != null ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${method} ${path} → ${res.status}: ${text}`);
  }
  return res.json();
}

const server = new McpServer({ name: 'chat', version: '0.1.0' });

// ── check_messages ────────────────────────────────────────────────────────────
server.tool('check_messages', 'Check for new messages in your inbox', {}, async () => {
  const data = await api('GET', '/receive');
  const msgs = data.messages ?? [];
  if (msgs.length === 0) return { content: [{ type: 'text', text: 'No new messages.' }] };

  // Track the teamId of the most recent message for memory isolation
  const lastMsg = msgs[msgs.length - 1];
  if (lastMsg.team_id) currentTeamId = lastMsg.team_id;

  const text = msgs.map(m =>
    `[${m.team_type === 'dm' ? `dm:@${m.team_name}` : `#${m.team_name}`}] ${m.sender_name}: ${m.content}`
    + (m.task_status ? ` [task #${m.task_number} ${m.task_status}]` : '')
  ).join('\n');
  return { content: [{ type: 'text', text }] };
});

// ── send_message ──────────────────────────────────────────────────────────────
server.tool('send_message', 'Send a message to a team, DM, or thread', {
  target:  z.string().describe('Target: #team-name | dm:@agentName | #team-name:shortMsgId'),
  content: z.string().describe('Message content'),
}, async ({ target, content }) => {
  const data = await api('POST', '/send', { target, content });
  return { content: [{ type: 'text', text: `Sent. messageId=${data.messageId} threadTarget=${data.threadTarget}` }] };
});

// ── search_messages ──────────────────────────────────────────────────────────
server.tool('search_messages', 'Search messages within a specific team. You must specify the team. Use this to find relevant conversations by keyword.', {
  query:   z.string().describe('Search query'),
  team: z.string().describe('Target team to search within, e.g. "#general", "dm:@richard". Required — you may only search teams you are a member of.'),
  limit:   z.number().optional().describe('Max results (default 10, max 20)'),
}, async ({ query, team, limit }) => {
  const trimmed = query.trim();
  if (!trimmed) return { content: [{ type: 'text', text: 'Search query cannot be empty.' }] };
  if (!team?.trim()) return { content: [{ type: 'text', text: 'team is required. Specify which team to search, e.g. "#general".' }] };
  const params = new URLSearchParams({ q: trimmed, limit: String(Math.min(limit ?? 10, 20)) });
  params.set('team', team);
  try {
    const data = await api('GET', `/search?${params}`);
    if (!data.results || data.results.length === 0)
      return { content: [{ type: 'text', text: 'No search results.' }] };
    const formatted = data.results.map((r, i) => [
      `[${i + 1}] msg=${r.id} seq=${r.seq} time=${r.createdAt}`,
      `team: #${r.teamName}`,
      `sender: @${r.senderName}${r.senderType === 'agent' ? ' (agent)' : ''}`,
      `content: ${r.snippet}`,
    ].join('\n')).join('\n\n');
    return { content: [{ type: 'text', text: `## Search Results for "${trimmed}" (${data.results.length} results)\n\n${formatted}` }] };
  } catch (err) {
    return { isError: true, content: [{ type: 'text', text: `Error: ${err.message}` }] };
  }
});

// ── view_file ────────────────────────────────────────────────────────────────
server.tool('view_file', 'Download an attached image by its attachment ID and save it locally so you can view it. Use this when messages contain image attachments.', {
  attachment_id: z.string().describe('The attachment UUID'),
}, async ({ attachment_id }) => {
  try {
    const fs = await import('fs');
    const pathMod = await import('path');
    const os = await import('os');
    const cacheDir = pathMod.join(os.homedir(), '.lightcone', 'attachments');
    fs.mkdirSync(cacheDir, { recursive: true });
    // Check cache
    const existing = fs.readdirSync(cacheDir).find(f => f.startsWith(attachment_id));
    if (existing) {
      return { content: [{ type: 'text', text: `File already cached at: ${pathMod.join(cacheDir, existing)}\n\nUse your Read tool to view this image.` }] };
    }
    const res = await fetch(`${SERVER_URL}/api/attachments/${attachment_id}`, {
      headers: { 'Authorization': `Bearer ${MACHINE_API_KEY}` },
      redirect: 'follow',
    });
    if (!res.ok) return { isError: true, content: [{ type: 'text', text: `Error: Failed to download attachment (${res.status})` }] };
    const contentType = res.headers.get('content-type') || 'application/octet-stream';
    const extMap = { 'image/jpeg': '.jpg', 'image/png': '.png', 'image/gif': '.gif', 'image/webp': '.webp' };
    const ext = extMap[contentType] || '.bin';
    const filePath = pathMod.join(cacheDir, `${attachment_id}${ext}`);
    const buffer = Buffer.from(await res.arrayBuffer());
    fs.writeFileSync(filePath, buffer);
    return { content: [{ type: 'text', text: `Downloaded to: ${filePath}\n\nUse your Read tool to view this image.` }] };
  } catch (err) {
    return { isError: true, content: [{ type: 'text', text: `Error: ${err.message}` }] };
  }
});

// ── list_server ───────────────────────────────────────────────────────────────
server.tool('list_server', 'List teams, agents, and humans on the server', {}, async () => {
  const data = await api('GET', '/server');
  const teams = (data.teams ?? []).map(c => `  #${c.name}${c.joined ? ' (joined)' : ''} — ${c.description}`).join('\n');
  const agents   = (data.agents   ?? []).map(a => `  @${a.name} [${a.status}]`).join('\n');
  const humans   = (data.humans   ?? []).map(h => `  @${h.name}`).join('\n');
  return { content: [{ type: 'text', text: `Teams:\n${teams}\n\nAgents:\n${agents}\n\nHumans:\n${humans}` }] };
});

// ── list_tasks ────────────────────────────────────────────────────────────────
server.tool('list_tasks', 'List tasks in a team', {
  team: z.string().describe('Target: #team-name'),
  status:  z.enum(['all', 'todo', 'in_progress', 'in_review', 'done']).optional(),
}, async ({ team, status }) => {
  const params = new URLSearchParams({ team, status: status ?? 'all' });
  const data = await api('GET', `/tasks?${params}`);
  const tasks = data.tasks ?? [];
  if (tasks.length === 0) return { content: [{ type: 'text', text: 'No tasks found.' }] };

  const text = tasks.map(t =>
    `#${t.taskNumber} [${t.status}] ${t.title}` +
    (t.claimedByName ? ` (claimed by ${t.claimedByName})` : '')
  ).join('\n');
  return { content: [{ type: 'text', text }] };
});

// ── create_tasks ──────────────────────────────────────────────────────────────
server.tool('create_tasks', 'Create one or more tasks in a team', {
  team: z.string().describe('Target: #team-name'),
  tasks:   z.array(z.object({ title: z.string() })).describe('Array of tasks to create'),
}, async ({ team, tasks }) => {
  const data = await api('POST', '/tasks', { team, tasks });
  const created = (data.tasks ?? []).map(t => `#${t.taskNumber} ${t.title}`).join('\n');
  return { content: [{ type: 'text', text: `Created:\n${created}` }] };
});

// ── claim_tasks ───────────────────────────────────────────────────────────────
server.tool('claim_tasks', 'Claim one or more tasks to work on', {
  team:         z.string().describe('Target: #team-name'),
  task_numbers: z.array(z.number()).optional().describe('Task numbers to claim'),
  message_ids:  z.array(z.string()).optional().describe('Short message IDs to claim'),
}, async ({ team, task_numbers, message_ids }) => {
  const data = await api('POST', '/tasks/claim', { team, task_numbers, message_ids });
  const results = (data.results ?? []).map(r =>
    `#${r.taskNumber}: ${r.success ? 'claimed' : `failed (${r.reason})`}`
  ).join('\n');
  return { content: [{ type: 'text', text: results }] };
});

// ── unclaim_task ──────────────────────────────────────────────────────────────
server.tool('unclaim_task', 'Release a claimed task', {
  team:        z.string().describe('Target: #team-name'),
  task_number: z.number().describe('Task number to unclaim'),
}, async ({ team, task_number }) => {
  await api('POST', '/tasks/unclaim', { team, task_number });
  return { content: [{ type: 'text', text: `Task #${task_number} unclaimed.` }] };
});

// ── update_task_status ────────────────────────────────────────────────────────
server.tool('update_task_status', 'Update the status of a task', {
  team:        z.string().describe('Target: #team-name'),
  task_number: z.number().describe('Task number'),
  status:      z.enum(['todo', 'in_progress', 'in_review', 'done']).describe('New status'),
}, async ({ team, task_number, status }) => {
  await api('POST', '/tasks/update-status', { team, task_number, status });
  return { content: [{ type: 'text', text: `Task #${task_number} → ${status}` }] };
});

// ── list_memory ───────────────────────────────────────────────────────────────
server.tool('list_memory', 'List all memory files stored for this agent in the current team', {}, async () => {
  const chParam = currentTeamId ? `&teamId=${encodeURIComponent(currentTeamId)}` : '';
  const data = await api('GET', `/memory?_=1${chParam}`);
  const files = data.files ?? [];
  if (files.length === 0) return { content: [{ type: 'text', text: 'No memory files yet.' }] };
  return { content: [{ type: 'text', text: files.map(f => f.path).join('\n') }] };
});

// ── read_memory ───────────────────────────────────────────────────────────────
server.tool('read_memory', 'Read a memory file by path (e.g. "MEMORY.md" or "notes/work-log.md")', {
  path: z.string().describe('File path, e.g. "MEMORY.md" or "notes/teams.md"'),
}, async ({ path }) => {
  const chParam = currentTeamId ? `&teamId=${encodeURIComponent(currentTeamId)}` : '';
  try {
    const data = await api('GET', `/memory?path=${encodeURIComponent(path)}${chParam}`);
    return { content: [{ type: 'text', text: data.content }] };
  } catch (err) {
    if (err.message.includes('404')) return { content: [{ type: 'text', text: `(empty — ${path} has no content yet)` }] };
    throw err;
  }
});

// ── write_memory ──────────────────────────────────────────────────────────────
server.tool('write_memory', 'Write or update a memory file (full content replace)', {
  path:    z.string().describe('File path, e.g. "MEMORY.md" or "notes/work-log.md"'),
  content: z.string().describe('Full file content to store'),
}, async ({ path, content }) => {
  const chParam = currentTeamId ? `&teamId=${encodeURIComponent(currentTeamId)}` : '';
  await api('PUT', `/memory?path=${encodeURIComponent(path)}${chParam}`, { content });
  return { content: [{ type: 'text', text: `Saved ${path}` }] };
});

// ── list_workspace ────────────────────────────────────────────────────────────
server.tool('list_workspace', 'List all files in the shared team workspace (BRIEF.md, KNOWLEDGE.md, artifacts/, notes/)', {}, async () => {
  if (!currentTeamId) return { content: [{ type: 'text', text: 'No team context.' }] };
  const data = await api('GET', `/team-memory?teamId=${encodeURIComponent(currentTeamId)}`);
  const files = data.files ?? [];
  if (files.length === 0) return { content: [{ type: 'text', text: 'Team workspace is empty.' }] };
  return { content: [{ type: 'text', text: files.map(f => f.path).join('\n') }] };
});

// ── read_workspace ────────────────────────────────────────────────────────────
server.tool('read_workspace', 'Read a file from the shared team workspace (e.g. "BRIEF.md", "KNOWLEDGE.md", "artifacts/report.html")', {
  path: z.string().describe('File path relative to team workspace root'),
}, async ({ path }) => {
  if (!currentTeamId) return { content: [{ type: 'text', text: 'No team context.' }] };
  try {
    const data = await api('GET', `/team-memory?path=${encodeURIComponent(path)}&teamId=${encodeURIComponent(currentTeamId)}`);
    const summary = dataUrlSummary(data.content);
    if (summary) {
      return {
        content: [{
          type: 'text',
          text: `${path} is a binary workspace file (${summary.mime}, ${formatBytes(summary.bytes)}). Do not read it as text. Use it by path in the Files tab. If a temporary public URL is needed for a chat preview or external platform, use upload_image separately.`,
        }],
      };
    }
    return { content: [{ type: 'text', text: data.content }] };
  } catch (err) {
    if (err.message.includes('404')) return { content: [{ type: 'text', text: `(empty — ${path} has no content yet)` }] };
    throw err;
  }
});

// ── write_workspace ───────────────────────────────────────────────────────────
server.tool('write_workspace', 'Write a file to the shared team workspace. Use this to save ALL deliverables: code, HTML, scripts, reports, data files, images — everything goes under artifacts/. Also use for KNOWLEDGE.md and shared notes. For binary files (images/PNG/JPG), encode as base64 data URL: read the file with fs.readFileSync, then format as "data:image/png;base64," + buf.toString("base64"). The server will decode and serve them correctly.', {
  path:    z.string().describe('File path relative to team workspace root, e.g. "artifacts/result.html" or "artifacts/cover.png"'),
  content: z.string().describe('File content. For images: base64 data URL "data:image/png;base64,<base64data>"'),
}, async ({ path, content }) => {
  if (!currentTeamId) return { content: [{ type: 'text', text: 'No team context.' }] };
  await api('PUT', `/team-memory?path=${encodeURIComponent(path)}&teamId=${encodeURIComponent(currentTeamId)}`, { content });
  return { content: [{ type: 'text', text: `Saved to team workspace: ${path}` }] };
});

server.tool('write_workspace_file', 'Write a local file directly to the shared team workspace. Prefer this over write_workspace for images/PDFs/binary files so large base64 content never enters the model context. The source file may be a relative path under the current agent workspace, or an absolute path inside the agent workspace/team shared artifacts/notes/tmp directories.', {
  file_path: z.string().describe('Local file path. Relative paths resolve from the current agent workspace. Absolute paths must stay inside the agent/team workspace.'),
  path:      z.string().describe('Destination path relative to team workspace root, e.g. "artifacts/cover.png"'),
}, async ({ file_path, path }) => {
  if (!currentTeamId) return { content: [{ type: 'text', text: 'No team context.' }] };
  const localPath = resolveLocalWorkspaceFile(file_path);
  const ext = extname(path || localPath).toLowerCase();
  const mime = WORKSPACE_BINARY_MIME[ext] ?? 'application/octet-stream';
  const buf = readFileSync(localPath);
  const content = `data:${mime};base64,${buf.toString('base64')}`;
  await api('PUT', `/team-memory?path=${encodeURIComponent(path)}&teamId=${encodeURIComponent(currentTeamId)}`, { content });
  return { content: [{ type: 'text', text: `Saved local file to team workspace: ${path} (${mime}, ${formatBytes(buf.length)})` }] };
});

// ── get_credential ───────────────────────────────────────────────────────────
server.tool('get_credential',
  'Retrieve decrypted credential fields for a platform granted to this agent (e.g. XHS_COOKIE for "xhs"). Use when you need to inject credentials into a browser session or external call.',
  {
    platform: z.string().describe('Platform key, e.g. "xhs", "x", "youtube"'),
  },
  async ({ platform }) => {
    try {
      const grants = await api('GET', '/credential-grants');
      const match = grants.find(g => g.platform === platform);
      if (!match) return { content: [{ type: 'text', text: `No credential found for platform "${platform}". Ask the human to connect the account via Settings → 连接外部账号.` }] };
      const fields = Object.entries(match.envVars).map(([k, v]) => `${k}=${v}`).join('\n');
      return { content: [{ type: 'text', text: fields }] };
    } catch (err) {
      return { isError: true, content: [{ type: 'text', text: `Error: ${err.message}` }] };
    }
  }
);

// ── skill_list ───────────────────────────────────────────────────────────────
server.tool('skill_list', 'List all skills available to you (platform + bound). Returns index only (name + description), not full content.', {}, async () => {
  const skills = await api('GET', `/skills`);
  if (!skills || skills.length === 0) return { content: [{ type: 'text', text: 'No skills available.' }] };
  const lines = skills.map(s => `- [${s.type}] **${s.name}** — ${s.description}`);
  return { content: [{ type: 'text', text: lines.join('\n') }] };
});

// ── skill_read ───────────────────────────────────────────────────────────────
server.tool('skill_read', 'Read the full content of a skill by name or ID', {
  name: z.string().describe('Skill name or ID'),
}, async ({ name }) => {
  try {
    const skill = await api('GET', `/skills/${encodeURIComponent(name)}`);
    return { content: [{ type: 'text', text: `# ${skill.name}\n\n${skill.content}` }] };
  } catch (err) {
    if (err.message.includes('404')) return { content: [{ type: 'text', text: `Skill "${name}" not found.` }] };
    throw err;
  }
});

// ── skill_create ─────────────────────────────────────────────────────────────
server.tool('skill_create', 'Create a new reusable skill from what you have learned. Auto-binds to this agent.', {
  name:        z.string().describe('Short skill name (lowercase, hyphens ok), e.g. "xhs-posting"'),
  description: z.string().describe('One-line description of what this skill covers'),
  content:     z.string().describe('Full skill content in markdown — procedures, steps, tips'),
  tags:        z.array(z.string()).optional().describe('Optional tags for categorization'),
}, async ({ name, description, content, tags }) => {
  const result = await api('POST', '/skills', { name, description, content, tags: tags ?? [] });
  return { content: [{ type: 'text', text: `Skill "${result.name}" created and bound to this agent.` }] };
});

// ── skill_update ─────────────────────────────────────────────────────────────
server.tool('skill_update', 'Update an existing skill content, description, or tags', {
  name:        z.string().describe('Skill name or ID to update'),
  content:     z.string().optional().describe('New full content (replaces existing)'),
  description: z.string().optional().describe('New description'),
  tags:        z.array(z.string()).optional().describe('New tags'),
}, async ({ name, content, description, tags }) => {
  const body = {};
  if (content != null) body.content = content;
  if (description != null) body.description = description;
  if (tags != null) body.tags = tags;
  const result = await api('PATCH', `/skills/${encodeURIComponent(name)}`, body);
  return { content: [{ type: 'text', text: `Skill "${result.name}" updated.` }] };
});

// ── skill_search ─────────────────────────────────────────────────────────────
server.tool('skill_search', 'Search for skills by keyword across all accessible skills', {
  query: z.string().describe('Search keyword'),
}, async ({ query }) => {
  const skills = await api('GET', `/skills/search?q=${encodeURIComponent(query)}`);
  if (!skills || skills.length === 0) return { content: [{ type: 'text', text: `No skills found for "${query}".` }] };
  const lines = skills.map(s => `- [${s.type}] **${s.name}** — ${s.description}`);
  return { content: [{ type: 'text', text: lines.join('\n') }] };
});

// ── read_file_base64 ──────────────────────────────────────────────────────────
// Agent 需要在本机读取图片文件内容，转为 base64 后上传服务器
server.tool('read_file_base64',
  '读取本机文件内容，返回 base64 编码。优先使用 write_workspace_file 保存正式产出；只有外部工具明确需要 base64 字符串时才使用。',
  {
    file_path: z.string().describe('本机文件路径。相对路径从当前 agent workspace 解析；绝对路径必须在 agent/team workspace 内。'),
  },
  async ({ file_path }) => {
    const localPath = resolveLocalWorkspaceFile(file_path);
    const data = readFileSync(localPath).toString('base64');
    return { content: [{ type: 'text', text: data }] };
  }
);

// ── upload_image ──────────────────────────────────────────────────────────────
server.tool('upload_image',
  '将本机图片文件上传为临时公开 URL，用于聊天预览、二维码截图或外部平台临时访问。它不会保存正式产出；正式产出必须同时写入 artifacts/，优先使用 write_workspace_file。',
  {
    file_path: z.string().describe('本机图片文件路径。相对路径从当前 agent workspace 解析；绝对路径必须在 agent/team workspace 内。'),
  },
  async ({ file_path }) => {
    const { extname, basename } = await import('path');
    const localPath = resolveLocalWorkspaceFile(file_path);
    const data = readFileSync(localPath).toString('base64');
    const filename = basename(localPath);
    const result = await api('POST', '/upload', { filename, data });
    return { content: [{ type: 'text', text: `临时公开 URL: ${result.url}\n注意：这不是正式产出存储。如需保留文件，请同时使用 write_workspace_file 保存到 artifacts/。` }] };
  }
);

// ── request_approval ──────────────────────────────────────────────────────────
server.tool('request_approval',
  'Request human approval before executing a sensitive platform action (posting, sending, publishing). Returns an action_id. After the human approves, call execute_approved_action with that ID.',
  {
    action_type:   z.string().describe('Action type, e.g. "x_post", "xhs_post", "email_send"'),
    platform:      z.string().describe('Target platform, e.g. "x", "xhs", "email"'),
    description:   z.string().describe('Human-readable summary of what will happen if approved'),
    payload:       z.record(z.any()).describe('Full action parameters (content, media_urls, etc.)'),
    credential_id: z.string().optional().describe('Which credential ID to use for execution'),
  },
  async ({ action_type, platform, description, payload, credential_id }) => {
    try {
      const data = await api('POST', '/actions/request', {
        action_type, platform, description,
        payload: JSON.stringify(payload),
        credential_id,
        team_id: currentTeamId,
      });
      return { content: [{ type: 'text', text:
        `Approval requested (action_id="${data.id}"). Waiting for human review.\n` +
        `After you receive an approval notification, call execute_approved_action(action_id="${data.id}").`
      }]};
    } catch (err) {
      return { isError: true, content: [{ type: 'text', text: `Error: ${err.message}` }] };
    }
  }
);

// ── execute_approved_action ───────────────────────────────────────────────────
server.tool('execute_approved_action',
  'Execute a previously approved action. Only call this after receiving an approval notification for the action.',
  {
    action_id: z.string().describe('The action_id returned by request_approval'),
  },
  async ({ action_id }) => {
    try {
      const data = await api('POST', `/actions/${action_id}/execute`, {});
      if (data.error) return { isError: true, content: [{ type: 'text', text: `Failed: ${data.error}` }] };
      return { content: [{ type: 'text', text:
        `Action approved. Now call the appropriate platform tool with approval_action_id="${action_id}" to actually perform the operation.\n` +
        `actionType=${data.actionType} platform=${data.platform}\n` +
        `Payload: ${JSON.stringify(data.payload)}\n\n` +
        `For xhs_post: call publish_content(platform="xhs", approval_action_id="${action_id}", ...) with the payload fields above.\n` +
        `For douyin_post: call publish_content(platform="douyin", approval_action_id="${action_id}", ...).\n` +
        `For x_post: call post_tweet(...).`
      }]};
    } catch (err) {
      return { isError: true, content: [{ type: 'text', text: `Error: ${err.message}` }] };
    }
  }
);

// ── start ─────────────────────────────────────────────────────────────────────
const transport = new StdioServerTransport();
await server.connect(transport);
console.error(`[chat-bridge] MCP Server started (agentId=${AGENT_ID})`);
