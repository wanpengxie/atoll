#!/usr/bin/env node
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

const SERVER_URL      = process.env.PLATFORM_SERVER_URL ?? process.env.SERVER_URL ?? 'http://localhost:3001';
const MACHINE_API_KEY = process.env.PLATFORM_MACHINE_API_KEY ?? process.env.MACHINE_API_KEY ?? '';
const AGENT_ID        = process.env.PLATFORM_AGENT_ID ?? process.env.AGENT_ID ?? '';

// ── Platform adapter registry ─────────────────────────────────────────────────
const ADAPTERS = {
  x: () => import('./adapters/x.js'),
};

async function getAdapter(platform) {
  const loader = ADAPTERS[platform];
  if (!loader) throw new Error(`Unsupported platform: '${platform}'. Supported: ${Object.keys(ADAPTERS).join(', ')}`);
  return loader();
}

// ── Rate limit check via server ───────────────────────────────────────────────
async function checkRateLimit(platform, credentialId) {
  if (!SERVER_URL || !MACHINE_API_KEY || !AGENT_ID) return; // skip if not configured
  try {
    const res = await fetch(`${SERVER_URL}/internal/agent/${AGENT_ID}/rate-check`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${MACHINE_API_KEY}`,
      },
      body: JSON.stringify({ platform, credentialId, windowMinutes: 15, maxActions: 10 }),
    });
    if (res.ok) {
      const data = await res.json();
      if (!data.allowed) throw new Error(`Rate limit exceeded for platform '${platform}'. Retry after ${data.retry_after}s.`);
    }
  } catch (err) {
    if (err.message.startsWith('Rate limit')) throw err;
    // Network error → allow (fail open for rate limiting)
  }
}

// ── Log action to server ──────────────────────────────────────────────────────
async function logAction(platform, actionType, credentialId, payload, result, status, error) {
  if (!SERVER_URL || !MACHINE_API_KEY || !AGENT_ID) return;
  try {
    await fetch(`${SERVER_URL}/internal/agent/${AGENT_ID}/action-log`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${MACHINE_API_KEY}`,
      },
      body: JSON.stringify({ platform, actionType, credentialId, payload, result, status, error }),
    });
  } catch { /* non-fatal */ }
}

const server = new McpServer({ name: 'platform-actions', version: '0.1.0' });

// ── platform_post ─────────────────────────────────────────────────────────────
server.tool('platform_post',
  'Publish a post to a social media platform. Only call this after execute_approved_action has confirmed the action is approved.',
  {
    platform:      z.string().describe('Target platform: "x", "xhs", "youtube", etc.'),
    content:       z.string().describe('Post content / text body'),
    media_urls:    z.array(z.string()).optional().describe('Optional list of media URLs to attach'),
    credential_id: z.string().optional().describe('Which credential to use (for audit logging)'),
  },
  async ({ platform, content, media_urls, credential_id }) => {
    try {
      await checkRateLimit(platform, credential_id ?? '');
      const adapter = await getAdapter(platform);
      const result = await adapter.post({ content, media_urls: media_urls ?? [] });
      await logAction(platform, 'post', credential_id ?? '', { content }, result, 'ok', null);
      return { content: [{ type: 'text', text: `Posted successfully.\n${JSON.stringify(result)}` }] };
    } catch (err) {
      await logAction(platform, 'post', credential_id ?? '', { content }, null, 'error', err.message);
      return { isError: true, content: [{ type: 'text', text: `Error: ${err.message}` }] };
    }
  }
);

// ── platform_get_account ──────────────────────────────────────────────────────
server.tool('platform_get_account',
  'Verify credentials and get account info for a platform.',
  {
    platform: z.string().describe('Platform to check: "x", "xhs", etc.'),
  },
  async ({ platform }) => {
    try {
      const adapter = await getAdapter(platform);
      const info = await adapter.getAccount();
      return { content: [{ type: 'text', text: `Account info: ${JSON.stringify(info)}` }] };
    } catch (err) {
      return { isError: true, content: [{ type: 'text', text: `Error: ${err.message}` }] };
    }
  }
);

// ── start ─────────────────────────────────────────────────────────────────────
const transport = new StdioServerTransport();
await server.connect(transport);
console.error(`[platform-actions] MCP Server started (agentId=${AGENT_ID})`);
