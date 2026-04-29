#!/usr/bin/env node
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { readdirSync, readFileSync, statSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';

const SERVER_URL      = process.env.SERVER_URL || '';
const MACHINE_API_KEY = process.env.MACHINE_API_KEY || '';
const AGENT_ID        = process.env.AGENT_ID || '';

async function apiPut(path, body) {
  const url = `${SERVER_URL}/internal/agent/${AGENT_ID}${path}`;
  const res = await fetch(url, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${MACHINE_API_KEY}`,
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API PUT ${path} → ${res.status}: ${text}`);
  }
  return res.json();
}

function scanDir(dir, base = '') {
  const results = [];
  let entries;
  try { entries = readdirSync(dir, { withFileTypes: true }); }
  catch { return results; }
  for (const entry of entries) {
    if (entry.name.startsWith('.')) continue;
    const rel = base ? `${base}/${entry.name}` : entry.name;
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...scanDir(full, rel));
    } else {
      results.push({ fullPath: full, relPath: rel });
    }
  }
  return results;
}

const server = new McpServer({ name: 'workspace-migrate', version: '0.1.0' });

server.tool('migrate_workspace', 'Migrate local workspace files to the server database for cross-machine access', {}, async () => {
  const workspaceRoot = join(homedir(), '.lightcone', 'workspace');

  let teamDirs;
  try { teamDirs = readdirSync(workspaceRoot, { withFileTypes: true }).filter(e => e.isDirectory()); }
  catch { return { content: [{ type: 'text', text: `No local workspace found at ${workspaceRoot}` }] }; }

  let totalTeams = 0;
  let totalFiles = 0;
  let errors = 0;
  const summary = [];

  for (const teamDir of teamDirs) {
    const teamId = teamDir.name;
    // skip agent subdirs — only scan team-level files (not inside agentId subdirs)
    const teamPath = join(workspaceRoot, teamId);
    const files = scanDir(teamPath).filter(f => {
      // exclude agent workspace subdirectories (36-char UUID pattern)
      const topLevel = f.relPath.split('/')[0];
      return !/^[0-9a-f-]{36}$/.test(topLevel);
    });

    if (files.length === 0) continue;

    totalTeams++;
    let teamUploaded = 0;

    for (const file of files) {
      try {
        const content = readFileSync(file.fullPath, 'utf8');
        await apiPut(`/team-memory?path=${encodeURIComponent(file.relPath)}&teamId=${encodeURIComponent(teamId)}`, { content });
        teamUploaded++;
        totalFiles++;
      } catch (err) {
        errors++;
        console.error(`[migrate] Failed ${file.fullPath}: ${err.message}`);
      }
    }

    summary.push(`  ${teamId}: ${teamUploaded} files`);
  }

  const report = [
    `Migration complete.`,
    `Teams: ${totalTeams}, Files: ${totalFiles}, Errors: ${errors}`,
    ...(summary.length ? ['', 'Details:', ...summary] : []),
  ].join('\n');

  return { content: [{ type: 'text', text: report }] };
});

const transport = new StdioServerTransport();
await server.connect(transport);
