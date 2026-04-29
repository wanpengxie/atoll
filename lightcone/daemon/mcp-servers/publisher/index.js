#!/usr/bin/env node
/**
 * Publisher MCP Server
 * Exposes deterministic publishing tools for XHS, Douyin, Kuaishou.
 * Runs on the daemon machine; credentials (profile dirs) are injected via env.
 *
 * MCP Tools:
 *   publish_content        — publish to a platform
 *   get_platform_requirements — get content limits/specs for a platform+type
 *   check_login_status     — verify the browser profile is still logged in
 */
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { existsSync, statSync, realpathSync, mkdirSync, writeFileSync } from 'fs';
import path from 'path';
import { getSession, closeSession } from './chrome-pool.js';
import { XhsAdapter } from './adapters/xhs.js';
import { DouyinAdapter } from './adapters/douyin.js';
import { KuaishouAdapter } from './adapters/kuaishou.js';
import { withProfileLock } from '../../src/profile-lock.js';

const SERVER_URL      = process.env.SERVER_URL ?? '';
const MACHINE_API_KEY = process.env.MACHINE_API_KEY ?? '';
const AGENT_ID        = process.env.AGENT_ID ?? '';
const TEAM_ID         = process.env.TEAM_ID ?? '';
const WORKSPACE_DIR   = process.env.WORKSPACE_DIR ?? process.cwd();
const TEAM_WORKSPACE_DIR = path.dirname(WORKSPACE_DIR);

// ── Platform registry ──────────────────────────────────────────────────────────

const PLATFORM_ENV_KEYS = {
  xhs:      'XHS_PROFILE_DIR',
  douyin:   'DOUYIN_PROFILE_DIR',
  kuaishou: 'KUAISHOU_PROFILE_DIR',
};

const PLATFORM_LABELS = {
  xhs:      '小红书',
  douyin:   '抖音',
  kuaishou: '快手',
};

function getProfileDir(platform) {
  const key = PLATFORM_ENV_KEYS[platform];
  if (!key) return null;
  return process.env[key] ?? null;
}

async function getAdapter(platform) {
  const profileDir = getProfileDir(platform);
  if (!profileDir) {
    throw new Error(`No profile dir for platform="${platform}". Has the user logged in and authorized this agent?`);
  }
  const cdp = await getSession(platform, profileDir);
  switch (platform) {
    case 'xhs':      return new XhsAdapter(cdp);
    case 'douyin':   return new DouyinAdapter(cdp);
    case 'kuaishou': return new KuaishouAdapter(cdp);
    default: throw new Error(`Unknown platform: ${platform}`);
  }
}

async function withPublisherProfile(platform, fn) {
  const profileDir = getProfileDir(platform);
  if (!profileDir) {
    throw new Error(`No profile dir for platform="${platform}". Has the user logged in and authorized this agent?`);
  }
  return withProfileLock(platform, profileDir, {
    owner: `publisher:${platform}`,
    timeoutMs: 30_000,
    staleMs: 20 * 60 * 1000,
  }, fn);
}

async function api(method, path, body) {
  if (!SERVER_URL || !MACHINE_API_KEY || !AGENT_ID) {
    throw new Error('Publisher MCP is missing SERVER_URL, MACHINE_API_KEY, or AGENT_ID');
  }
  const res = await fetch(`${SERVER_URL}/internal/agent/${AGENT_ID}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${MACHINE_API_KEY}`,
    },
    body: body != null ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`lightcone API ${method} ${path} failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function validateApproval(actionId, platform) {
  if (!actionId) {
    throw new Error('approval_action_id is required. Request human approval first, then pass the approved action_id to publish_content.');
  }
  const data = await api('POST', `/actions/${encodeURIComponent(actionId)}/execute`, {});
  if (data.platform && data.platform !== platform) {
    throw new Error(`Approval platform mismatch: approved for "${data.platform}", requested "${platform}"`);
  }
  return data;
}

async function completeApproval(actionId, ok, result, error) {
  try {
    await api('POST', `/actions/${encodeURIComponent(actionId)}/complete`, { ok, result, error });
  } catch (err) {
    console.error(`[publisher] Failed to complete approval action ${actionId}: ${err.message}`);
  }
}

const MEDIA_LIMITS = {
  xhs:      { maxImages: 9,  imageExts: ['.jpg', '.jpeg', '.png', '.webp'], videoExts: ['.mp4', '.mov'] },
  douyin:   { maxImages: 35, imageExts: ['.jpg', '.jpeg', '.png', '.webp'], videoExts: ['.mp4', '.mov', '.webm'] },
  kuaishou: { maxImages: 12, imageExts: ['.jpg', '.jpeg', '.png'],         videoExts: ['.mp4', '.mov'] },
};

function isInsideDir(filePath, dir) {
  const rel = path.relative(dir, filePath);
  return rel === '' || (!!rel && !rel.startsWith('..') && !path.isAbsolute(rel));
}

function decodeWorkspaceContent(content) {
  if (typeof content !== 'string') throw new Error('workspace file content is not a string');
  if (!content.startsWith('data:')) return Buffer.from(content, 'utf8');

  const commaIdx = content.indexOf(',');
  if (commaIdx === -1) throw new Error('invalid data URL in workspace file');
  const header = content.slice(5, commaIdx);
  const body = content.slice(commaIdx + 1);
  if (header.split(';').includes('base64')) return Buffer.from(body, 'base64');
  return Buffer.from(decodeURIComponent(body), 'utf8');
}

function workspacePathFromMediaPath(filePath, approvalData) {
  if (!filePath) return null;

  const normalized = filePath.replaceAll('\\', '/');
  const virtualMatch = normalized.match(/^\/agent-workspace\/([^/]+)\/workspace\/(.+)$/);
  if (virtualMatch) return { teamId: virtualMatch[1], relPath: virtualMatch[2] };

  const workspaceSegmentMatch = normalized.match(/\/workspace\/((?:artifacts|notes|tmp)\/.+)$/);
  if (workspaceSegmentMatch) {
    return { teamId: approvalData?.teamId ?? TEAM_ID, relPath: workspaceSegmentMatch[1] };
  }

  if (!path.isAbsolute(filePath) && /^(artifacts|notes|tmp)\//.test(normalized)) {
    return { teamId: approvalData?.teamId ?? TEAM_ID, relPath: normalized };
  }

  return null;
}

async function materializeWorkspaceMedia(filePath, approvalData) {
  if (!filePath || existsSync(filePath)) return filePath;

  const workspacePath = workspacePathFromMediaPath(filePath, approvalData);
  if (!workspacePath?.teamId || !workspacePath.relPath) return filePath;

  const localPath = path.resolve(TEAM_WORKSPACE_DIR, workspacePath.relPath);
  const allowedRoots = ['artifacts', 'notes', 'tmp'].map(dir => path.join(TEAM_WORKSPACE_DIR, dir));
  if (!allowedRoots.some(root => isInsideDir(localPath, root))) {
    throw new Error(`workspace media path is outside allowed team workspace directories: ${filePath}`);
  }

  if (existsSync(localPath)) return localPath;

  const data = await api(
    'GET',
    `/team-memory?path=${encodeURIComponent(workspacePath.relPath)}&teamId=${encodeURIComponent(workspacePath.teamId)}`
  );
  mkdirSync(path.dirname(localPath), { recursive: true });
  writeFileSync(localPath, decodeWorkspaceContent(data.content));
  console.error(`[publisher] Materialized team workspace media ${workspacePath.relPath} -> ${localPath}`);
  return localPath;
}

async function materializeMedia({ images = [], video, cover }, approvalData) {
  return {
    images: await Promise.all(images.map(filePath => materializeWorkspaceMedia(filePath, approvalData))),
    video: video ? await materializeWorkspaceMedia(video, approvalData) : video,
    cover: cover ? await materializeWorkspaceMedia(cover, approvalData) : cover,
  };
}

function validateLocalFile(filePath, { kind, required = false, allowedExts = [] }) {
  if (!filePath) {
    if (required) throw new Error(`${kind} file path is required`);
    return null;
  }
  if (!path.isAbsolute(filePath)) {
    throw new Error(`${kind} path must be absolute: ${filePath}`);
  }
  if (!existsSync(filePath)) {
    throw new Error(`${kind} file does not exist: ${filePath}`);
  }

  const realFile = realpathSync(filePath);
  const allowedRoots = [
    realpathSync(WORKSPACE_DIR),
    ...['artifacts', 'notes', 'tmp']
      .map(dir => path.join(TEAM_WORKSPACE_DIR, dir))
      .filter(existsSync)
      .map(dir => realpathSync(dir)),
  ];
  if (!allowedRoots.some(root => isInsideDir(realFile, root))) {
    throw new Error(`${kind} file must be inside the agent workspace or team shared artifacts/notes/tmp directory: ${filePath}`);
  }

  const stat = statSync(realFile);
  if (!stat.isFile()) throw new Error(`${kind} path is not a file: ${filePath}`);
  const ext = path.extname(realFile).toLowerCase();
  if (allowedExts.length > 0 && !allowedExts.includes(ext)) {
    throw new Error(`${kind} file has unsupported extension "${ext}". Allowed: ${allowedExts.join(', ')}`);
  }
  return realFile;
}

function validateMedia({ platform, contentType, images = [], video, cover }) {
  const limits = MEDIA_LIMITS[platform] ?? MEDIA_LIMITS.xhs;
  if (contentType === 'image_text') {
    if (images.length === 0) {
      throw new Error(`image_text requires at least 1 image on ${platform}. Provide absolute image paths inside the agent workspace artifacts directory.`);
    }
    if (images.length > limits.maxImages) throw new Error(`image_text supports at most ${limits.maxImages} images on ${platform}`);
    return {
      images: images.map(filePath => validateLocalFile(filePath, { kind: 'image', required: true, allowedExts: limits.imageExts })),
      video: null,
      cover: null,
    };
  }
  return {
    images: [],
    video: validateLocalFile(video, { kind: 'video', required: true, allowedExts: limits.videoExts }),
    cover: cover ? validateLocalFile(cover, { kind: 'cover', allowedExts: limits.imageExts }) : null,
  };
}

// ── MCP Server ─────────────────────────────────────────────────────────────────

const server = new McpServer({
  name: 'publisher',
  version: '1.0.0',
});

// ── Tool: publish_content ──────────────────────────────────────────────────────

server.tool(
  'publish_content',
  `发布内容到指定平台。支持平台: xhs（小红书）、douyin（抖音）、kuaishou（快手）。
内容类型: image_text（图文）、short_video（短视频）。
images/video 字段填写本地绝对路径（在 agent workspace 目录下）。`,
  {
    platform: z.enum(['xhs', 'douyin', 'kuaishou']).describe('目标平台'),
    content_type: z.enum(['image_text', 'short_video']).describe('内容类型'),
    title: z.string().optional().describe('标题（部分平台必须，部分可选）'),
    text: z.string().describe('正文内容'),
    tags: z.array(z.string()).optional().describe('话题标签列表，不含 # 号'),
    images: z.array(z.string()).optional().describe('图片本地绝对路径列表'),
    video: z.string().optional().describe('视频本地绝对路径'),
    cover: z.string().optional().describe('封面图本地绝对路径（视频用，可选）'),
    approval_action_id: z.string().describe('已由人类批准的 action_id。必须先调用 request_approval，并在收到批准通知后传入该 ID。'),
  },
  async ({ platform, content_type, title, text, tags, images, video, cover, approval_action_id }) => {
    const label = PLATFORM_LABELS[platform] ?? platform;
    try {
      const approvalData = await validateApproval(approval_action_id, platform);
      const localMedia = await materializeMedia({ images: images ?? [], video, cover }, approvalData);
      const media = validateMedia({ platform, contentType: content_type, ...localMedia });
      const result = await withPublisherProfile(platform, async () => {
        const adapter = await getAdapter(platform);
        if (content_type === 'image_text') {
          return adapter.publishImageText({ title, text, tags: tags ?? [], images: media.images });
        }
        if (content_type === 'short_video') {
          return adapter.publishShortVideo({ title, text, tags: tags ?? [], video: media.video, cover: media.cover });
        }
        throw new Error(`Unsupported content_type: ${content_type}`);
      });

      await completeApproval(approval_action_id, true, result, null);
      if (result?.dry_run) {
        return {
          content: [{ type: 'text', text: `✓ ${label}发布流程已完成到发布前一步，已跳过最终“发布”点击。` }],
        };
      }
      const postUrl = result.post_url ? `\n发布链接: ${result.post_url}` : '';
      return {
        content: [{ type: 'text', text: `✓ 已成功发布到${label}。${postUrl}` }],
      };
    } catch (err) {
      if (err.message?.startsWith('LOGIN_EXPIRED')) {
        closeSession(platform);
        await completeApproval(approval_action_id, false, null, err.message);
        return {
          content: [{ type: 'text', text: `登录已过期，请在「连接外部账号」中重新扫码连接${label}，然后重试。` }],
          isError: true,
        };
      }
      if (approval_action_id) await completeApproval(approval_action_id, false, null, err.message);
      return {
        content: [{ type: 'text', text: `发布失败: ${err.message}` }],
        isError: true,
      };
    }
  }
);

// ── Tool: get_platform_requirements ───────────────────────────────────────────

server.tool(
  'get_platform_requirements',
  '获取指定平台和内容类型的规格要求（字数上限、图片格式、视频规格等）。发布前先调用此工具确认内容符合规范。',
  {
    platform: z.enum(['xhs', 'douyin', 'kuaishou']).describe('目标平台'),
    content_type: z.enum(['image_text', 'short_video']).describe('内容类型'),
  },
  async ({ platform, content_type }) => {
    try {
      let adapter;
      switch (platform) {
        case 'xhs':      adapter = new XhsAdapter(null); break;
        case 'douyin':   adapter = new DouyinAdapter(null); break;
        case 'kuaishou': adapter = new KuaishouAdapter(null); break;
        default: throw new Error(`Unknown platform: ${platform}`);
      }
      const req = adapter.getRequirements(content_type);
      if (!req) return { content: [{ type: 'text', text: `该平台不支持 ${content_type} 类型` }] };
      return { content: [{ type: 'text', text: JSON.stringify(req, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text', text: `Error: ${err.message}` }], isError: true };
    }
  }
);

// ── Tool: check_login_status ───────────────────────────────────────────────────

server.tool(
  'check_login_status',
  '检查指定平台的浏览器 Profile 是否仍处于登录状态。如果未登录，提示用户重新扫码连接。',
  {
    platform: z.enum(['xhs', 'douyin', 'kuaishou']).describe('目标平台'),
  },
  async ({ platform }) => {
    const label = PLATFORM_LABELS[platform] ?? platform;
    try {
      const { loggedIn, url, profileDir } = await withPublisherProfile(platform, async () => {
        const adapter = await getAdapter(platform);
        return adapter.checkLoginStatus();
      });
      if (loggedIn) {
        return { content: [{ type: 'text', text: `✓ ${label} 已登录，可以发布内容。` }] };
      } else {
        closeSession(platform);
        return {
          content: [{ type: 'text', text: `${label} 未登录或登录已过期。\n调试信息: url=${url}\nprofileDir=${profileDir}\n请在「连接外部账号」中重新扫码连接。` }],
          isError: true,
        };
      }
    } catch (err) {
      return { content: [{ type: 'text', text: `检查失败: ${err.message}` }], isError: true };
    }
  }
);

// ── Start ──────────────────────────────────────────────────────────────────────

const transport = new StdioServerTransport();
await server.connect(transport);
