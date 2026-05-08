// Vitest unit tests for FX2 (round-2 codex#t56.2): get-note arg-resolution
// 三层合同对齐 — extension 端在 note_id 缺失时从 url 解析 note_id +
// xsec_token，与 daemon validator + CLI real_provider 的契约对齐。
//
// 仅覆盖：URL 解析纯函数 + execute() 的 arg 早期校验路径。页面侧 __INITIAL_STATE__
// 注入路径不在本测试范围（依赖真实 chrome.tabs / runInMainWorld）。

import { describe, it, expect, vi } from 'vitest';

vi.mock('xiaohongshu-mcp-shared', () => ({
  XIAOHONGSHU_TOOL_NAMES: {
    GET_NOTE: 'xhs_get_note',
  },
  XIAOHONGSHU_URLS: {
    HOME: 'https://www.xiaohongshu.com',
  },
  ERROR_MESSAGES: {
    INVALID_ARGUMENTS: '参数无效',
  },
}));

// BaseTool 在真实代码里包含 captureTabSessionSnapshot / findOrCreateXHSTab /
// cleanupToolTabSession / createErrorResult 等 helper。本测试只触发早期 arg
// 校验路径，所以只 stub createErrorResult 即可——其它方法不会被调用。
vi.mock('../base-tool', () => ({
  BaseTool: class {
    createErrorResult(message: string) {
      return {
        content: [{ type: 'text', text: JSON.stringify({ success: false, error: message }) }],
        isError: true,
      };
    }
  },
}));

vi.mock('../inject-script', () => ({
  runInMainWorld: vi.fn(),
}));

vi.mock('../wait-utils', () => ({
  waitForLoadState: vi.fn(),
}));

import {
  XhsGetNoteTool,
  XHS_NOTE_ID_FROM_URL_PATTERN,
  parseNoteIdentityFromUrl,
} from './get-note';

function extractError(result: any): string {
  const text = result?.content?.[0]?.text ?? '';
  try {
    const parsed = JSON.parse(text);
    return parsed.error ?? '';
  } catch {
    return text;
  }
}

describe('XHS_NOTE_ID_FROM_URL_PATTERN', () => {
  it('matches /explore/<id>', () => {
    const id = '0123456789abcdef01234567';
    const m = `https://www.xiaohongshu.com/explore/${id}?xsec_token=abc`.match(
      XHS_NOTE_ID_FROM_URL_PATTERN,
    );
    expect(m?.[1]).toBe(id);
  });

  it('matches /discovery/item/<id>', () => {
    const id = 'abcdef0123456789abcdef01';
    const m = `https://www.xiaohongshu.com/discovery/item/${id}?xsec_token=tok`.match(
      XHS_NOTE_ID_FROM_URL_PATTERN,
    );
    expect(m?.[1]).toBe(id);
  });

  it('matches bare /item/<id> (短链/分享场景)', () => {
    const id = 'aaaa1111bbbb2222cccc3333';
    const m = `https://example.com/item/${id}`.match(XHS_NOTE_ID_FROM_URL_PATTERN);
    expect(m?.[1]).toBe(id);
  });

  it('does not match unrelated paths', () => {
    expect('https://www.xiaohongshu.com/user/profile/abc'.match(XHS_NOTE_ID_FROM_URL_PATTERN)).toBeNull();
    expect('https://creator.xiaohongshu.com/publish/publish'.match(XHS_NOTE_ID_FROM_URL_PATTERN)).toBeNull();
  });
});

describe('parseNoteIdentityFromUrl', () => {
  it('extracts both note_id and xsec_token from a full /explore URL', () => {
    const got = parseNoteIdentityFromUrl(
      'https://www.xiaohongshu.com/explore/0123456789abcdef01234567?xsec_token=tk1&xsec_source=pc_search',
    );
    expect(got).toEqual({ noteId: '0123456789abcdef01234567', xsecToken: 'tk1' });
  });

  it('extracts xsec_token only when path has no note_id', () => {
    const got = parseNoteIdentityFromUrl(
      'https://www.xiaohongshu.com/user/profile/u1?xsec_token=tk2',
    );
    expect(got).toEqual({ noteId: '', xsecToken: 'tk2' });
  });

  it('extracts note_id only when URL has no xsec_token', () => {
    const got = parseNoteIdentityFromUrl(
      'https://www.xiaohongshu.com/discovery/item/abcdef0123456789abcdef01',
    );
    expect(got).toEqual({ noteId: 'abcdef0123456789abcdef01', xsecToken: '' });
  });

  it('returns null when URL is empty', () => {
    expect(parseNoteIdentityFromUrl('')).toBeNull();
    expect(parseNoteIdentityFromUrl('   ')).toBeNull();
  });

  it('returns null when URL is unparseable + has no useful info', () => {
    expect(parseNoteIdentityFromUrl('not a url')).toBeNull();
  });
});

describe('XhsGetNoteTool.execute (early-fail paths)', () => {
  it('rejects when all args empty', async () => {
    const tool = new XhsGetNoteTool();
    const result = await tool.execute({} as any);
    expect(result.isError).toBe(true);
    expect(extractError(result)).toMatch(/note url|note_id|xsec_token/);
  });

  it('rejects when only xsec_token given (no note_id, no url)', async () => {
    const tool = new XhsGetNoteTool();
    const result = await tool.execute({ xsec_token: 'tk' });
    expect(result.isError).toBe(true);
    expect(extractError(result)).toMatch(/note url 或/);
  });

  it('rejects when only note_id given (no token, no url)', async () => {
    const tool = new XhsGetNoteTool();
    const result = await tool.execute({ note_id: 'n1' });
    expect(result.isError).toBe(true);
    // note_id alone 走到 "没给 URL → 必须有 note_id + xsec_token" 分支
    expect(extractError(result)).toMatch(/note url 或.*note_id.*xsec_token|无 token/);
  });

  it('rejects when URL provided but note_id cannot be parsed (path 不是 note 详情)', async () => {
    const tool = new XhsGetNoteTool();
    // /user/profile 不会匹配 note_id 正则，也没给 args.note_id；url 有 token
    // 但缺 note_id → 走"无法从 URL 解析 note_id"分支
    const result = await tool.execute({
      url: 'https://www.xiaohongshu.com/user/profile/u1?xsec_token=tk',
    });
    expect(result.isError).toBe(true);
    expect(extractError(result)).toMatch(/无法从 URL 解析 note_id/);
  });

  it('parses note_id + xsec_token from URL alone (FX2 主路径) — 通过早期校验进入 validateXhsNoteUrl', async () => {
    // URL 完整含 note_id + xsec_token → 早期校验通过 → 后续 validateXhsNoteUrl
    // 也通过（host + path + token 都正确）→ 进入 captureTabSessionSnapshot 阶段。
    // 由于我们 stub 的 BaseTool 不实现 captureTabSessionSnapshot，会抛 TypeError；
    // 用 try/catch 隔离，仅断言"早期 arg 校验未拒绝"。
    const tool = new XhsGetNoteTool();
    let earlyRejected: any = null;
    try {
      await tool.execute({
        url: 'https://www.xiaohongshu.com/explore/0123456789abcdef01234567?xsec_token=tk1',
      });
    } catch (err) {
      // 预期：BaseTool stub 不实现 captureTabSessionSnapshot，抛 TypeError —
      // 这正好证明执行已穿越早期 arg 校验。
      earlyRejected = err;
    }
    // 不应是 createErrorResult 的早期 isError 返回；要么穿透到下游（throw），
    // 要么穿透到 validateXhsNoteUrl 之外。
    expect(earlyRejected).toBeTruthy();
    expect(String(earlyRejected?.message ?? earlyRejected)).not.toMatch(
      /参数无效|note url 或|无法从 URL 解析/,
    );
  });

  it('parses note_id from URL when args.note_id missing (regression for FX2)', async () => {
    // 仅给 url，args.note_id 留空 → parseNoteIdentityFromUrl 应抽出 note_id
    // → execute 不应在 arg 校验阶段拒绝。
    const tool = new XhsGetNoteTool();
    let earlyRejected: any = null;
    try {
      await tool.execute({
        url: 'https://www.xiaohongshu.com/discovery/item/abcdef0123456789abcdef01?xsec_token=tk2',
      });
    } catch (err) {
      earlyRejected = err;
    }
    expect(earlyRejected).toBeTruthy();
    expect(String(earlyRejected?.message ?? earlyRejected)).not.toMatch(
      /note_id is required|无法从 URL 解析/,
    );
  });
});
