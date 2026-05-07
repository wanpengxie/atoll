import { XIAOHONGSHU_URLS } from 'xiaohongshu-mcp-shared';

export type XhsContentKind = 'note' | 'profile';

export function safeParseUrl(input: string): URL | null {
  try {
    return new URL((input || '').trim());
  } catch {
    return null;
  }
}

export function isXhsHost(hostname: string): boolean {
  return hostname === 'xiaohongshu.com' || hostname.endsWith('.xiaohongshu.com');
}

export function isXhsNotePath(pathname: string): boolean {
  return pathname.startsWith('/explore/') || pathname.startsWith('/discovery/item/');
}

export function isXhsProfilePath(pathname: string): boolean {
  return pathname.startsWith('/user/profile/');
}

export function getXsecToken(u: URL): string {
  return (u.searchParams.get('xsec_token') || '').trim();
}

export function validateXhsNoteUrl(noteUrl: string): { ok: true; parsed: URL } | { ok: false; error: string } {
  const u = safeParseUrl(noteUrl);
  if (!u) return { ok: false, error: 'noteUrl 格式不正确' };
  if (!isXhsHost(u.hostname)) return { ok: false, error: 'noteUrl 必须是小红书的 URL' };
  if (!isXhsNotePath(u.pathname)) return { ok: false, error: 'noteUrl 必须是小红书笔记链接（/explore/...）' };
  if (!getXsecToken(u)) {
    return { ok: false, error: 'noteUrl 必须包含 xsec_token（请从小红书内“分享-复制链接”获取完整链接）' };
  }
  return { ok: true, parsed: u };
}

export function validateXhsProfileUrl(profileUrl: string): { ok: true; parsed: URL } | { ok: false; error: string } {
  const u = safeParseUrl(profileUrl);
  if (!u) return { ok: false, error: 'url 格式不正确' };
  if (!isXhsHost(u.hostname)) return { ok: false, error: 'url 必须是小红书的 URL' };
  if (!isXhsProfilePath(u.pathname)) return { ok: false, error: 'url 必须是小红书用户主页链接（/user/profile/...）' };
  if (!getXsecToken(u)) {
    return { ok: false, error: 'url 必须包含 xsec_token（请从小红书内“分享-复制链接”获取完整链接）' };
  }
  return { ok: true, parsed: u };
}

export function buildXhsExploreNoteUrl(noteId: string, xsecToken: string, xsecSource: string = 'pc_search'): string {
  const id = (noteId || '').trim();
  const token = (xsecToken || '').trim();
  if (!id || !token) return '';

  const u = new URL(`${XIAOHONGSHU_URLS.HOME}/explore/${encodeURIComponent(id)}`);
  u.searchParams.set('xsec_token', token);
  if (xsecSource) {
    u.searchParams.set('xsec_source', xsecSource);
  }
  return u.toString();
}

