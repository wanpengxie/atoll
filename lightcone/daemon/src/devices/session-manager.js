// SessionManager — per-user xhs session 持久化（M1.1-T3 §7.1.1）。
//
// 文件布局：<baseDir>/{user_id}/xhs-session.json
// 默认 baseDir = `<coagentProjectDir(projectKey)>/users`（见 paths.js）。
//
// 写入策略：write-tmp + rename 原子替换；mkdir -p 父目录；userId 必须正则白名单
// 防止 path traversal。
//
// 公开 API（与 spec §4.4 align）：
//   getSession(userId)            → object | null
//   updateSession(userId, patch)  → 持久化后的完整 session
//   deleteSession(userId)         → boolean (true = 删除了文件，false = 不存在)
//
// session shape:
//   {
//     user_id: string,
//     cookies: array,
//     login_state: 'logged_in' | 'expired' | 'unknown' | string,
//     last_updated_at: number (epoch ms),
//     expires_at: number | null
//   }

import path from 'node:path';
import { chmodSync, existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { coagentProjectDir, normalizeProjectKey } from '../paths.js';

const USER_ID_PATTERN = /^[A-Za-z0-9_.-]{1,64}$/;
const SESSION_FILE = 'xhs-session.json';

export function defaultSessionBaseDir(projectKey = normalizeProjectKey()) {
  return path.join(coagentProjectDir(projectKey), 'users');
}

export class SessionManager {
  /**
   * @param {object} opts
   * @param {string} [opts.baseDir] override base dir; defaults to `<coagentProjectDir>/users`.
   * @param {string} [opts.projectKey] used to compute defaultBaseDir if baseDir omitted.
   */
  constructor({ baseDir, projectKey } = {}) {
    this.baseDir = baseDir ?? defaultSessionBaseDir(projectKey);
  }

  static assertUserId(userId) {
    const value = String(userId ?? '').trim();
    // Pattern allows alnum + `_`/`.`/`-`. Reject `.`/`..` literally so user_id
    // can't resolve to a parent directory; also reject leading `-` (CLI flag-ish).
    const looksLikeRelativePath = value === '.' || value === '..';
    if (!USER_ID_PATTERN.test(value) || looksLikeRelativePath) {
      const err = new Error(`invalid user_id: ${JSON.stringify(userId)} (must match ${USER_ID_PATTERN})`);
      err.code = 'bad_request';
      err.statusCode = 400;
      throw err;
    }
    return value;
  }

  userDir(userId) {
    const safe = SessionManager.assertUserId(userId);
    return path.join(this.baseDir, safe);
  }

  sessionPath(userId) {
    return path.join(this.userDir(userId), SESSION_FILE);
  }

  /** Read session file; returns null when missing. Malformed JSON → throw bad_request. */
  getSession(userId) {
    const filePath = this.sessionPath(userId);
    if (!existsSync(filePath)) return null;
    let raw;
    try {
      raw = readFileSync(filePath, 'utf8');
    } catch (err) {
      const error = new Error(`read session failed for ${userId}: ${err.message}`);
      error.code = 'session_read_failed';
      error.statusCode = 500;
      throw error;
    }
    if (!raw.trim()) return null;
    try {
      return JSON.parse(raw);
    } catch (err) {
      const error = new Error(`session file corrupted for ${userId}: ${err.message}`);
      error.code = 'session_corrupted';
      error.statusCode = 500;
      throw error;
    }
  }

  /**
   * Atomic merge-and-write. Patch fields shallow-overwrite existing session.
   * `last_updated_at` 总是被服务端覆盖为当前时间。
   *
   * @param {string} userId
   * @param {object} patch
   * @returns {object} merged session
   */
  updateSession(userId, patch = {}) {
    const safeUserId = SessionManager.assertUserId(userId);
    const existing = this.getSession(safeUserId) ?? {};
    const merged = {
      ...existing,
      ...patch,
      user_id: safeUserId,
      last_updated_at: Date.now(),
    };
    if (merged.cookies != null && !Array.isArray(merged.cookies)) {
      const err = new Error('session cookies must be an array');
      err.code = 'bad_request';
      err.statusCode = 400;
      throw err;
    }
    if (merged.expires_at !== undefined && merged.expires_at !== null) {
      const numeric = Number(merged.expires_at);
      if (!Number.isFinite(numeric)) {
        const err = new Error('session expires_at must be a finite number when provided');
        err.code = 'bad_request';
        err.statusCode = 400;
        throw err;
      }
      merged.expires_at = numeric;
    }
    if (merged.login_state == null) merged.login_state = 'unknown';

    // Restrict baseDir + per-user dirs to owner only — these files contain
    // xhs cookies and login state. Fix-T2 §7 / round-1 review claude-M6.
    // mkdirSync's `mode` option only takes effect when the directory is
    // created; pre-existing dirs (e.g. left by an older release at 0o755)
    // keep their original mode. Follow up with chmodSync so upgrade paths
    // are pinned to 0o700. Round-2 review codex-#t57.2 / FX4 / R3-T2.
    const dir = this.userDir(safeUserId);
    mkdirSync(this.baseDir, { recursive: true, mode: 0o700 });
    chmodSync(this.baseDir, 0o700);
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    chmodSync(dir, 0o700);
    const finalPath = this.sessionPath(safeUserId);
    const tmpPath = `${finalPath}.tmp-${process.pid}-${randomBytes(4).toString('hex')}`;
    const json = JSON.stringify(merged, null, 2);
    try {
      writeFileSync(tmpPath, json, { encoding: 'utf8', mode: 0o600 });
      renameSync(tmpPath, finalPath);
      // Defend against process umask interfering with the explicit 0o600
      // mode passed to writeFileSync; rename preserves source mode but
      // an explicit chmod keeps the contract auditable on every write.
      chmodSync(finalPath, 0o600);
    } catch (err) {
      try { if (existsSync(tmpPath)) unlinkSync(tmpPath); } catch {}
      const error = new Error(`write session failed for ${safeUserId}: ${err.message}`);
      error.code = 'session_write_failed';
      error.statusCode = 500;
      throw error;
    }
    return merged;
  }

  /** Remove session file; returns true if removed, false if not present. */
  deleteSession(userId) {
    const filePath = this.sessionPath(userId);
    if (!existsSync(filePath)) return false;
    try {
      unlinkSync(filePath);
      return true;
    } catch (err) {
      if (err && err.code === 'ENOENT') return false;
      const error = new Error(`delete session failed for ${userId}: ${err.message}`);
      error.code = 'session_delete_failed';
      error.statusCode = 500;
      throw error;
    }
  }
}
