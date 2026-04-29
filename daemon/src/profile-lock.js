import { mkdirSync, rmSync, writeFileSync, readFileSync, existsSync, statSync } from 'fs';
import { homedir } from 'os';
import path from 'path';
import { randomUUID } from 'crypto';

const LOCK_ROOT = path.join(homedir(), '.lightcone', 'profile-locks');
const DEFAULT_STALE_MS = 20 * 60 * 1000;
const DEFAULT_TIMEOUT_MS = 5 * 60 * 1000;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function sanitize(value) {
  return String(value || 'unknown').replace(/[^a-zA-Z0-9_.-]/g, '_');
}

function lockDir(platform, profileDir) {
  const basename = path.basename(profileDir || 'default');
  return path.join(LOCK_ROOT, `${sanitize(platform)}-${sanitize(basename)}.lock`);
}

function lockInfo(dir) {
  try {
    return JSON.parse(readFileSync(path.join(dir, 'owner.json'), 'utf8'));
  } catch {
    return null;
  }
}

function isStale(dir, staleMs) {
  try {
    const info = lockInfo(dir);
    const createdAt = info?.createdAt ? Date.parse(info.createdAt) : statSync(dir).mtimeMs;
    return Number.isFinite(createdAt) && Date.now() - createdAt > staleMs;
  } catch {
    return true;
  }
}

function pidIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === 'EPERM';
  }
}

function isAbandoned(dir) {
  const info = lockInfo(dir);
  if (!info?.pid) return false;
  return !pidIsAlive(info.pid);
}

function removeLockDir(dir) {
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

const heldLocks = new Map();

export async function acquireProfileLock(platform, profileDir, {
  owner = 'unknown',
  timeoutMs = DEFAULT_TIMEOUT_MS,
  staleMs = DEFAULT_STALE_MS,
  pollMs = 500,
} = {}) {
  mkdirSync(LOCK_ROOT, { recursive: true });
  const dir = lockDir(platform, profileDir);
  const deadline = Date.now() + timeoutMs;

  while (true) {
    try {
      mkdirSync(dir);
      const token = randomUUID();
      const release = () => {
        const info = lockInfo(dir);
        heldLocks.delete(dir);
        if (!info || info.token === token || info.pid === process.pid) removeLockDir(dir);
      };
      writeFileSync(path.join(dir, 'owner.json'), JSON.stringify({
        platform,
        profileDir,
        owner,
        pid: process.pid,
        token,
        createdAt: new Date().toISOString(),
      }, null, 2));
      heldLocks.set(dir, token);
      return { release, dir };
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      if (existsSync(dir) && (isAbandoned(dir) || isStale(dir, staleMs))) {
        removeLockDir(dir);
        continue;
      }
      if (Date.now() >= deadline) {
        const info = lockInfo(dir);
        throw new Error(`Profile is busy for platform="${platform}" owner="${info?.owner ?? 'unknown'}". Retry after the current login or publish operation finishes.`);
      }
      await sleep(pollMs);
    }
  }
}

export function releaseProfileLocksForProcess() {
  for (const [dir, token] of heldLocks) {
    const info = lockInfo(dir);
    if (!info || info.token === token || info.pid === process.pid) removeLockDir(dir);
    heldLocks.delete(dir);
  }
}

export async function withProfileLock(platform, profileDir, options, fn) {
  const lock = await acquireProfileLock(platform, profileDir, options);
  try {
    return await fn();
  } finally {
    lock.release();
  }
}
