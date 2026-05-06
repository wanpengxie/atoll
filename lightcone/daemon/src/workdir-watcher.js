import { existsSync, readdirSync, statSync, watch } from 'fs';
import path from 'path';
import { nowIso } from './time.js';

function toPosixPath(value) {
  return value.split(path.sep).join('/');
}

function safeStat(filePath) {
  try {
    return statSync(filePath);
  } catch {
    return null;
  }
}

function walkDirectories(root) {
  if (!existsSync(root)) return [];
  const dirs = [root];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    dirs.push(...walkDirectories(path.join(root, entry.name)));
  }
  return dirs;
}

const WATCHED_DIRECTORIES = ['artifacts', 'notes', 'schedules'];

function isIgnoredPath(relativePath) {
  if (!relativePath.startsWith('agents/')) return false;

  const segments = relativePath.split('/');
  const agentPathSegments = segments.slice(2);
  return agentPathSegments.some((segment) => (
    segment === 'trace'
    || segment === 'cursor.json'
    || segment.startsWith('session')
  ));
}

function isWatchableDirectory(relativePath) {
  return WATCHED_DIRECTORIES.some((topLevel) => (
    relativePath === topLevel || relativePath.startsWith(`${topLevel}/`)
  ));
}

export class WorkdirWatcher {
  constructor({ workdir, onEvent, debounceMs = 50 }) {
    this.workdir = workdir;
    this.onEvent = onEvent;
    this.debounceMs = debounceMs;
    this.watchers = new Map();
    this.pending = new Map();
  }

  start() {
    this._watchPath(path.join(this.workdir, 'channel.yaml'));
    for (const topLevel of WATCHED_DIRECTORIES) {
      for (const dir of walkDirectories(path.join(this.workdir, topLevel))) {
        this._watchPath(dir);
      }
    }
  }

  stop() {
    for (const watcher of this.watchers.values()) {
      watcher.close();
    }
    this.watchers.clear();
    for (const timer of this.pending.values()) {
      clearTimeout(timer);
    }
    this.pending.clear();
  }

  handleChange(filePath, eventType = 'change') {
    const absolutePath = path.isAbsolute(filePath) ? filePath : path.join(this.workdir, filePath);
    const relativePath = toPosixPath(path.relative(this.workdir, absolutePath));
    if (!relativePath || relativePath.startsWith('..')) return null;
    if (isIgnoredPath(relativePath)) return null;

    const stat = safeStat(absolutePath);
    if (stat?.isDirectory() && isWatchableDirectory(relativePath)) {
      this._watchPath(absolutePath);
    }

    if (relativePath === 'channel.yaml') {
      return this._emit({
        type: 'channel.config.updated',
        source: 'workdir-watcher',
        created_at: nowIso(),
        payload: {
          op: eventType,
          path: relativePath,
          size: stat?.size ?? null,
          mtime_ms: stat?.mtimeMs ?? null,
        },
      });
    }

    if (
      relativePath.startsWith('artifacts/')
      || relativePath.startsWith('notes/')
      || relativePath.startsWith('schedules/')
      || relativePath.startsWith('agents/')
    ) {
      return this._emit({
        type: 'workdir.changed',
        source: 'workdir-watcher',
        created_at: nowIso(),
        payload: {
          op: eventType,
          path: relativePath,
          size: stat?.size ?? null,
          mtime_ms: stat?.mtimeMs ?? null,
        },
      });
    }

    return null;
  }

  _watchPath(targetPath) {
    const stat = safeStat(targetPath);
    if (!stat || this.watchers.has(targetPath)) return;
    const isDirectory = stat.isDirectory();
    const watcher = watch(targetPath, { persistent: false }, (eventType, fileName) => {
      if (!fileName && isDirectory) return;
      const changedPath = isDirectory ? path.join(targetPath, String(fileName)) : targetPath;
      const key = `${eventType}:${changedPath}`;
      if (this.pending.has(key)) clearTimeout(this.pending.get(key));
      this.pending.set(key, setTimeout(() => {
        this.pending.delete(key);
        this.handleChange(changedPath, eventType);
      }, this.debounceMs));
    });
    watcher.on('error', () => {
      this.watchers.delete(targetPath);
    });
    this.watchers.set(targetPath, watcher);
  }

  _emit(event) {
    this.onEvent?.(event);
    return event;
  }
}
