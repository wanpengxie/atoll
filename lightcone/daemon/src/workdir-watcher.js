import { existsSync, readdirSync, statSync, watch } from 'fs';
import path from 'path';

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

export class WorkdirWatcher {
  constructor({ workdir, onEvent, debounceMs = 50 }) {
    this.workdir = workdir;
    this.onEvent = onEvent;
    this.debounceMs = debounceMs;
    this.watchers = new Map();
    this.pending = new Map();
  }

  start() {
    this._watchDirectory(this.workdir);
    for (const topLevel of ['artifacts', 'notes', 'agents']) {
      for (const dir of walkDirectories(path.join(this.workdir, topLevel))) {
        this._watchDirectory(dir);
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

    const stat = safeStat(absolutePath);
    if (stat?.isDirectory()) {
      this._watchDirectory(absolutePath);
    }

    if (relativePath === 'channel.yaml') {
      return this._emit({
        type: 'channel.config.updated',
        source: 'workdir-watcher',
        created_at: new Date().toISOString(),
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
      || relativePath.startsWith('agents/')
    ) {
      return this._emit({
        type: 'workdir.changed',
        source: 'workdir-watcher',
        created_at: new Date().toISOString(),
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

  _watchDirectory(dir) {
    if (!existsSync(dir) || this.watchers.has(dir)) return;
    const watcher = watch(dir, { persistent: false }, (eventType, fileName) => {
      if (!fileName) return;
      const changedPath = path.join(dir, String(fileName));
      const key = `${eventType}:${changedPath}`;
      if (this.pending.has(key)) clearTimeout(this.pending.get(key));
      this.pending.set(key, setTimeout(() => {
        this.pending.delete(key);
        this.handleChange(changedPath, eventType);
      }, this.debounceMs));
    });
    watcher.on('error', () => {
      this.watchers.delete(dir);
    });
    this.watchers.set(dir, watcher);
  }

  _emit(event) {
    this.onEvent?.(event);
    return event;
  }
}
