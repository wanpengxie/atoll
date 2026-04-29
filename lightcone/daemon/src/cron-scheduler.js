const POLL_INTERVAL_MS = 1_000;
const HEALTHY_UPTIME_MS = 10_000;

function normalizeInteger(value, label) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed)) {
    throw new Error(`invalid ${label}: ${value}`);
  }
  return parsed;
}

function expandCronPart(part, min, max, label) {
  if (part === '*') return null;

  const values = new Set();
  for (const piece of part.split(',')) {
    const token = piece.trim();
    if (!token) continue;

    let range = token;
    let step = 1;
    if (token.includes('/')) {
      const [base, stepToken] = token.split('/');
      range = base;
      step = normalizeInteger(stepToken, `${label} step`);
      if (step <= 0) {
        throw new Error(`invalid ${label} step: ${token}`);
      }
    }

    let start = min;
    let end = max;
    if (range !== '*') {
      if (range.includes('-')) {
        const [rawStart, rawEnd] = range.split('-');
        start = normalizeInteger(rawStart, `${label} start`);
        end = normalizeInteger(rawEnd, `${label} end`);
      } else {
        start = normalizeInteger(range, label);
        end = start;
      }
    }

    if (start < min || end > max || start > end) {
      throw new Error(`invalid ${label} range: ${token}`);
    }

    for (let current = start; current <= end; current += step) {
      values.add(current);
    }
  }

  return values;
}

function buildCronMatcher(expression) {
  const parts = String(expression ?? '').trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error(`cron must have 5 fields, got ${parts.length}`);
  }

  const minute = expandCronPart(parts[0], 0, 59, 'minute');
  const hour = expandCronPart(parts[1], 0, 23, 'hour');
  const day = expandCronPart(parts[2], 1, 31, 'day');
  const month = expandCronPart(parts[3], 1, 12, 'month');
  const weekday = expandCronPart(parts[4], 0, 6, 'weekday');

  return (date) => {
    const checks = [
      [minute, date.getMinutes()],
      [hour, date.getHours()],
      [day, date.getDate()],
      [month, date.getMonth() + 1],
      [weekday, date.getDay()],
    ];

    return checks.every(([allowed, value]) => allowed === null || allowed.has(value));
  };
}

function minuteKey(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hour = String(date.getHours()).padStart(2, '0');
  const minute = String(date.getMinutes()).padStart(2, '0');
  return `${year}-${month}-${day}T${hour}:${minute}`;
}

function normalizeSchedule(schedule) {
  const kind = schedule.kind ?? (schedule.cron ? 'cron' : 'at');
  const normalized = {
    id: String(schedule.id),
    channelId: String(schedule.channelId ?? schedule.channel_id),
    kind,
    cron: schedule.cron ?? schedule.cron_expr ?? null,
    at: schedule.at ?? schedule.next_run_at ?? null,
    reason: String(schedule.reason ?? ''),
    payload: schedule.payload ?? {},
    createdAt: schedule.createdAt ?? schedule.created_at ?? new Date().toISOString(),
    createdBy: schedule.createdBy ?? schedule.created_by ?? 'unknown',
  };

  if (normalized.kind === 'cron') {
    normalized.matcher = buildCronMatcher(normalized.cron);
  } else if (!normalized.at) {
    throw new Error(`schedule ${normalized.id} missing at timestamp`);
  }

  return normalized;
}

export class CronScheduler {
  constructor({ onTick }) {
    this.onTick = onTick;
    this.entries = new Map();
    this.timer = setInterval(() => {
      this._poll().catch((err) => {
        console.error('[CronScheduler] Poll failed:', err.message);
      });
    }, POLL_INTERVAL_MS);
    this.timer.unref?.();
  }

  loadChannel(channelId, schedules) {
    this.clearChannel(channelId);
    for (const schedule of schedules) {
      this.register(schedule);
    }
  }

  register(schedule) {
    const normalized = normalizeSchedule(schedule);
    this.entries.set(this._entryKey(normalized.channelId, normalized.id), normalized);
    return normalized;
  }

  cancel(channelId, scheduleId) {
    return this.entries.delete(this._entryKey(channelId, scheduleId));
  }

  clearChannel(channelId) {
    for (const key of this.entries.keys()) {
      if (key.startsWith(`${channelId}:`)) {
        this.entries.delete(key);
      }
    }
  }

  list(channelId) {
    return [...this.entries.values()].filter((entry) => entry.channelId === channelId);
  }

  stop() {
    clearInterval(this.timer);
    this.entries.clear();
  }

  _entryKey(channelId, scheduleId) {
    return `${channelId}:${scheduleId}`;
  }

  async _poll() {
    const now = new Date();
    const currentMinute = minuteKey(now);

    for (const [key, entry] of this.entries) {
      if (entry.kind === 'cron') {
        if (now.getSeconds() > 1) continue;
        if (entry.lastFiredMinute === currentMinute) continue;
        if (!entry.matcher(now)) continue;

        entry.lastFiredMinute = currentMinute;
        await this.onTick({
          channelId: entry.channelId,
          scheduleId: entry.id,
          kind: entry.kind,
          reason: entry.reason,
          payload: entry.payload,
          original: entry,
        });
        continue;
      }

      const atTime = new Date(entry.at);
      if (Number.isNaN(atTime.getTime())) {
        console.warn(`[CronScheduler] Ignoring invalid schedule.at for ${entry.id}: ${entry.at}`);
        this.entries.delete(key);
        continue;
      }
      if (entry.firedAt) continue;
      if (atTime.getTime() > now.getTime()) continue;

      entry.firedAt = now.toISOString();
      await this.onTick({
        channelId: entry.channelId,
        scheduleId: entry.id,
        kind: entry.kind,
        reason: entry.reason,
        payload: entry.payload,
        original: entry,
      });
      this.entries.delete(key);
    }
  }
}

export { HEALTHY_UPTIME_MS };
