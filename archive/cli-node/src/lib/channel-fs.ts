import { existsSync, readFileSync, readdirSync, unlinkSync } from 'node:fs';
import path from 'node:path';
import { parse } from 'yaml';
import { CliError } from './errors.js';

interface ChannelMember {
  memberType?: string;
  member_type?: string;
  memberId?: string;
  member_id?: string;
  displayName?: string;
  display_name?: string;
  joinedAt?: string;
  joined_at?: string;
}

interface ChannelMetaFile {
  channel_id?: string;
  name?: string;
  type?: string;
  status?: string;
  capability_set?: {
    cli_binaries?: string[];
  };
  members?: ChannelMember[];
  created_at?: string;
  archived_at?: string | null;
}

interface MessageRecord {
  content?: string;
  createdAt?: string;
  created_at?: string;
  [key: string]: unknown;
}

function parseStructuredFile<T>(filePath: string): T {
  return parse(readFileSync(filePath, 'utf8')) as T;
}

function sortByCreatedAt<T extends { createdAt?: string; created_at?: string }>(items: T[]): T[] {
  return [...items].sort((left, right) => {
    const a = new Date(left.createdAt ?? left.created_at ?? 0).getTime();
    const b = new Date(right.createdAt ?? right.created_at ?? 0).getTime();
    return a - b;
  });
}

export function resolveWorkdir(): string {
  const workdir = process.env.COAGENT_WORKDIR || process.cwd();
  if (!existsSync(workdir)) {
    throw new CliError('workdir_not_found', `Workdir not found: ${workdir}`, 1);
  }
  return workdir;
}

export function channelMetaPath(workdir = resolveWorkdir()): string {
  return path.join(workdir, 'channel.yaml');
}

export function requireChannelId(workdir = resolveWorkdir()): string {
  const envChannelId = String(process.env.COAGENT_CHANNEL_ID ?? process.env.CHANNEL_ID ?? '').trim();
  if (envChannelId) return envChannelId;

  const meta = readChannelMeta(workdir);
  if (!meta.channel_id) {
    throw new CliError('channel_id_missing', 'channel_id is required and channel.yaml does not define it', 1);
  }
  return meta.channel_id;
}

export function readChannelMeta(workdir = resolveWorkdir()) {
  const metaPath = channelMetaPath(workdir);
  if (!existsSync(metaPath)) {
    throw new CliError('channel_meta_not_found', `channel.yaml not found in ${workdir}`, 1);
  }

  const raw = parseStructuredFile<ChannelMetaFile>(metaPath);
  return {
    channel_id: String(raw.channel_id ?? '').trim(),
    name: String(raw.name ?? '').trim(),
    type: String(raw.type ?? '').trim(),
    status: String(raw.status ?? '').trim(),
    capability_set: {
      cli_binaries: Array.isArray(raw.capability_set?.cli_binaries)
        ? raw.capability_set.cli_binaries.map((item) => String(item).trim()).filter(Boolean)
        : [],
    },
    members: Array.isArray(raw.members) ? raw.members.map((member) => ({
      member_type: String(member.member_type ?? member.memberType ?? '').trim(),
      member_id: String(member.member_id ?? member.memberId ?? '').trim(),
      display_name: String(member.display_name ?? member.displayName ?? '').trim(),
      joined_at: String(member.joined_at ?? member.joinedAt ?? '').trim(),
    })) : [],
    created_at: String(raw.created_at ?? '').trim(),
    archived_at: raw.archived_at ?? null,
  };
}

export function readMessages(workdir = resolveWorkdir()): MessageRecord[] {
  const messagesDir = path.join(workdir, 'messages');
  if (!existsSync(messagesDir)) return [];

  const messages: MessageRecord[] = [];
  for (const fileName of readdirSync(messagesDir).filter((entry) => entry.endsWith('.jsonl')).sort()) {
    const filePath = path.join(messagesDir, fileName);
    const lines = readFileSync(filePath, 'utf8')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean);

    for (const line of lines) {
      try {
        messages.push(JSON.parse(line) as MessageRecord);
      } catch {}
    }
  }

  return sortByCreatedAt(messages);
}

export function readSchedules(workdir = resolveWorkdir()): Record<string, unknown>[] {
  const schedulesDir = path.join(workdir, 'schedules');
  if (!existsSync(schedulesDir)) return [];

  const schedules: Record<string, unknown>[] = [];
  for (const fileName of readdirSync(schedulesDir).filter((entry) => entry.endsWith('.yaml')).sort()) {
    const filePath = path.join(schedulesDir, fileName);
    schedules.push(parseStructuredFile<Record<string, unknown>>(filePath));
  }

  return sortByCreatedAt(schedules as Array<{ createdAt?: string; created_at?: string }>) as Record<string, unknown>[];
}

export function deleteScheduleFile(scheduleId: string, workdir = resolveWorkdir()): void {
  const schedulePath = path.join(workdir, 'schedules', `${scheduleId}.yaml`);
  if (existsSync(schedulePath)) {
    unlinkSync(schedulePath);
  }
}
