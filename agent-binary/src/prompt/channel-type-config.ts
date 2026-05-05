import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';

export type HandlerProtocol = 'deterministic' | 'agentic' | 'hybrid';
export type SenderKindSelector = 'human' | 'agent' | 'system' | 'external' | '*';

export interface DispatchTableEntry {
  sender_kind: SenderKindSelector;
  payload_type: string;
  protocol: HandlerProtocol;
  handler: string;
  description: string;
}

export interface BusinessCli {
  name: string;
  commands: string[];
  purpose?: string;
}

export interface DispatchTypeConfig {
  type: string;
  description: string;
  target?: string;
  params?: string[];
}

export interface TaskTypeConfig {
  type: string;
  description: string;
}

export interface BusinessSop {
  name: string;
  protocol: HandlerProtocol;
  steps: string[];
}

export interface ChannelTypeConfig {
  channel_type: string;
  display_name: string;
  description: string;
  dispatch_table: DispatchTableEntry[];
  business_clis: BusinessCli[];
  dispatch_types: DispatchTypeConfig[];
  task_types: TaskTypeConfig[];
  invariants: string[];
  business_sop: BusinessSop[];
  capabilities: string[];
}

export class ChannelTypeConfigError extends Error {
  code = 'invalid_channel_type_config';

  constructor(message: string) {
    super(message);
    this.name = 'ChannelTypeConfigError';
  }
}

const PROTOCOLS = new Set<HandlerProtocol>(['deterministic', 'agentic', 'hybrid']);
const SENDER_KINDS = new Set<SenderKindSelector>(['human', 'agent', 'system', 'external', '*']);

function configPath(channelType: string): string {
  return fileURLToPath(new URL(`./channel-types/${channelType}.yaml`, import.meta.url));
}

function asRecord(value: unknown, path: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new ChannelTypeConfigError(`${path} must be an object`);
  }
  return value as Record<string, unknown>;
}

function requiredString(value: unknown, path: string): string {
  const text = String(value ?? '').trim();
  if (!text) throw new ChannelTypeConfigError(`${path} is required`);
  return text;
}

function optionalString(value: unknown): string | undefined {
  const text = String(value ?? '').trim();
  return text || undefined;
}

function stringList(value: unknown, path: string): string[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw new ChannelTypeConfigError(`${path} must be an array`);
  return value.map((item, index) => requiredString(item, `${path}[${index}]`));
}

function objectList(value: unknown, path: string): Record<string, unknown>[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw new ChannelTypeConfigError(`${path} must be an array`);
  return value.map((item, index) => asRecord(item, `${path}[${index}]`));
}

function protocol(value: unknown, path: string): HandlerProtocol {
  const text = requiredString(value, path) as HandlerProtocol;
  if (!PROTOCOLS.has(text)) {
    throw new ChannelTypeConfigError(`${path} must be deterministic, agentic, or hybrid`);
  }
  return text;
}

function senderKind(value: unknown, path: string): SenderKindSelector {
  const text = requiredString(value, path) as SenderKindSelector;
  if (!SENDER_KINDS.has(text)) {
    throw new ChannelTypeConfigError(`${path} must be human, agent, system, external, or *`);
  }
  return text;
}

export function validateChannelTypeConfig(input: unknown): ChannelTypeConfig {
  const raw = asRecord(input, 'channel_type_config');
  const dispatchTable = objectList(raw.dispatch_table, 'dispatch_table').map((entry, index) => ({
    sender_kind: senderKind(entry.sender_kind, `dispatch_table[${index}].sender_kind`),
    payload_type: requiredString(entry.payload_type, `dispatch_table[${index}].payload_type`),
    protocol: protocol(entry.protocol, `dispatch_table[${index}].protocol`),
    handler: requiredString(entry.handler, `dispatch_table[${index}].handler`),
    description: requiredString(entry.description, `dispatch_table[${index}].description`),
  }));
  if (dispatchTable.length === 0) {
    throw new ChannelTypeConfigError('dispatch_table must contain at least one entry');
  }

  const businessClis = objectList(raw.business_clis, 'business_clis').map((entry, index) => {
    const commands = stringList(entry.commands, `business_clis[${index}].commands`);
    if (commands.length === 0) {
      throw new ChannelTypeConfigError(`business_clis[${index}].commands must contain at least one command`);
    }
    return {
      name: requiredString(entry.name, `business_clis[${index}].name`),
      commands,
      purpose: optionalString(entry.purpose),
    };
  });

  return {
    channel_type: requiredString(raw.channel_type, 'channel_type'),
    display_name: requiredString(raw.display_name, 'display_name'),
    description: requiredString(raw.description, 'description'),
    dispatch_table: dispatchTable,
    business_clis: businessClis,
    dispatch_types: objectList(raw.dispatch_types, 'dispatch_types').map((entry, index) => ({
      type: requiredString(entry.type, `dispatch_types[${index}].type`),
      description: requiredString(entry.description, `dispatch_types[${index}].description`),
      target: optionalString(entry.target),
      params: stringList(entry.params, `dispatch_types[${index}].params`),
    })),
    task_types: objectList(raw.task_types, 'task_types').map((entry, index) => ({
      type: requiredString(entry.type, `task_types[${index}].type`),
      description: requiredString(entry.description, `task_types[${index}].description`),
    })),
    invariants: stringList(raw.invariants, 'invariants'),
    business_sop: objectList(raw.business_sop, 'business_sop').map((entry, index) => {
      const steps = stringList(entry.steps, `business_sop[${index}].steps`);
      if (steps.length === 0) {
        throw new ChannelTypeConfigError(`business_sop[${index}].steps must contain at least one step`);
      }
      return {
        name: requiredString(entry.name, `business_sop[${index}].name`),
        protocol: protocol(entry.protocol, `business_sop[${index}].protocol`),
        steps,
      };
    }),
    capabilities: stringList(raw.capabilities, 'capabilities'),
  };
}

export function parseChannelTypeConfigText(raw: string): ChannelTypeConfig {
  return validateChannelTypeConfig(parse(raw));
}

function genericChannelTypeConfig(channelType: string): ChannelTypeConfig {
  return {
    channel_type: channelType,
    display_name: `${channelType} Channel`,
    description: 'Generic channel type fallback with only common coagent operations.',
    dispatch_table: [
      {
        sender_kind: 'human',
        payload_type: 'user.text',
        protocol: 'agentic',
        handler: 'handle_user_text',
        description: 'Use the stable coagent protocol and decide the next action from channel context.',
      },
    ],
    business_clis: [],
    dispatch_types: [],
    task_types: [],
    invariants: [],
    business_sop: [],
    capabilities: [],
  };
}

export function loadChannelTypeConfig(channelType: string): ChannelTypeConfig {
  const normalized = String(channelType ?? '').trim() || 'echo';
  try {
    return parseChannelTypeConfigText(readFileSync(configPath(normalized), 'utf8'));
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT') {
      return genericChannelTypeConfig(normalized);
    }
    throw error;
  }
}
