import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import type { AgentEnv } from '../types/env.js';
import {
  type BusinessCli,
  type ChannelTypeConfig,
  loadChannelTypeConfig,
} from './channel-type-config.js';

export interface PromptParts {
  basePrompt: string;
  channelConfigPrompt: string;
  systemPrompt: string;
}

function templatePath(filename: string): string {
  return fileURLToPath(new URL(`./templates/${filename}`, import.meta.url));
}

function readTemplate(filename: string): string {
  return readFileSync(templatePath(filename), 'utf8').trim();
}

function indentLines(lines: string[], indent = '  '): string[] {
  return lines.map((line) => `${indent}${line}`);
}

function renderBusinessCli(cli: BusinessCli): string[] {
  return [
    `- ${cli.name}${cli.purpose ? `: ${cli.purpose}` : ''}`,
    ...indentLines(cli.commands.map((command) => `- ${command}`), '  '),
  ];
}

function channelTypeConfigForEnv(env: AgentEnv): ChannelTypeConfig {
  return loadChannelTypeConfig(env.channelType);
}

function renderChannelTypeConfigPrompt(config: ChannelTypeConfig): string {
  const lines = [
    '<channel_type_config>',
    `channel_type: ${config.channel_type}`,
    `display_name: ${config.display_name}`,
    `description: ${config.description}`,
    '',
    '<dispatch_table>',
    ...config.dispatch_table.map((entry) => (
      `- (${entry.sender_kind} x ${entry.payload_type}) -> ${entry.protocol} ${entry.handler}: ${entry.description}`
    )),
    '</dispatch_table>',
    '',
    '<business_cli_list>',
    ...(config.business_clis.length > 0
      ? config.business_clis.flatMap(renderBusinessCli)
      : ['- none']),
    '</business_cli_list>',
    '',
    '<business_dispatch_types>',
    ...(config.dispatch_types.length > 0
      ? config.dispatch_types.map((entry) => (
        `- ${entry.type}: ${entry.description}`
        + `${entry.target ? ` target=${entry.target}` : ''}`
        + `${entry.params && entry.params.length > 0 ? ` params=${entry.params.join(',')}` : ''}`
      ))
      : ['- none']),
    '</business_dispatch_types>',
    '',
    '<business_task_types>',
    ...(config.task_types.length > 0
      ? config.task_types.map((entry) => `- ${entry.type}: ${entry.description}`)
      : ['- none']),
    '</business_task_types>',
    '',
    '<channel_type_invariants>',
    ...(config.invariants.length > 0 ? config.invariants.map((item) => `- ${item}`) : ['- none']),
    '</channel_type_invariants>',
    '',
    '<business_sop>',
    ...(config.business_sop.length > 0
      ? config.business_sop.flatMap((entry) => [
        `<${entry.name} protocol="${entry.protocol}">`,
        ...entry.steps.map((step, index) => `${index + 1}. ${step}`),
        `</${entry.name}>`,
      ])
      : ['none']),
    '</business_sop>',
    '',
    '<business_capability_index>',
    ...(config.capabilities.length > 0 ? config.capabilities.map((item) => `- ${item}`) : ['- none']),
    '</business_capability_index>',
    '</channel_type_config>',
  ];

  return lines.join('\n');
}

function renderChannelContextPrompt(env: AgentEnv): string {
  return [
    '<channel_context>',
    `channel_id: ${env.channelId}`,
    `channel_name: ${env.channelName || env.channelId}`,
    `agent_name: ${env.agentName}`,
    `workspace_id: ${env.workspaceId || '<none>'}`,
    `workdir: ${path.resolve(env.workdir)}`,
    `channel_type: ${env.channelType || 'echo'}`,
    '</channel_context>',
  ].join('\n');
}

export function buildPromptParts(env: AgentEnv): PromptParts {
  const basePrompt = readTemplate('base.md');
  const config = channelTypeConfigForEnv(env);
  const channelConfigPrompt = renderChannelTypeConfigPrompt(config);
  return {
    basePrompt,
    channelConfigPrompt,
    systemPrompt: `${basePrompt}\n\n${channelConfigPrompt}`,
  };
}

export function buildSystemPrompt(env: AgentEnv): string {
  return buildPromptParts(env).systemPrompt;
}

export function buildUserTurn(event: unknown, env: AgentEnv): string {
  return [
    renderChannelContextPrompt(env),
    '',
    'Handle the following daemon trigger event.',
    'Use channel CLI commands when you need to send a reply, inspect channel state, schedule work, or run business actions.',
    'Follow the injected dispatch table for the event sender.kind and payload.type.',
    'If no visible action is needed, finish quietly after checking the event.',
    '',
    JSON.stringify(event, null, 2),
  ].join('\n');
}
