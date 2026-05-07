// services/cmd-handlers.ts — coagent device cmd 适配层。
//
// daemon 通过 WS 推 `{type:"command", correlation_id, cmd, params, session}`，
// coagent-device.ts 收到后调用 dispatchCommand(frame) → 这里：
//   - 校验 cmd 合法
//   - 把 params 映射到 background 内部既有 *Tool.execute() 入参
//   - 把 ToolResult.content[0].text 解 JSON 作为 callback `result`
//   - 异常 → throw Error；coagent-device.ts 捕获后回 `{status:"error", error:{code,message}}`
//
// phase-4 中 5 个 cmd 都接到具体 tool；phase-3 打通骨架（publish 已接 PublishContentTool，
// 其他 cmd 标记 not_implemented，待 phase-4 完成）。

import {
  COAGENT_DEVICE_COMMANDS,
  type CoagentDeviceCommand,
} from 'xiaohongshu-mcp-shared';

export interface CommandFrame {
  type: 'command';
  correlation_id: string;
  cmd: string;
  params?: Record<string, unknown>;
  session?: {
    cookies?: Array<Record<string, unknown>>;
    user_id?: string;
    expires_at?: number | null;
  } | null;
}

export interface CommandResultEnvelope {
  /** Plain JSON object that goes into callback `result`. */
  result: Record<string, unknown>;
}

export type CommandHandler = (
  params: Record<string, unknown>,
  session: CommandFrame['session']
) => Promise<CommandResultEnvelope>;

const handlerRegistry = new Map<CoagentDeviceCommand, CommandHandler>();

export function registerCommandHandler(cmd: CoagentDeviceCommand, handler: CommandHandler): void {
  handlerRegistry.set(cmd, handler);
}

export function getCommandHandler(cmd: string): CommandHandler | undefined {
  return handlerRegistry.get(cmd as CoagentDeviceCommand);
}

export function listRegisteredCommands(): CoagentDeviceCommand[] {
  return Array.from(handlerRegistry.keys());
}

/** Validate that `cmd` is one of the 5 spec commands. */
export function isKnownCommand(cmd: string): cmd is CoagentDeviceCommand {
  return Object.values(COAGENT_DEVICE_COMMANDS).includes(cmd as CoagentDeviceCommand);
}

/** Stub fallback used until phase-4 hooks every command. */
export const notImplementedHandler: CommandHandler = async (_params, _session) => {
  const err = new Error('command handler not implemented');
  (err as any).code = 'not_implemented';
  throw err;
};
