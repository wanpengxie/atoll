// services/cmd-handlers-init.ts — 在 background 启动时把 5 个 daemon cmd 接到具体 tool。
//
// daemon WS 推 `{cmd, params, session}` → coagent-device.ts 调
// `getCommandHandler(cmd)(params, session)`。本文件负责一次性注册全部 5 个 cmd。
//
// 设计取舍：
//   - 把 frame.params 直接当成 *Tool.execute() 的入参（小红书工具入参基本是 plain object，
//     无 schema 约束；后端 daemon 端已校验过），失败即抛错。
//   - 把 ToolResult.content[0].text 解 JSON 作为 callback `result.data`。
//   - 工具内 isError=true 时，把 text 作为 message 抛 Error → coagent-device.ts 回 error envelope。
//   - 暂不消费 frame.session.cookies（chrome.cookies 自动持久；daemon 推过来作为兜底字段，
//     未来 V0.5+ 可在用户切换账号时主动注入；当前 V0.1 已登录状态由用户自己维护）。

import type { ToolResult } from 'xiaohongshu-mcp-shared';
import {
  COAGENT_DEVICE_COMMANDS,
  type CoagentDeviceCommand,
} from 'xiaohongshu-mcp-shared';
import {
  registerCommandHandler,
  type CommandHandler,
  type CommandResultEnvelope,
} from './cmd-handlers';

import { PublishContentTool } from '../tools/publish-content';
import { SearchFeedsTool } from '../tools/search-feeds';
import { XhsGetNoteTool } from '../tools/xiaohongshu/get-note';
import { XhsGetMyRecentTool } from '../tools/xiaohongshu/get-my-recent';
import { XhsPublishStatusTool } from '../tools/xiaohongshu/publish-status';

let initialized = false;

export function initCoagentDeviceCmdHandlers(): void {
  if (initialized) return;
  initialized = true;

  const publishTool = new PublishContentTool();
  const searchTool = new SearchFeedsTool();
  const getNoteTool = new XhsGetNoteTool();
  const getMyRecentTool = new XhsGetMyRecentTool();
  const publishStatusTool = new XhsPublishStatusTool();

  registerCommandHandler(
    COAGENT_DEVICE_COMMANDS.PUBLISH,
    wrapTool(publishTool, COAGENT_DEVICE_COMMANDS.PUBLISH)
  );
  registerCommandHandler(
    COAGENT_DEVICE_COMMANDS.SEARCH,
    wrapTool(searchTool, COAGENT_DEVICE_COMMANDS.SEARCH)
  );
  registerCommandHandler(
    COAGENT_DEVICE_COMMANDS.GET_NOTE,
    wrapTool(getNoteTool, COAGENT_DEVICE_COMMANDS.GET_NOTE)
  );
  registerCommandHandler(
    COAGENT_DEVICE_COMMANDS.GET_MY_RECENT,
    wrapTool(getMyRecentTool, COAGENT_DEVICE_COMMANDS.GET_MY_RECENT)
  );
  registerCommandHandler(
    COAGENT_DEVICE_COMMANDS.PUBLISH_STATUS,
    wrapTool(publishStatusTool, COAGENT_DEVICE_COMMANDS.PUBLISH_STATUS)
  );

  console.log('[CoagentDeviceCmd] registered handlers', Object.values(COAGENT_DEVICE_COMMANDS));
}

interface ToolLike {
  name: string;
  execute(args: any): Promise<ToolResult>;
}

function wrapTool(tool: ToolLike, cmd: CoagentDeviceCommand): CommandHandler {
  return async (params, _session): Promise<CommandResultEnvelope> => {
    const inner = await tool.execute(params ?? {});
    if (inner.isError) {
      const message = String(inner.content?.[0]?.text ?? `${tool.name} failed`);
      const err = new Error(message);
      (err as any).code = `${cmd}_failed`;
      throw err;
    }

    // 期望 ToolResult.content[0].text 是 JSON 字符串；不是的话原样塞 raw 字段。
    const text = String(inner.content?.[0]?.text ?? '');
    let data: Record<string, unknown> = {};
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === 'object') {
        data = parsed as Record<string, unknown>;
      } else {
        data = { raw: parsed };
      }
    } catch {
      data = { raw: text };
    }

    return { result: data };
  };
}
