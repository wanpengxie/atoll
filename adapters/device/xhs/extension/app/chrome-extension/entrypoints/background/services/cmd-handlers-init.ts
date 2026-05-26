// services/cmd-handlers-init.ts — 在 background 启动时把 5 个 daemon cmd 接到具体 tool。
//
// proxy WS 推 `{cmd, params, session}` → server-devicebus.ts 调
// `getCommandHandler(cmd)(params, session)`。本文件负责一次性注册全部 5 个 cmd。
//
// 设计取舍：
//   - 把 frame.params 直接当成 *Tool.execute() 的入参（小红书工具入参基本是 plain object，
//     无 schema 约束；后端 daemon 端已校验过），失败即抛错。
//   - 把 ToolResult.content[0].text 解 JSON 作为 callback `result.data`。
//   - 工具内 isError=true 时，把 text 作为 message 抛 Error → server-devicebus.ts 回 error envelope。
//     若 text 是 JSON 且含 `code` 字段（M1.1 Fix-T4 §1：publish_timeout/publish_canceled），
//     则透传该 code，不被 `${cmd}_failed` 默认值覆盖，daemon 端能识别为结构化失败。
//
// frame.session 处理：
//   - 当前 V0.1：daemon 推送 session 仅作为兜底字段，已登录状态由用户在浏览器里自己维护，
//     chrome.cookies 自动持久化。这里**显式不消费** session.cookies。
//   - V0.5+ TODO：实现"按 frame.session.cookies 主动写入 chrome.cookies"
//     （用户在 daemon 切换主人 user_id 时无需手动重登）；M1.1 Fix-T4 §5 标记为 TODO，
//     PM 创建 V0.5 占位 ticket 后再行实现。
//     daemon 端 deviceCommandSend 在 SessionManager.getSession 返回 null 时
//     emit warn `device.session.missing` 不阻塞 dispatch，已在 Fix-T3 daemon 域跟进。

import type { ToolResult } from 'coagent-xhs-shared';
import {
  COAGENT_DEVICE_COMMANDS,
  type CoagentDeviceCommand,
} from 'coagent-xhs-shared';
import {
  registerCommandHandler,
  type CommandHandler,
  type CommandResultEnvelope,
} from './cmd-handlers';

import { PublishContentTool } from '../tools/publish-content';
import { PublishLongContentTool } from '../tools/publish-long-content';
import { SearchFeedsTool } from '../tools/search-feeds';
import { CheckLoginStatusTool } from '../tools/check-login';
import { InjectScriptTool } from '../tools/inject-script';
import { XhsGetNoteTool } from '../tools/xiaohongshu/get-note';
import { XhsGetMyRecentTool } from '../tools/xiaohongshu/get-my-recent';
import { XhsPublishStatusTool } from '../tools/xiaohongshu/publish-status';
import { XhsGetNoteCommentsTool } from '../tools/xiaohongshu/get-note-comments';
import { XhsGetNoteAnalyticsTool } from '../tools/xiaohongshu/get-note-analytics';
import { XhsGetCreatorMetricsTool } from '../tools/xiaohongshu/get-creator-metrics';
import { XhsGetTrendingTopicsTool } from '../tools/xiaohongshu/get-trending-topics';
import { XhsAnalyzeMyProfileTool, XhsAnalyzeProfileTool } from '../tools/xiaohongshu/analyze-profile';

let initialized = false;

export function initCoagentDeviceCmdHandlers(): void {
  if (initialized) return;
  initialized = true;

  // 注册顺序对应 COAGENT_DEVICE_COMMANDS 的 14 条 cmd（5 legacy + 9 new）。
  const registrations: Array<[CoagentDeviceCommand, ToolLike]> = [
    [COAGENT_DEVICE_COMMANDS.PUBLISH,             new PublishContentTool()],
    [COAGENT_DEVICE_COMMANDS.SEARCH,              new SearchFeedsTool()],
    [COAGENT_DEVICE_COMMANDS.GET_NOTE,            new XhsGetNoteTool()],
    [COAGENT_DEVICE_COMMANDS.GET_MY_RECENT,       new XhsGetMyRecentTool()],
    [COAGENT_DEVICE_COMMANDS.PUBLISH_STATUS,      new XhsPublishStatusTool()],
    [COAGENT_DEVICE_COMMANDS.PUBLISH_LONG_CONTENT, new PublishLongContentTool()],
    [COAGENT_DEVICE_COMMANDS.CHECK_LOGIN_STATUS,  new CheckLoginStatusTool()],
    [COAGENT_DEVICE_COMMANDS.INJECT_SCRIPT,       new InjectScriptTool()],
    [COAGENT_DEVICE_COMMANDS.ANALYZE_MY_PROFILE,  new XhsAnalyzeMyProfileTool()],
    [COAGENT_DEVICE_COMMANDS.ANALYZE_PROFILE,     new XhsAnalyzeProfileTool()],
    [COAGENT_DEVICE_COMMANDS.GET_NOTE_COMMENTS,   new XhsGetNoteCommentsTool()],
    [COAGENT_DEVICE_COMMANDS.GET_NOTE_ANALYTICS,  new XhsGetNoteAnalyticsTool()],
    [COAGENT_DEVICE_COMMANDS.GET_CREATOR_METRICS, new XhsGetCreatorMetricsTool()],
    [COAGENT_DEVICE_COMMANDS.GET_TRENDING_TOPICS, new XhsGetTrendingTopicsTool()],
  ];
  for (const [cmd, tool] of registrations) {
    registerCommandHandler(cmd, wrapTool(tool, cmd));
  }

  console.log('[CoagentDeviceCmd] registered handlers', Object.values(COAGENT_DEVICE_COMMANDS));
}

interface ToolLike {
  name: string;
  execute(args: any): Promise<ToolResult>;
}

function wrapTool(tool: ToolLike, cmd: CoagentDeviceCommand): CommandHandler {
  return async (
    params,
    // TODO(V0.5): consume frame.session.cookies for active account switching —
    // daemon SessionManager 推送 cookies 后，extension 应当通过 chrome.cookies.set
    // 主动写入；当前 V0.1 已登录状态由用户自己维护，故显式忽略。
    // PM 跟进 ticket 占位：V0.5 device-session-injection。
    _session,
    context,
  ): Promise<CommandResultEnvelope> => {
    // R3-T4 FX9：把 dispatch correlation_id 注入 tool args，让
    // PublishContentTool.execute 能在 chrome.storage.local 持久化 publish-wait
    // state（SW evict 时由 background recovery 兜底重发 callback）。
    // 字段命名 `__correlationId` 表明是平台注入元数据，工具实现读完即用，
    // 不会跑到页面侧脚本。
    const augmented =
      context?.correlationId
        ? { ...(params ?? {}), __correlationId: context.correlationId }
        : params ?? {};
    const inner = await tool.execute(augmented);
    if (inner.isError) {
      const text = String(inner.content?.[0]?.text ?? `${tool.name} failed`);
      // 优先把 text 当结构化错误 envelope 解（{code, message, ...}），允许工具自定义
      // code 透传到 daemon callback envelope（如 publish_timeout / publish_canceled）。
      let parsedCode: string | undefined;
      let parsedMessage: string | undefined;
      try {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === 'object') {
          if (typeof (parsed as any).code === 'string') {
            parsedCode = (parsed as any).code;
          }
          if (typeof (parsed as any).message === 'string') {
            parsedMessage = (parsed as any).message;
          }
        }
      } catch {
        /* text 不是 JSON，按原样作为 message。 */
      }
      const err = new Error(parsedMessage ?? text);
      (err as any).code = parsedCode ?? `${cmd}_failed`;
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
