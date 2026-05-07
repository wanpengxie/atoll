import { XIAOHONGSHU_TOOL_NAMES, ToolResult, ERROR_MESSAGES } from 'xiaohongshu-mcp-shared';
import { BaseTool } from './base-tool';
import { CheckLoginStatusTool } from './check-login';
import { PublishContentTool } from './publish-content';
import { PublishLongContentTool } from './publish-long-content';
import { SearchFeedsTool } from './search-feeds';
import { InjectScriptTool, ReadPageDataTool } from './inject-script';
import { ChromeNavigateTool } from './atomic/navigate';
import { BrowserControlTool } from './atomic/browser-control';
import { ChromeExtractDataTool } from './atomic/extract-data';
import { ChromeClickTool } from './atomic/click';
import { ChromeFillTool } from './atomic/fill';
import { ChromeKeyboardTool } from './atomic/keyboard';
import { ChromeUploadFileTool } from './atomic/upload-file';
import { ChromeSetFilesTool } from './atomic/set-files';
import { ChromeWaitElementsTool } from './atomic/wait-elements';
import { SyncCookiesTool, cookieSyncService } from './sync-cookies';
import { ExecuteCdpScriptTool } from './execute-cdp-script';

// 小红书数据工具
import { XhsGetCreatorMetricsTool } from './xiaohongshu/get-creator-metrics';
import { XhsGetNoteAnalyticsTool } from './xiaohongshu/get-note-analytics';
import { XhsGetNoteCommentsTool } from './xiaohongshu/get-note-comments';
import { XhsGetTrendingTopicsTool } from './xiaohongshu/get-trending-topics';
import { XhsAnalyzeMyProfileTool, XhsAnalyzeProfileTool } from './xiaohongshu/analyze-profile';
// M1.1-T2 新增：5 个 daemon cmd 直接对应的工具实现
import { XhsGetNoteTool } from './xiaohongshu/get-note';
import { XhsGetMyRecentTool } from './xiaohongshu/get-my-recent';
import { XhsPublishStatusTool } from './xiaohongshu/publish-status';

// 抖音工具
import { DouyinGetCreatorInfoTool } from './douyin/get-creator-info';
import { DouyinCheckLoginTool } from './douyin/check-login';
import { DouyinPublishContentTool } from './douyin/publish-content';

// coagent device cmd handler 注册
import { initCoagentDeviceCmdHandlers } from '../services/cmd-handlers-init';

// 工具注册表
const toolsRegistry = new Map<string, any>();

/**
 * 初始化工具注册表
 */
export function initToolsRegistry() {
  // 注册所有工具
  registerTool(new CheckLoginStatusTool());
  registerTool(new PublishContentTool());
  registerTool(new PublishLongContentTool());
  registerTool(new SearchFeedsTool());
  registerTool(new InjectScriptTool());
  registerTool(new ReadPageDataTool());
  registerTool(new SyncCookiesTool());

  // 注册原子工具
  registerTool(new ChromeNavigateTool());
  registerTool(new BrowserControlTool());
  registerTool(new ChromeExtractDataTool());
  registerTool(new ChromeClickTool());
  registerTool(new ChromeFillTool());
  registerTool(new ChromeKeyboardTool());
  registerTool(new ChromeUploadFileTool());
  registerTool(new ChromeSetFilesTool());
  registerTool(new ChromeWaitElementsTool());

  // 注册小红书数据工具
  registerTool(new XhsGetCreatorMetricsTool());
  registerTool(new XhsGetNoteAnalyticsTool());
  registerTool(new XhsGetNoteCommentsTool());
  registerTool(new XhsGetTrendingTopicsTool());
  registerTool(new XhsAnalyzeMyProfileTool());
  registerTool(new XhsAnalyzeProfileTool());

  // M1.1-T2: 5 个 daemon cmd 直对工具
  registerTool(new XhsGetNoteTool());
  registerTool(new XhsGetMyRecentTool());
  registerTool(new XhsPublishStatusTool());

  // 注册 CDP 通用工具
  registerTool(new ExecuteCdpScriptTool());

  // 注册抖音工具
  registerTool(new DouyinGetCreatorInfoTool());
  registerTool(new DouyinCheckLoginTool());
  registerTool(new DouyinPublishContentTool());

  // 启动 Cookie 自动同步服务
  cookieSyncService.start();

  // 注册 coagent daemon device cmd handlers（5 个 cmd → 工具实现的桥）
  initCoagentDeviceCmdHandlers();

  console.log(`Registered ${toolsRegistry.size} tools`);
}

/**
 * 注册工具
 */
function registerTool(tool: BaseTool) {
  toolsRegistry.set(tool.name, tool);
}

/**
 * 处理工具调用
 */
export async function handleCallTool(payload: any): Promise<ToolResult> {
  const { name, args } = payload;
  console.info('[Tools] handleCallTool invoked', {
    name,
    argKeys: args && typeof args === 'object' ? Object.keys(args) : [],
  });

  const tool = toolsRegistry.get(name);
  if (!tool) {
    console.error('[Tools] tool not found', { name });
    return {
      content: [
        {
          type: 'error',
          text: `${ERROR_MESSAGES.TOOL_NOT_FOUND}: ${name}`,
        },
      ],
      isError: true,
    };
  }

  try {
    const result = await tool.execute(args);
    console.info('[Tools] tool execution finished', {
      name,
      isError: Boolean(result?.isError),
    });
    return result;
  } catch (error) {
    console.error(`Tool execution failed for ${name}:`, error);
    return {
      content: [
        {
          type: 'error',
          text: error instanceof Error ? error.message : ERROR_MESSAGES.TOOL_EXECUTION_FAILED,
        },
      ],
      isError: true,
    };
  }
}

/**
 * 基础工具类
 */
export { BaseTool };
