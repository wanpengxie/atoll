import { XIAOHONGSHU_TOOL_NAMES } from './constants';

export { XIAOHONGSHU_TOOL_NAMES };

/**
 * 本地工具 schema 类型（不依赖外部 SDK）。
 * `inputSchema.properties` 用 `Record<string, unknown>` 容纳现有 schema 的
 * enum/default/maxLength/items/additionalProperties 等子字段。
 */
export type ToolSchema = {
  name: string;
  description: string;
  inputSchema: {
    type: 'object';
    properties: Record<string, unknown>;
    required?: string[];
  };
};

/**
 * 工具Schema定义
 */
export const TOOL_SCHEMAS: ToolSchema[] = [
  {
    name: XIAOHONGSHU_TOOL_NAMES.CHECK_LOGIN_STATUS,
    description: '检查小红书登录状态',
    inputSchema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.PUBLISH_CONTENT,
    description: '发布小红书图文内容（填写标题、内容、上传图片、添加标签并自动发布）',
    inputSchema: {
      type: 'object',
      properties: {
        title: {
          type: 'string',
          description: '内容标题（小红书限制：最多20个中文字或英文单词）',
        },
        content: {
          type: 'string',
          description: '正文内容，不包含以#开头的标签内容',
        },
        images: {
          type: 'array',
          description: '图片URL列表，支持网络地址或本地文件路径（至少1张，最多9张）',
          items: {
            type: 'string',
            description: '图片URL地址，支持 http/https URL 或本地文件路径',
          },
          minItems: 1,
          maxItems: 9,
        },
        tags: {
          type: 'array',
          description: '话题标签列表（可选），如 ["美食", "旅行", "生活"]',
          items: {
            type: 'string',
          },
        },
        publish_at: {
          type: 'string',
          description:
            '定时发布时间（可选）。支持 RFC3339（如 2026-03-01T20:30:00+08:00）或本地格式 YYYY-MM-DD HH:mm（如 2026-03-01 20:30）',
        },
      },
      required: ['title', 'content', 'images'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.PUBLISH_LONG_CONTENT,
    description: '发布小红书长文内容（固定写长文模式，填写长文标题与正文并进入发布页）',
    inputSchema: {
      type: 'object',
      properties: {
        title: {
          type: 'string',
          description: '长文标题（建议不超过64个字）',
          maxLength: 64,
        },
        content: {
          type: 'string',
          description: '长文正文内容（用于生成长文页面）',
        },
        description: {
          type: 'string',
          description: '发布页文案描述（可选，展示在最终发布页正文区域）',
        },
        publish_at: {
          type: 'string',
          description:
            '定时发布时间（可选）。支持 RFC3339（如 2026-03-01T20:30:00+08:00）或本地格式 YYYY-MM-DD HH:mm（如 2026-03-01 20:30）',
        },
      },
      required: ['title', 'content'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.SEARCH_FEEDS,
    description: '搜索小红书内容',
    inputSchema: {
      type: 'object',
      properties: {
        keyword: {
          type: 'string',
          description: '搜索关键词',
        },
        sort: {
          type: 'string',
          description: '排序方式：general(综合), popularity(热门), time(最新)',
          enum: ['general', 'popularity', 'time'],
          default: 'general',
        },
        limit: {
          type: 'number',
          description: '获取数量限制（默认20）',
          default: 20,
        },
      },
      required: ['keyword'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.INJECT_SCRIPT,
    description: '在指定执行环境中注入并运行任意 JavaScript 代码',
    inputSchema: {
      type: 'object',
      properties: {
        code: {
          type: 'string',
          description: '要在页面执行的 JS 代码字符串，需返回可序列化的结果',
        },
        world: {
          type: 'string',
          description: '脚本执行环境，MAIN 表示页面上下文，ISOLATED 表示内容脚本上下文',
          enum: ['MAIN', 'ISOLATED'],
          default: 'MAIN',
        },
        args: {
          type: 'object',
          description: '传入脚本的参数对象，可选',
          additionalProperties: true,
        },
      },
      required: ['code'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.READ_PAGE_DATA,
    description: '按照路径读取页面 window 对象上的数据',
    inputSchema: {
      type: 'object',
      properties: {
        path: {
          type: 'string',
          description: '要读取的 window 属性路径，例如 __INITIAL_STATE__.note.noteInfo',
        },
      },
      required: ['path'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.ANALYZE_MY_PROFILE,
    description:
      '分析当前登录账号主页（必须走点击路线；需传 savePath 用于后端落盘，避免大结果塞进 tool result）',
    inputSchema: {
      type: 'object',
      properties: {
        savePath: {
          type: 'string',
          description:
            '保存结果的路径（必填）。建议使用容器路径，如 /home/xhs/workspace/materials/xhs/profile.json',
        },
      },
      required: ['savePath'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.ANALYZE_PROFILE,
    description:
      '分析指定用户主页（必须传 url，且 url 必须包含 xsec_token；分析自己请改用 xhs_analyze_my_profile）',
    inputSchema: {
      type: 'object',
      properties: {
        url: {
          type: 'string',
          description:
            '目标用户主页 URL（必填，必须包含 xsec_token；建议从小红书内“分享-复制链接”获取）',
        },
        sampleCount: {
          type: 'number',
          description: '采集笔记数量，默认 20',
          default: 20,
        },
        savePath: {
          type: 'string',
          description:
            '保存结果的路径（必填）。建议使用容器路径，如 /home/xhs/workspace/materials/xhs/profile.json',
        },
      },
      required: ['url', 'savePath'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.GET_NOTE_COMMENTS,
    description: '获取笔记详情和评论（noteUrl 必须包含 xsec_token）',
    inputSchema: {
      type: 'object',
      properties: {
        noteUrl: {
          type: 'string',
          description: '笔记 URL（必填，必须包含 xsec_token；建议从小红书内“分享-复制链接”获取）',
        },
      },
      required: ['noteUrl'],
    },
  },
  {
    name: XIAOHONGSHU_TOOL_NAMES.GET_TRENDING_TOPICS,
    description: '获取小红书热点数据（新红 xh.newrank.cn，支持热搜词榜单或关键词热点笔记）',
    inputSchema: {
      type: 'object',
      properties: {
        rankType: {
          type: 'string',
          description: '榜单类型：day(日榜)、week(周榜)、month(月榜)',
          enum: ['day', 'week', 'month'],
          default: 'day',
        },
        limit: {
          type: 'number',
          description: '返回条数限制，默认20，最大100',
          default: 20,
        },
        keyword: {
          type: 'string',
          description:
            '关键词（可选）。传入后走 notesSearch 链路，返回该关键词的热点笔记；不传则返回热搜词榜单',
        },
        searchword: {
          type: 'string',
          description: 'keyword 别名（兼容页面 URL 参数命名）',
        },
      },
      required: [],
    },
  },
];
