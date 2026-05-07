# Chrome Extension

小红书 MCP Chrome 扩展，通过 WebSocket 连接到 Go Backend，执行浏览器自动化操作。

## 功能特性

- WebSocket 连接管理
- 小红书页面操作自动化
- MCP 工具执行
- 连接状态监控

## 快速开始

### 安装依赖

```bash
cd extension/app/chrome-extension
pnpm install
```

### 开发模式

```bash
pnpm dev
```

构建产物在 `dist/chrome-mv3-dev` 目录

### 生产构建

```bash
pnpm build
```

构建产物在 `dist/chrome-mv3` 目录

### 加载到 Chrome

1. 打开 `chrome://extensions/`
2. 开启"开发者模式"
3. 点击"加载已解压的扩展程序"
4. 选择对应的 `dist/` 目录

## 连接配置

### 开发环境

在扩展弹出窗口中输入：
- 服务器: `ws://localhost:18040/ws`
- API Key: 从 Dashboard 获取

### 生产环境

在扩展弹出窗口中输入：
- 服务器: `wss://mcp.zouying.work/ws`
- API Key: 从 Dashboard 获取

## 项目结构

```
extension/
├── app/chrome-extension/
│   ├── entrypoints/
│   │   ├── background/    # Service Worker
│   │   ├── content/       # Content Scripts
│   │   └── popup/         # 弹出窗口UI
│   ├── dist/              # 构建产物
│   └── wxt.config.ts      # WXT 配置
└── README.md
```

## MCP 工具

扩展（WebSocket 端）可执行以下工具（由 Go Backend 转发调用）：

| 工具名 | 描述 |
|--------|------|
| `xhs_check_login_status` | 检查登录状态（旧实现，后端通常用原子工具编排替代） |
| `xhs_publish_content` | 发布图文笔记（旧实现，后端通常用原子工具编排替代） |
| `xhs_search_feeds` | 搜索笔记（旧实现，后端通常用原子工具编排替代） |
| `xhs_inject_script` | 在页面上下文/隔离上下文注入并执行 JS |
| `xhs_read_page_data` | 读取页面 `window` 上的数据（按 path） |
| `xhs_sync_cookies` | 同步小红书 Cookie 到后端（用于创作者平台接口） |
| `xhs_get_creator_metrics` | 获取创作者中心账号数据与笔记列表（creator.xiaohongshu.com） |
| `xhs_get_note_analytics` | 获取单篇笔记诊断与观众画像（creator.xiaohongshu.com） |
| `xhs_get_note_comments` | 获取笔记详情与评论（需 noteUrl 含 xsec_token） |
| `xhs_get_trending_topics` | 获取热点数据（新红 xh.newrank.cn） |
| `xhs_analyze_my_profile` | 分析当前登录账号主页（点击路线） |
| `xhs_analyze_profile` | 分析指定用户主页（需 url 含 xsec_token） |

## 开发指南

### 环境模式

- **开发模式** (`pnpm dev`): 显示"设置"和"控制台"按钮，保留调试信息
- **生产模式** (`pnpm build`): 隐藏调试功能，优化性能

### 调试

1. 打开 `chrome://extensions/`
2. 找到扩展，点击 "Service Worker" 查看后台脚本日志
3. 在小红书页面，右键 → 检查 → Console 查看 Content Script 日志

## 常见问题

**扩展无法连接？**
- 确认 Go Backend 正在运行（端口 18040）
- 检查 API Key 是否正确
- 查看浏览器控制台错误信息

**工具调用无响应？**
- 确认小红书页面已打开
- 检查 Service Worker 日志
- 验证扩展连接状态（弹出窗口显示"已连接"）

**连接频繁断开？**
- Service Worker 休眠导致，点击扩展图标重新激活
- 检查网络连接稳定性

## 许可证

MIT
