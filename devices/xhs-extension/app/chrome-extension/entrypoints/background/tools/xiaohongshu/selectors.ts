/**
 * 小红书创作者平台相关常量
 */

// 创作者平台 URL
export const XHS_CREATOR_URLS = {
  // 数据中心 - 笔记分析页面
  DATA_CENTER: 'https://creator.xiaohongshu.com/statistics/data-analysis',
  // 笔记详情分析（需要拼接 note_id）
  NOTE_DETAIL: 'https://creator.xiaohongshu.com/statistics/note',
  // 创作者首页
  HOME: 'https://creator.xiaohongshu.com/home',
};

// 小红书创作者平台 API 端点
export const XHS_API_ENDPOINTS = {
  // 笔记列表（含指标）
  NOTE_LIST: '/api/galaxy/creator/datacenter/note/analyze/list',
  // 笔记详情（诊断数据）
  NOTE_BASE: '/api/galaxy/creator/datacenter/note/base',
  // 观众画像
  NOTE_AUDIENCE: '/api/galaxy/creator/datacenter/note/audience/source/detail',
  // 账号概览
  ACCOUNT_OVERVIEW: '/api/galaxy/creator/home/overview',
};

// 数据提取超时时间
export const XHS_TIMEOUTS = {
  PAGE_LOAD: 30000,
  DATA_FETCH: 10000,
  ELEMENT_WAIT: 5000,
};
