/**
 * 抖音创作者平台相关常量
 */

// 创作者平台 URL
export const DOUYIN_CREATOR_URLS = {
  // 创作者首页（含数据概览）
  HOME: 'https://creator.douyin.com/creator-micro/home',
  // 内容发布页面
  PUBLISH: 'https://creator.douyin.com/creator-micro/content/upload',
  // 数据中心
  DATA_CENTER: 'https://creator.douyin.com/creator-micro/data/overview',
  // 登录检测页
  LOGIN_CHECK: 'https://creator.douyin.com',
};

// DOM 选择器
export const DOUYIN_SELECTORS = {
  // 用户名
  USERNAME: '.user-name, .creator-name, [class*="userName"], [class*="nickName"]',
  // 数据统计项
  STATS_ITEM: '.stats-item, .data-item, [class*="dataItem"], [class*="statsItem"]',
  // 登录状态检测
  LOGIN_AVATAR: '.avatar, [class*="avatar"], [class*="userAvatar"]',
  NOT_LOGGED_IN: '.login-btn, [class*="loginBtn"], [class*="notLogin"]',
};

// 数据提取超时时间
export const DOUYIN_TIMEOUTS = {
  PAGE_LOAD: 30000,
  DATA_FETCH: 10000,
  ELEMENT_WAIT: 5000,
};

// 文件上传相关
export const DOUYIN_UPLOAD = {
  MAX_IMAGES: 35,
  SUPPORTED_FORMATS: ['jpg', 'jpeg', 'png', 'gif', 'webp'],
  MAX_FILE_SIZE: 20 * 1024 * 1024, // 20MB
};
