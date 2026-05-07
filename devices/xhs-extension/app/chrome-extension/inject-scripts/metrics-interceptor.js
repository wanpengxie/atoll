/* eslint-disable */
/**
 * Metrics Interceptor
 *
 * 在 MAIN world 中运行，拦截小红书创作者平台的 API 响应
 * 被动采集模式：用户访问创作者平台时自动捕获数据
 *
 * 监听的 API 端点：
 * - /api/galaxy/creator/datacenter/note/analyze/list  笔记列表（含指标）
 * - /api/galaxy/creator/datacenter/note/base          笔记详情（诊断数据）
 * - /api/galaxy/creator/datacenter/note/audience/source/detail  观众画像
 */

(() => {
  // 防止重复注入
  if (window.__XIAOHONGSHU_METRICS_INTERCEPTOR_LOADED__) return;
  window.__XIAOHONGSHU_METRICS_INTERCEPTOR_LOADED__ = true;

  // 事件名称
  const EVENT_NAME = {
    METRICS_CAPTURED: 'xiaohongshu-mcp:metrics-captured',
    CLEANUP: 'xiaohongshu-mcp:cleanup',
  };

  // 监听的 API 路径
  const API_PATTERNS = {
    NOTE_LIST: '/api/galaxy/creator/datacenter/note/analyze/list',
    NOTE_BASE: '/api/galaxy/creator/datacenter/note/base',
    NOTE_AUDIENCE: '/api/galaxy/creator/datacenter/note/audience/source/detail',
  };

  // 缓存详情数据（用于合并 base 和 audience）
  const detailCache = new Map();
  const CACHE_TTL = 5 * 60 * 1000; // 5分钟过期

  /**
   * 清理过期缓存
   */
  const cleanExpiredCache = () => {
    const now = Date.now();
    for (const [key, value] of detailCache.entries()) {
      if (now - value.timestamp > CACHE_TTL) {
        detailCache.delete(key);
      }
    }
  };

  /**
   * 从 URL 中提取 note_id
   */
  const extractNoteId = (url) => {
    try {
      const urlObj = new URL(url, window.location.origin);
      return urlObj.searchParams.get('note_id');
    } catch {
      return null;
    }
  };

  /**
   * 发送采集到的数据到 content script
   */
  const sendCapturedData = (dataType, data) => {
    window.dispatchEvent(
      new CustomEvent(EVENT_NAME.METRICS_CAPTURED, {
        detail: {
          dataType,
          data,
          timestamp: Date.now(),
          url: window.location.href,
        },
      })
    );
  };

  /**
   * 处理笔记列表响应
   */
  const handleNoteListResponse = (responseData) => {
    try {
      const data = typeof responseData === 'string' ? JSON.parse(responseData) : responseData;

      if (data.success && data.data?.note_infos?.length > 0) {
        console.log('[XHS Metrics] 捕获笔记列表:', data.data.note_infos.length, '条');
        sendCapturedData('list', data);
      }
    } catch (error) {
      console.error('[XHS Metrics] 解析列表响应失败:', error);
    }
  };

  /**
   * 处理笔记详情响应（base 或 audience）
   * 只有当 base 和 audience 都收集齐全时才发送，避免重复推送
   */
  const handleNoteDetailResponse = (url, responseData, type) => {
    try {
      const noteId = extractNoteId(url);
      if (!noteId) {
        console.warn('[XHS Metrics] 无法从 URL 提取 note_id:', url);
        return;
      }

      const data = typeof responseData === 'string' ? JSON.parse(responseData) : responseData;

      if (!data.success) {
        return;
      }

      // 获取或创建缓存条目
      if (!detailCache.has(noteId)) {
        detailCache.set(noteId, {
          noteId,
          timestamp: Date.now(),
          base: null,
          audience: null,
          sent: false,  // 标记是否已发送
        });
      }

      const cacheEntry = detailCache.get(noteId);
      cacheEntry.timestamp = Date.now();

      if (type === 'base') {
        cacheEntry.base = data;
        console.log('[XHS Metrics] 捕获笔记详情 (base):', noteId);
      } else if (type === 'audience') {
        cacheEntry.audience = data;
        console.log('[XHS Metrics] 捕获观众画像:', noteId);
      }

      // 只有当两种数据都齐全且未发送过时才发送
      if (cacheEntry.base && cacheEntry.audience && !cacheEntry.sent) {
        cacheEntry.sent = true;
        sendCapturedData('detail', {
          noteId: cacheEntry.noteId,
          base: cacheEntry.base,
          audience: cacheEntry.audience,
        });
        console.log('[XHS Metrics] 发送完整详情数据:', noteId);

        // 发送后清除缓存
        detailCache.delete(noteId);
      }
    } catch (error) {
      console.error('[XHS Metrics] 解析详情响应失败:', error);
    }
  };

  /**
   * 检查 URL 是否匹配监听的 API
   */
  const matchApiPattern = (url) => {
    if (url.includes(API_PATTERNS.NOTE_LIST)) {
      return 'list';
    }
    if (url.includes(API_PATTERNS.NOTE_BASE)) {
      return 'base';
    }
    if (url.includes(API_PATTERNS.NOTE_AUDIENCE)) {
      return 'audience';
    }
    return null;
  };

  /**
   * 拦截 fetch
   */
  const originalFetch = window.fetch;
  window.fetch = async function (...args) {
    const [input, init] = args;
    const url = typeof input === 'string' ? input : input?.url || '';

    const apiType = matchApiPattern(url);

    // 如果不是我们关心的 API，直接返回
    if (!apiType) {
      return originalFetch.apply(this, args);
    }

    try {
      const response = await originalFetch.apply(this, args);

      // 克隆响应以便读取内容
      const clonedResponse = response.clone();

      // 异步处理响应，不阻塞原始调用
      clonedResponse.json().then((data) => {
        switch (apiType) {
          case 'list':
            handleNoteListResponse(data);
            break;
          case 'base':
            handleNoteDetailResponse(url, data, 'base');
            break;
          case 'audience':
            handleNoteDetailResponse(url, data, 'audience');
            break;
        }
      }).catch((error) => {
        console.warn('[XHS Metrics] 读取响应失败:', error);
      });

      return response;
    } catch (error) {
      throw error;
    }
  };

  /**
   * 拦截 XMLHttpRequest
   */
  const originalXHROpen = XMLHttpRequest.prototype.open;
  const originalXHRSend = XMLHttpRequest.prototype.send;

  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    this._xhsMetricsUrl = url;
    this._xhsMetricsApiType = matchApiPattern(url);
    return originalXHROpen.apply(this, [method, url, ...rest]);
  };

  XMLHttpRequest.prototype.send = function (...args) {
    if (this._xhsMetricsApiType) {
      this.addEventListener('load', function () {
        try {
          if (this.status === 200) {
            const data = JSON.parse(this.responseText);
            switch (this._xhsMetricsApiType) {
              case 'list':
                handleNoteListResponse(data);
                break;
              case 'base':
                handleNoteDetailResponse(this._xhsMetricsUrl, data, 'base');
                break;
              case 'audience':
                handleNoteDetailResponse(this._xhsMetricsUrl, data, 'audience');
                break;
            }
          }
        } catch (error) {
          console.warn('[XHS Metrics] XHR 响应处理失败:', error);
        }
      });
    }
    return originalXHRSend.apply(this, args);
  };

  /**
   * 清理定时器
   */
  const cleanupInterval = setInterval(cleanExpiredCache, 60 * 1000);

  /**
   * 清理处理器
   */
  const cleanupHandler = () => {
    // 恢复原始 fetch
    window.fetch = originalFetch;
    XMLHttpRequest.prototype.open = originalXHROpen;
    XMLHttpRequest.prototype.send = originalXHRSend;

    // 清理缓存和定时器
    detailCache.clear();
    clearInterval(cleanupInterval);

    // 移除监听器
    window.removeEventListener(EVENT_NAME.CLEANUP, cleanupHandler);

    // 移除标记
    delete window.__XIAOHONGSHU_METRICS_INTERCEPTOR_LOADED__;

    console.log('[XHS Metrics] 拦截器已清理');
  };

  // 监听清理事件
  window.addEventListener(EVENT_NAME.CLEANUP, cleanupHandler);

  console.log('[XHS Metrics] 拦截器已加载，监听创作者平台 API');
})();
