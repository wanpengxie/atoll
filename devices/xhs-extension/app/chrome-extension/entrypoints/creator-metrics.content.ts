/**
 * Creator Platform Metrics Content Script
 *
 * 在 creator.xiaohongshu.com 上自动注入指标拦截器
 * 被动采集用户访问创作者平台时的 API 响应
 */

export default defineContentScript({
  matches: ['https://creator.xiaohongshu.com/*'],
  runAt: 'document_start',

  main() {
    console.log('[XHS Metrics] Content script 已加载');

    // 注入 MAIN world 的拦截器（通过 script 标签）
    injectScriptToMainWorld('inject-scripts/metrics-interceptor.js');

    // 设置事件监听器（在 ISOLATED world 中运行）
    setupMetricsBridge();

    console.log('[XHS Metrics] 初始化完成');
  },
});

/**
 * 通过 script 标签注入脚本到 MAIN world
 */
function injectScriptToMainWorld(scriptPath: string) {
  const script = document.createElement('script');
  script.src = chrome.runtime.getURL(scriptPath);
  script.type = 'text/javascript';

  script.onload = () => {
    console.log('[XHS Metrics] MAIN world 拦截器已注入');
    script.remove();
  };

  script.onerror = (error) => {
    console.error('[XHS Metrics] MAIN world 拦截器注入失败:', error);
  };

  // 尽早注入
  (document.head || document.documentElement).appendChild(script);
}

/**
 * 设置指标桥接（在 ISOLATED world 中监听 MAIN world 的事件）
 */
function setupMetricsBridge() {
  const EVENT_NAME = 'xiaohongshu-mcp:metrics-captured';

  /**
   * 处理捕获的指标数据
   */
  const handleCapturedMetrics = async (event: CustomEvent) => {
    const { dataType, data, timestamp, url } = event.detail;

    console.log('[XHS Metrics Bridge] 收到数据:', dataType, 'from:', url);

    try {
      // 发送给 background script
      const response = await chrome.runtime.sendMessage({
        type: 'METRICS_CAPTURED',
        payload: {
          dataType,
          data,
          timestamp,
          sourceUrl: url,
        },
      });

      if (response?.success) {
        console.log('[XHS Metrics Bridge] 数据推送成功');
      } else {
        console.warn('[XHS Metrics Bridge] 数据推送失败:', response?.error);
      }
    } catch (error) {
      console.error('[XHS Metrics Bridge] 发送数据失败:', error);
    }
  };

  // 监听 MAIN world 的数据
  window.addEventListener(EVENT_NAME, handleCapturedMetrics as EventListener);

  console.log('[XHS Metrics Bridge] 事件监听器已设置');
}
