/**
 * Metrics Collector Service
 *
 * 负责接收 content script 转发的数据，并推送到后端
 * 简化版：直接推送，不做队列缓存
 */

import { getConnectionConfig } from '../connection-state';

interface CapturedMetricsPayload {
  dataType: 'list' | 'detail';
  data: any;
  timestamp: number;
  sourceUrl: string;
}

interface PushResult {
  success: boolean;
  error?: string;
  notesSaved?: number;
}

class MetricsCollectorService {
  /**
   * 处理捕获的指标数据
   */
  async handleCapturedMetrics(payload: CapturedMetricsPayload): Promise<PushResult> {
    const { dataType, data } = payload;

    console.log('[MetricsCollector] 收到数据:', dataType);

    return this.pushToBackend(dataType, data);
  }

  /**
   * 推送数据到后端
   */
  private async pushToBackend(dataType: 'list' | 'detail', data: any): Promise<PushResult> {
    try {
      const config = await getConnectionConfig();

      if (!config.serverUrl || !config.apiKey) {
        console.warn('[MetricsCollector] 缺少服务器配置');
        return { success: false, error: '未配置服务器地址或 API Key' };
      }

      // 构建请求体
      let requestBody: Record<string, any>;

      if (dataType === 'list') {
        requestBody = {
          data_type: 'list',
          list_data: data,
        };
      } else {
        // detail 数据 - 只包含非空字段
        requestBody = {
          data_type: 'detail',
          note_id: data.noteId,
        };

        // 只有当 base 存在且非 null 时才添加
        if (data.base != null) {
          requestBody.base_data = data.base;
        }

        // 只有当 audience 存在且非 null 时才添加
        if (data.audience != null) {
          requestBody.audience_data = data.audience;
        }
      }

      // 推送到后端
      // 将 ws:// 或 wss:// 转换为 http:// 或 https://，并移除 /ws 后缀
      const baseUrl = config.serverUrl
        .replace(/\/ws\/?$/, '')
        .replace(/\/$/, '')
        .replace(/^ws:\/\//, 'http://')
        .replace(/^wss:\/\//, 'https://');
      const pushUrl = `${baseUrl}/api/xhs/metrics/push`;

      console.log('[MetricsCollector] 推送到:', pushUrl);

      const response = await fetch(pushUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${config.apiKey}`,
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error('[MetricsCollector] 推送失败:', response.status, errorText);
        return { success: false, error: `HTTP ${response.status}: ${errorText}` };
      }

      const result = await response.json();
      console.log('[MetricsCollector] 推送成功:', result);

      return {
        success: result.success,
        notesSaved: result.notes_saved,
        error: result.error,
      };
    } catch (error) {
      console.error('[MetricsCollector] 推送异常:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }
}

// 单例
export const metricsCollectorService = new MetricsCollectorService();
