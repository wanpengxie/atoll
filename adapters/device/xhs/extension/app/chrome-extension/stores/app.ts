import { defineStore } from 'pinia';
import { ref, computed } from 'vue';

export interface ConnectionStatus {
  connected: boolean;
  serverUrl: string;
  extensionId?: string;
  lastPing?: Date;
  reconnecting?: boolean;
  lastError?: string;
  lastUpdated?: number;
}

export interface UserInfo {
  isLoggedIn: boolean;
  username?: string;
  avatar?: string;
  userId?: string;
}

export interface ToolExecution {
  id: string;
  toolName: string;
  args: any;
  status: 'pending' | 'running' | 'success' | 'error';
  result?: any;
  error?: string;
  startTime: Date;
  endTime?: Date;
}

export const useAppStore = defineStore('app', () => {
  // 连接状态（默认空，由 popup 侧从 storage 加载真实 device 配置后回填）
  const connectionStatus = ref<ConnectionStatus>({
    connected: false,
    serverUrl: '',
  });

  // 用户信息
  const userInfo = ref<UserInfo>({
    isLoggedIn: false,
  });

  // 工具执行历史
  const executionHistory = ref<ToolExecution[]>([]);

  // 设置
  const settings = ref({
    autoConnect: true,
    showNotifications: true,
    debugMode: false,
    maxHistoryItems: 50,
  });

  // 计算属性
  const isReady = computed(() => {
    return connectionStatus.value.connected && userInfo.value.isLoggedIn;
  });

  const runningExecutions = computed(() => {
    return executionHistory.value.filter((e) => e.status === 'running');
  });

  const recentExecutions = computed(() => {
    return executionHistory.value
      .slice(0, settings.value.maxHistoryItems)
      .sort((a, b) => b.startTime.getTime() - a.startTime.getTime());
  });

  // Actions
  const updateConnectionStatus = (status: Partial<ConnectionStatus>) => {
    connectionStatus.value = {
      ...connectionStatus.value,
      ...status,
    };
  };

  const updateUserInfo = (info: Partial<UserInfo>) => {
    userInfo.value = { ...userInfo.value, ...info };
  };

  const addExecution = (execution: ToolExecution) => {
    executionHistory.value.unshift(execution);
    // 限制历史记录数量
    if (executionHistory.value.length > settings.value.maxHistoryItems * 2) {
      executionHistory.value = executionHistory.value.slice(0, settings.value.maxHistoryItems);
    }
  };

  const updateExecution = (id: string, updates: Partial<ToolExecution>) => {
    const index = executionHistory.value.findIndex((e) => e.id === id);
    if (index !== -1) {
      executionHistory.value[index] = {
        ...executionHistory.value[index],
        ...updates,
      };
    }
  };

  const clearHistory = () => {
    executionHistory.value = [];
  };

  const updateSettings = (newSettings: Partial<typeof settings.value>) => {
    settings.value = { ...settings.value, ...newSettings };
    // 保存到Chrome存储
    chrome.storage.local.set({ settings: settings.value });
  };

  // 从Chrome存储加载设置
  const loadSettings = async () => {
    const stored = await chrome.storage.local.get('settings');
    if (stored.settings) {
      settings.value = { ...settings.value, ...stored.settings };
    }
  };

  return {
    // State
    connectionStatus,
    userInfo,
    executionHistory,
    settings,

    // Computed
    isReady,
    runningExecutions,
    recentExecutions,

    // Actions
    updateConnectionStatus,
    updateUserInfo,
    addExecution,
    updateExecution,
    clearHistory,
    updateSettings,
    loadSettings,
  };
});
