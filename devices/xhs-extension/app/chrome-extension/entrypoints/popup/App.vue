<template>
  <div class="popup-container">
    <!-- 头部 -->
    <header class="popup-header">
      <div class="header-content">
        <img src="/icon-128.png" alt="Logo" class="logo" />
        <div class="header-text">
          <h1>Coagent · 小红书 Device</h1>
        </div>
      </div>
    </header>

    <!-- 主体内容 -->
    <main class="popup-main">
      <!-- Daemon 设备配置 -->
      <section class="connection-card">
        <div class="connection-card__header">
          <div>
            <h2>🛰️ Coagent Daemon Device</h2>
            <p>
              填写 daemon 地址、device api key、device id，点击 "连接" 与 daemon
              建立 WebSocket 长连。
            </p>
          </div>
          <StatusIndicator :status="connectionStatus" :text="connectionStatusText" />
        </div>

        <el-form label-position="top" class="connection-form">
          <el-form-item label="Daemon WebSocket URL" class="server-url-item">
            <el-input
              v-model="connectionForm.serverUrl"
              placeholder="ws://127.0.0.1:9501/device/{deviceId}"
            />
          </el-form-item>

          <el-form-item label="Daemon HTTP base" class="server-url-item">
            <el-input
              v-model="connectionForm.daemonHttpBase"
              :placeholder="defaultDaemonHttpBase"
            />
          </el-form-item>

          <el-form-item label="Device ID" class="server-url-item">
            <el-input
              v-model="connectionForm.deviceId"
              placeholder="例 xhs-laptop-001"
            />
          </el-form-item>

          <el-form-item label="Device API Key" class="api-key-item">
            <el-input
              v-model="connectionForm.apiKey"
              type="password"
              show-password
              placeholder="device api key（与 daemon DEVICE_KEYS 一致）"
              class="api-key-input"
            />
          </el-form-item>

          <el-form-item
            label="主人 user_id（可选 - 留空使用 daemon 默认）"
            class="server-url-item"
          >
            <el-input
              v-model="connectionForm.userId"
              placeholder="例 user-001（留空时由 daemon 解析当前主人）"
            />
          </el-form-item>

          <div class="connection-form__actions">
            <el-button
              type="primary"
              :loading="connecting"
              @click="connectDevice"
              class="action-btn action-btn--primary"
            >
              连接
            </el-button>
            <el-button
              :disabled="!store.connectionStatus.connected"
              @click="disconnectDevice"
              class="action-btn"
            >
              断开
            </el-button>
            <el-button
              :loading="savingConnection"
              @click="saveConnectionSettings"
              class="action-btn"
            >
              保存
            </el-button>
          </div>

          <el-checkbox
            v-model="connectionForm.autoReconnect"
            @change="handleAutoReconnectChange"
            class="auto-reconnect-checkbox"
          >
            自动重连
          </el-checkbox>
        </el-form>
      </section>

    </main>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from 'vue';
import { useAppStore } from '@/stores/app';
import StatusIndicator from '@/components/StatusIndicator.vue';
import {
  getDefaultWebSocketUrl,
  getDefaultDaemonHttpBase,
} from '@/entrypoints/background/connection-state';
import { ElMessage } from 'element-plus';

const store = useAppStore();

const defaultDaemonHttpBase = getDefaultDaemonHttpBase();

// 状态：5 字段 device 配置（spec §6.2.5）
const connectionForm = ref({
  serverUrl: getDefaultWebSocketUrl(),
  autoReconnect: true,
  apiKey: '',
  daemonHttpBase: defaultDaemonHttpBase,
  deviceId: '',
  userId: '',
});
const connecting = ref(false);
const savingConnection = ref(false);
const configLoaded = ref(false);

// 计算属性
const connectionStatus = computed<'connected' | 'disconnected' | 'error' | 'loading'>(() => {
  if (store.connectionStatus.reconnecting) {
    return 'loading';
  }
  if (!store.connectionStatus.connected) {
    return store.connectionStatus.lastError ? 'error' : 'disconnected';
  }
  return 'connected';
});
const connectionStatusText = computed(() => {
  if (store.connectionStatus.reconnecting) {
    return '重新连接中...';
  }
  if (!store.connectionStatus.connected) {
    return store.connectionStatus.lastError || '未连接';
  }
  return '已连接';
});

const loadConnectionConfig = async () => {
  const response = await chrome.runtime.sendMessage({ type: 'GET_CONNECTION_CONFIG' });
  if (response?.success) {
    connectionForm.value.serverUrl = response.config.serverUrl || getDefaultWebSocketUrl();
    connectionForm.value.autoReconnect = response.config.autoReconnect;
    connectionForm.value.apiKey = response.config.apiKey || '';
    connectionForm.value.daemonHttpBase =
      response.config.daemonHttpBase || defaultDaemonHttpBase;
    connectionForm.value.deviceId = response.config.deviceId || '';
    connectionForm.value.userId = response.config.userId || '';
  }
  configLoaded.value = true;
};

const buildPayload = () => ({
  serverUrl: connectionForm.value.serverUrl,
  autoReconnect: connectionForm.value.autoReconnect,
  apiKey: connectionForm.value.apiKey,
  daemonHttpBase: connectionForm.value.daemonHttpBase || defaultDaemonHttpBase,
  deviceId: connectionForm.value.deviceId,
  userId: connectionForm.value.userId,
});

const connectDevice = async () => {
  connecting.value = true;
  const response = await chrome.runtime.sendMessage({
    type: 'CONNECT_DEVICE',
    payload: buildPayload(),
  });
  connecting.value = false;

  if (response?.success) {
    ElMessage.success('已发起 device 连接');
  } else {
    ElMessage.error(response?.error || 'device 连接失败');
  }
};

const disconnectDevice = async () => {
  await chrome.runtime.sendMessage({ type: 'DISCONNECT_DEVICE' });
  ElMessage.success('已断开 daemon');
};

const saveConnectionSettings = async () => {
  savingConnection.value = true;
  const response = await chrome.runtime.sendMessage({
    type: 'SAVE_CONNECTION_CONFIG',
    payload: buildPayload(),
  });
  savingConnection.value = false;

  if (response?.success) {
    connectionForm.value.serverUrl = response.config.serverUrl || getDefaultWebSocketUrl();
    connectionForm.value.autoReconnect = response.config.autoReconnect;
    connectionForm.value.apiKey = response.config.apiKey || '';
    connectionForm.value.daemonHttpBase =
      response.config.daemonHttpBase || defaultDaemonHttpBase;
    connectionForm.value.deviceId = response.config.deviceId || '';
    connectionForm.value.userId = response.config.userId || '';
    ElMessage.success('device 配置已保存');
  } else {
    ElMessage.error(response?.error || '保存失败');
  }
};

const handleAutoReconnectChange = async () => {
  if (!configLoaded.value) return;
  await saveConnectionSettings();
};


function syncConnectionStatus(status: any) {
  store.updateConnectionStatus({
    connected: Boolean(status.connected),
    serverUrl: status.serverUrl || store.connectionStatus.serverUrl,
    reconnecting: Boolean(status.reconnecting),
    lastError: status.lastError,
    lastUpdated: status.lastUpdated,
  });
}

const messageListener = (message: any) => {
  if (message?.type === 'SERVER_STATUS_CHANGED') {
    syncConnectionStatus(message.payload);
  }
};

// 生命周期
onMounted(async () => {
  await store.loadSettings();
  await loadConnectionConfig();

  const statusResponse = await chrome.runtime.sendMessage({ type: 'GET_STATUS' });
  if (statusResponse?.success) {
    syncConnectionStatus(statusResponse.status);
  }

  chrome.runtime.onMessage.addListener(messageListener);
});

onBeforeUnmount(() => {
  chrome.runtime.onMessage.removeListener(messageListener);
});
</script>

<style scoped>
.popup-container {
  width: 400px;
  min-height: 520px;
  display: flex;
  flex-direction: column;
  background: #F8F9FA;
}

.popup-header {
  background: #FFFFFF;
  color: #1F2937;
  padding: 20px;
  border-bottom: 1px solid #E5E7EB;
}

.header-content {
  display: flex;
  align-items: center;
  gap: 14px;
}

.logo {
  width: 44px;
  height: 44px;
  border-radius: 10px;
  border: 1px solid #E5E7EB;
}

.header-text h1 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: #000000;
}

.popup-main {
  flex: 1;
  padding: 20px;
  overflow-y: auto;
}

.connection-card {
  background: #FFFFFF;
  border-radius: 8px;
  padding: 20px;
  border: 1px solid #E5E7EB;
  margin-bottom: 20px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.connection-card__header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
  flex-wrap: nowrap;
}

.connection-card__header > div:first-child {
  flex: 1;
  min-width: 0;
}

.connection-card__header h2 {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
  color: #000000;
  letter-spacing: -0.01em;
}

.connection-card__header p {
  margin: 6px 0 0;
  font-size: 13px;
  color: #6B7280;
  line-height: 1.5;
}

.api-key-link {
  color: #000000;
  text-decoration: underline;
  text-decoration-thickness: 1px;
  text-underline-offset: 2px;
  font-weight: 500;
  transition: color 0.2s;
}

.api-key-link:hover {
  color: #374151;
}

.connection-form {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

/* 输入框通用样式 */
.server-url-item :deep(.el-input__wrapper),
.api-key-item :deep(.el-input__wrapper) {
  padding: 10px 14px;
  border-radius: 8px;
  background: #FFFFFF;
  border: 1.5px solid #E5E7EB;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.02);
  transition: all 0.2s;
}

.server-url-item :deep(.el-input__wrapper:hover),
.api-key-item :deep(.el-input__wrapper:hover) {
  border-color: #D1D5DB;
}

.server-url-item :deep(.el-input__wrapper.is-focus),
.api-key-item :deep(.el-input__wrapper.is-focus) {
  border-color: #000000;
  box-shadow: 0 0 0 3px rgba(0, 0, 0, 0.05);
}

.server-url-item :deep(.el-input__inner),
.api-key-item :deep(.el-input__inner) {
  font-size: 14px;
  color: #1F2937;
  height: 22px;
  line-height: 22px;
}

.server-url-item :deep(.el-input__inner::placeholder),
.api-key-item :deep(.el-input__inner::placeholder) {
  color: #9CA3AF;
}

/* 按钮容器 */
.connection-form__actions {
  display: flex;
  gap: 10px;
}

/* 按钮通用样式 */
.action-btn {
  height: 38px;
  padding: 0 20px;
  font-size: 14px;
  font-weight: 500;
  border-radius: 8px;
  border: 1.5px solid #E5E7EB;
  background: #FFFFFF;
  color: #1F2937;
  transition: all 0.2s;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.02);
}

.action-btn:hover:not(:disabled) {
  border-color: #000000;
  background: #F9FAFB;
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.06);
}

.action-btn:active:not(:disabled) {
  transform: translateY(0);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.04);
}

.action-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* 主要按钮（连接）*/
.action-btn--primary {
  background: #000000;
  color: #FFFFFF;
  border-color: #000000;
}

.action-btn--primary:hover:not(:disabled) {
  background: #1F2937;
  border-color: #1F2937;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
}

.action-btn--primary:active:not(:disabled) {
  background: #374151;
}

/* 复选框样式 */
.auto-reconnect-checkbox {
  display: inline-flex;
  align-items: center;
  padding: 10px 14px;
  background: #F8F9FA;
  border: 1.5px solid #E5E7EB;
  border-radius: 8px;
  transition: all 0.2s;
  cursor: pointer;
  user-select: none;
}

.auto-reconnect-checkbox:hover {
  background: #F3F4F6;
  border-color: #D1D5DB;
}

.auto-reconnect-checkbox :deep(.el-checkbox__label) {
  font-size: 14px;
  color: #1F2937;
  font-weight: 500;
  cursor: pointer;
  padding-left: 8px;
}

.auto-reconnect-checkbox :deep(.el-checkbox__input) {
  display: flex;
  align-items: center;
}

.auto-reconnect-checkbox :deep(.el-checkbox__inner) {
  width: 18px;
  height: 18px;
  border-radius: 6px;
  border: 1.5px solid #D1D5DB;
  background: #FFFFFF;
  transition: all 0.2s;
}

.auto-reconnect-checkbox :deep(.el-checkbox__inner:hover) {
  border-color: #9CA3AF;
}

.auto-reconnect-checkbox :deep(.el-checkbox__input.is-checked .el-checkbox__inner) {
  background-color: #000000;
  border-color: #000000;
}

.auto-reconnect-checkbox :deep(.el-checkbox__input.is-checked .el-checkbox__inner::after) {
  border-color: #FFFFFF;
  border-width: 2px;
}

.auto-reconnect-checkbox :deep(.el-checkbox__input.is-checked) ~ .el-checkbox__label {
  color: #000000;
}

</style>
