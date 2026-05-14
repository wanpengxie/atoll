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
      <!-- 主入口：1-key 流程（coagent resolve API） -->
      <section class="connection-card">
        <div class="connection-card__header">
          <div>
            <h2>🛰️ Coagent Device</h2>
            <p>
              填 Coagent api-key + 点 "连接"，扩展自动从 coagent server 反查
              daemon 连接信息并建立 WebSocket 长连。
            </p>
          </div>
          <StatusIndicator :status="connectionStatus" :text="connectionStatusText" />
        </div>

        <el-form label-position="top" class="connection-form">
          <el-form-item label="Coagent api-key" class="api-key-item">
            <el-input
              v-model="primaryForm.apiKey"
              type="password"
              show-password
              placeholder="sk_dev_xxx"
              class="api-key-input"
            />
          </el-form-item>

          <el-form-item label="Server URL（coagent server）" class="server-url-item">
            <el-input
              v-model="primaryForm.coagentServerUrl"
              :placeholder="defaultCoagentServerUrl"
            />
          </el-form-item>

          <div v-if="primaryError" class="primary-error">
            <span class="primary-error__icon">⚠️</span>
            <span class="primary-error__text">{{ primaryError }}</span>
          </div>

          <div class="connection-form__actions">
            <el-button
              type="primary"
              :loading="resolving"
              @click="connectViaResolve"
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
          </div>
        </el-form>
      </section>

      <!-- Advanced 折叠：旧 5 字段（dev/test 用） -->
      <el-collapse v-model="advancedOpen" class="advanced-collapse">
        <el-collapse-item name="advanced">
          <template #title>
            <span class="advanced-title">⚙️ Advanced（手动配置 5 字段）</span>
          </template>
          <section class="advanced-card">
            <p class="advanced-desc">
              dev / test 场景：跳过 coagent resolve，直接手动填 daemon
              连接信息。生产用 main 入口的 api-key 即可。
            </p>

            <el-form label-position="top" class="connection-form">
              <el-form-item label="Daemon WebSocket URL" class="server-url-item">
                <el-input
                  v-model="advancedForm.serverUrl"
                  placeholder="ws://127.0.0.1:9501/device/{deviceId}"
                />
              </el-form-item>

              <el-form-item label="Daemon HTTP base" class="server-url-item">
                <el-input
                  v-model="advancedForm.daemonHttpBase"
                  :placeholder="defaultDaemonHttpBase"
                />
              </el-form-item>

              <el-form-item label="Device ID" class="server-url-item">
                <el-input v-model="advancedForm.deviceId" placeholder="例 xhs-laptop-001" />
              </el-form-item>

              <el-form-item label="Device API Key" class="api-key-item">
                <el-input
                  v-model="advancedForm.apiKey"
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
                  v-model="advancedForm.userId"
                  placeholder="例 user-001"
                />
              </el-form-item>

              <div class="connection-form__actions">
                <el-button
                  :loading="advancedConnecting"
                  @click="connectAdvanced"
                  class="action-btn action-btn--primary"
                >
                  连接（advanced）
                </el-button>
                <el-button
                  :loading="savingAdvanced"
                  @click="saveAdvanced"
                  class="action-btn"
                >
                  保存
                </el-button>
              </div>

              <el-checkbox
                v-model="advancedForm.autoReconnect"
                @change="handleAutoReconnectChange"
                class="auto-reconnect-checkbox"
              >
                自动重连
              </el-checkbox>
            </el-form>
          </section>
        </el-collapse-item>
      </el-collapse>
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
  getDefaultCoagentServerUrl,
} from '@/entrypoints/background/connection-state';
import { ElMessage } from 'element-plus';

const store = useAppStore();

const defaultDaemonHttpBase = getDefaultDaemonHttpBase();
const defaultCoagentServerUrl = getDefaultCoagentServerUrl();

// ── 主入口表单（1-key resolve 流程）────────────────────────────────────
const primaryForm = ref({
  apiKey: '',
  coagentServerUrl: defaultCoagentServerUrl,
});
const resolving = ref(false);
/** Resolve 失败时的友好提示（中文）。connect 成功 / disconnect 时清空。 */
const primaryError = ref<string>('');

// ── Advanced 表单（旧 5 字段直连流程）──────────────────────────────────
const advancedForm = ref({
  serverUrl: getDefaultWebSocketUrl(),
  autoReconnect: true,
  apiKey: '',
  daemonHttpBase: defaultDaemonHttpBase,
  deviceId: '',
  userId: '',
});
const advancedConnecting = ref(false);
const savingAdvanced = ref(false);
const advancedOpen = ref<string[]>([]); // collapse 默认收起
const configLoaded = ref(false);

// ── 连接状态徽章 ──────────────────────────────────────────────────────
type StatusKind = 'connected' | 'disconnected' | 'error' | 'loading';
const connectionStatus = computed<StatusKind>(() => {
  if (resolving.value || store.connectionStatus.reconnecting) return 'loading';
  if (!store.connectionStatus.connected) {
    if (primaryError.value || store.connectionStatus.lastError) return 'error';
    return 'disconnected';
  }
  return 'connected';
});
const connectionStatusText = computed(() => {
  if (resolving.value) return '解析中...';
  if (store.connectionStatus.reconnecting) return '重新连接中...';
  if (!store.connectionStatus.connected) {
    if (primaryError.value) return '解析失败';
    if (store.connectionStatus.lastError) return store.connectionStatus.lastError;
    return '未连接';
  }
  return '已连接';
});

// ── storage 加载 / 回填 ───────────────────────────────────────────────
const loadConnectionConfig = async () => {
  const response = await chrome.runtime.sendMessage({ type: 'GET_CONNECTION_CONFIG' });
  if (response?.success) {
    const c = response.config ?? {};
    // 主入口：coagentServerUrl 优先；没填过用默认。apiKey 用 device key 同字段。
    primaryForm.value.coagentServerUrl =
      c.coagentServerUrl || defaultCoagentServerUrl;
    primaryForm.value.apiKey = c.apiKey || '';

    // Advanced 5 字段
    advancedForm.value.serverUrl = c.serverUrl || getDefaultWebSocketUrl();
    advancedForm.value.autoReconnect = c.autoReconnect ?? true;
    advancedForm.value.apiKey = c.apiKey || '';
    advancedForm.value.daemonHttpBase = c.daemonHttpBase || defaultDaemonHttpBase;
    advancedForm.value.deviceId = c.deviceId || '';
    advancedForm.value.userId = c.userId || '';
  }
  configLoaded.value = true;
};

// ── 主入口 connect（resolve + connect 一步走） ─────────────────────────
const connectViaResolve = async () => {
  primaryError.value = '';
  resolving.value = true;
  try {
    const response = await chrome.runtime.sendMessage({
      type: 'RESOLVE_AND_CONNECT',
      payload: {
        coagentServerUrl: primaryForm.value.coagentServerUrl,
        apiKey: primaryForm.value.apiKey,
      },
    });
    if (response?.success) {
      ElMessage.success('已发起 device 连接');
      // resolve 后回填 advanced 表单方便切换查看。
      if (response.config) {
        const c = response.config;
        advancedForm.value.serverUrl = c.serverUrl || advancedForm.value.serverUrl;
        advancedForm.value.daemonHttpBase = c.daemonHttpBase || advancedForm.value.daemonHttpBase;
        advancedForm.value.deviceId = c.deviceId || advancedForm.value.deviceId;
        advancedForm.value.apiKey = c.apiKey || advancedForm.value.apiKey;
        advancedForm.value.userId = c.userId || advancedForm.value.userId;
      }
    } else {
      // 友好提示：网络类失败提示走 advanced fallback。
      const kind: string = response?.errorKind || 'unknown';
      const baseMsg = response?.error || '连接失败';
      primaryError.value =
        kind === 'network' || kind === 'unavailable'
          ? `${baseMsg} — 若 server 不可达，可展开 Advanced 直接配 daemon 5 字段`
          : baseMsg;
      ElMessage.error(baseMsg);
    }
  } finally {
    resolving.value = false;
  }
};

// ── Advanced：旧 5 字段直连 / 保存 ─────────────────────────────────────
const buildAdvancedPayload = () => ({
  serverUrl: advancedForm.value.serverUrl,
  autoReconnect: advancedForm.value.autoReconnect,
  apiKey: advancedForm.value.apiKey,
  daemonHttpBase: advancedForm.value.daemonHttpBase || defaultDaemonHttpBase,
  deviceId: advancedForm.value.deviceId,
  userId: advancedForm.value.userId,
});

const connectAdvanced = async () => {
  primaryError.value = '';
  advancedConnecting.value = true;
  try {
    const response = await chrome.runtime.sendMessage({
      type: 'CONNECT_DEVICE',
      payload: buildAdvancedPayload(),
    });
    if (response?.success) {
      ElMessage.success('已发起 device 连接（advanced）');
    } else {
      ElMessage.error(response?.error || 'device 连接失败');
    }
  } finally {
    advancedConnecting.value = false;
  }
};

const saveAdvanced = async () => {
  savingAdvanced.value = true;
  try {
    const response = await chrome.runtime.sendMessage({
      type: 'SAVE_CONNECTION_CONFIG',
      payload: buildAdvancedPayload(),
    });
    if (response?.success) {
      const c = response.config ?? {};
      advancedForm.value.serverUrl = c.serverUrl || getDefaultWebSocketUrl();
      advancedForm.value.autoReconnect = c.autoReconnect ?? true;
      advancedForm.value.apiKey = c.apiKey || '';
      advancedForm.value.daemonHttpBase = c.daemonHttpBase || defaultDaemonHttpBase;
      advancedForm.value.deviceId = c.deviceId || '';
      advancedForm.value.userId = c.userId || '';
      ElMessage.success('device 配置已保存');
    } else {
      ElMessage.error(response?.error || '保存失败');
    }
  } finally {
    savingAdvanced.value = false;
  }
};

const disconnectDevice = async () => {
  await chrome.runtime.sendMessage({ type: 'DISCONNECT_DEVICE' });
  primaryError.value = '';
  ElMessage.success('已断开 daemon');
};

const handleAutoReconnectChange = async () => {
  if (!configLoaded.value) return;
  await saveAdvanced();
};

// ── 状态广播 ──────────────────────────────────────────────────────────
function syncConnectionStatus(status: any) {
  store.updateConnectionStatus({
    connected: Boolean(status.connected),
    serverUrl: status.serverUrl || store.connectionStatus.serverUrl,
    reconnecting: Boolean(status.reconnecting),
    lastError: status.lastError,
    lastUpdated: status.lastUpdated,
  });
  // 一旦真正连上，清空主入口的临时错误显示。
  if (status.connected) primaryError.value = '';
}

const messageListener = (message: any) => {
  if (message?.type === 'SERVER_STATUS_CHANGED') {
    syncConnectionStatus(message.payload);
  }
};

// ── 生命周期 ──────────────────────────────────────────────────────────
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
  margin-bottom: 16px;
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

.connection-form {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

/* 错误提示 */
.primary-error {
  display: flex;
  gap: 8px;
  padding: 10px 12px;
  background: #FEF2F2;
  border: 1px solid #FECACA;
  border-radius: 8px;
  font-size: 13px;
  color: #B91C1C;
  line-height: 1.4;
}

.primary-error__icon {
  flex-shrink: 0;
}

.primary-error__text {
  word-break: break-word;
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

/* Advanced 折叠 */
.advanced-collapse {
  background: #FFFFFF;
  border-radius: 8px;
  border: 1px solid #E5E7EB;
}

.advanced-collapse :deep(.el-collapse-item__header) {
  padding: 0 16px;
  border-bottom: 1px solid #E5E7EB;
  background: #F9FAFB;
  font-weight: 500;
}

.advanced-collapse :deep(.el-collapse-item__wrap) {
  border-bottom: none;
}

.advanced-title {
  font-size: 14px;
  color: #1F2937;
  font-weight: 500;
}

.advanced-card {
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.advanced-desc {
  margin: 0 0 4px;
  font-size: 12px;
  color: #6B7280;
  line-height: 1.5;
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
