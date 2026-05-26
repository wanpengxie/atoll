<template>
  <div class="popup-container">
    <header class="popup-header">
      <div class="header-content">
        <img src="/icon-128.png" alt="Logo" class="logo" />
        <div class="header-text">
          <h1>Coagent · 小红书 Device</h1>
        </div>
      </div>
    </header>

    <main class="popup-main">
      <section class="connection-card">
        <div class="connection-card__header">
          <div>
            <h2>本机 proxy daemon</h2>
            <p>启动 coagent-proxy 后连接本机端口。此模式不需要 token 或配对码。</p>
          </div>
          <StatusIndicator :status="proxyStatusKind" :text="proxyStatusText" />
        </div>

        <el-form label-position="top" class="connection-form">
          <el-form-item label="Proxy endpoint" class="server-url-item">
            <el-input v-model="proxyEndpoint" :placeholder="defaultProxyEndpoint" />
          </el-form-item>

          <div class="connection-form__actions">
            <el-button
              type="primary"
              :loading="proxyConnecting"
              @click="connectProxyDaemon"
              class="action-btn action-btn--primary"
            >
              连接本机 proxy daemon
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
    </main>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from 'vue';
import { ElMessage } from 'element-plus';
import { useAppStore } from '@/stores/app';
import StatusIndicator from '@/components/StatusIndicator.vue';
import { getDefaultProxyEndpoint } from '@/entrypoints/background/connection-state';

const store = useAppStore();
const defaultProxyEndpoint = getDefaultProxyEndpoint();
const proxyEndpoint = ref(defaultProxyEndpoint);
const proxyConnecting = ref(false);
const proxyProbeState = ref<'idle' | 'detected' | 'not_detected'>('idle');

type StatusKind = 'connected' | 'disconnected' | 'error' | 'loading';

const proxyStatusKind = computed<StatusKind>(() => {
  if (proxyConnecting.value) return 'loading';
  if (store.connectionStatus.connected) return 'connected';
  if (proxyProbeState.value === 'not_detected' || store.connectionStatus.lastError) return 'error';
  return 'disconnected';
});

const proxyStatusText = computed(() => {
  if (proxyConnecting.value) return '探寻中...';
  if (store.connectionStatus.connected) return '已连接本机 daemon';
  if (proxyProbeState.value === 'not_detected') return '未检测到本机 daemon';
  return store.connectionStatus.lastError || '未连接';
});

const loadConnectionConfig = async () => {
  const response = await chrome.runtime.sendMessage({ type: 'GET_CONNECTION_CONFIG' });
  if (response?.success) {
    const c = response.config ?? {};
    proxyEndpoint.value = c.proxyEndpoint || defaultProxyEndpoint;
  }
};

const connectProxyDaemon = async () => {
  proxyConnecting.value = true;
  try {
    const response = await chrome.runtime.sendMessage({
      type: 'CONNECT_PROXY_DAEMON',
      payload: {
        proxyEndpoint: proxyEndpoint.value || defaultProxyEndpoint,
      },
    });
    if (response?.success) {
      proxyProbeState.value = 'detected';
      ElMessage.success('已连接本机 proxy daemon');
    } else {
      proxyProbeState.value = 'not_detected';
      ElMessage.error(response?.error || '未检测到本机 proxy daemon');
    }
  } finally {
    proxyConnecting.value = false;
  }
};

const disconnectDevice = async () => {
  await chrome.runtime.sendMessage({ type: 'DISCONNECT_DEVICE' });
  proxyProbeState.value = 'idle';
  ElMessage.success('已断开 daemon');
};

function syncConnectionStatus(status: any) {
  store.updateConnectionStatus({
    connected: Boolean(status.connected),
    serverUrl: status.serverUrl || store.connectionStatus.serverUrl,
    reconnecting: Boolean(status.reconnecting),
    lastError: status.lastError,
    lastUpdated: status.lastUpdated,
  });
  if (status.connected) proxyProbeState.value = 'detected';
}

const messageListener = (message: any) => {
  if (message?.type === 'SERVER_STATUS_CHANGED') {
    syncConnectionStatus(message.payload);
  }
};

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
  min-height: 360px;
  display: flex;
  flex-direction: column;
  background: #f8f9fa;
}

.popup-header {
  background: #ffffff;
  color: #1f2937;
  padding: 20px;
  border-bottom: 1px solid #e5e7eb;
}

.header-content {
  display: flex;
  align-items: center;
  gap: 14px;
}

.logo {
  width: 44px;
  height: 44px;
  border-radius: 8px;
  border: 1px solid #e5e7eb;
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
  background: #ffffff;
  border-radius: 8px;
  padding: 20px;
  border: 1px solid #e5e7eb;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.connection-card__header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}

.connection-card__header > div:first-child {
  flex: 1;
  min-width: 0;
}

.connection-card h2 {
  margin: 0 0 6px;
  font-size: 16px;
  color: #111827;
}

.connection-card p {
  margin: 0;
  font-size: 13px;
  line-height: 1.5;
  color: #6b7280;
}

.connection-form {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.server-url-item {
  margin-bottom: 0;
}

.connection-form__actions {
  display: flex;
  gap: 10px;
  align-items: center;
}

.action-btn {
  flex: 1;
}
</style>
