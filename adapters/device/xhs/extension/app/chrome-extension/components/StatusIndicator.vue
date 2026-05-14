<template>
  <div class="status-indicator">
    <div class="status-dot" :class="statusClass"></div>
    <span class="status-text">{{ statusText }}</span>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';

interface Props {
  status: 'connected' | 'disconnected' | 'error' | 'loading';
  text?: string;
}

const props = defineProps<Props>();

const statusClass = computed(() => {
  return `status-${props.status}`;
});

const statusText = computed(() => {
  if (props.text) return props.text;

  const defaultTexts = {
    connected: '已连接',
    disconnected: '未连接',
    error: '连接错误',
    loading: '连接中...',
  };

  return defaultTexts[props.status];
});
</script>

<style scoped>
.status-indicator {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 8px 14px;
  border-radius: 6px;
  white-space: nowrap;
  flex-shrink: 0;
  transition: all 0.3s ease;
}

/* 已连接 - 绿色背景 */
.status-indicator:has(.status-connected) {
  background: #ECFDF5;
  border: 1px solid #10B981;
}

/* 未连接 - 灰色背景 */
.status-indicator:has(.status-disconnected) {
  background: #F3F4F6;
  border: 1px solid #E5E7EB;
}

/* 错误 - 红色背景 */
.status-indicator:has(.status-error) {
  background: #FEF2F2;
  border: 1px solid #EF4444;
}

/* 连接中 - 蓝色背景 */
.status-indicator:has(.status-loading) {
  background: #EFF6FF;
  border: 1px solid #3B82F6;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.status-dot.status-connected {
  background-color: #10B981;
  box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.2);
  animation: pulse-green 2s ease-in-out infinite;
}

.status-dot.status-disconnected {
  background-color: #9CA3AF;
  animation: none;
}

.status-dot.status-error {
  background-color: #EF4444;
  box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.2);
  animation: pulse-red 1.5s ease-in-out infinite;
}

.status-dot.status-loading {
  background-color: #3B82F6;
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
  animation: pulse-blue 1s ease-in-out infinite;
}

.status-text {
  font-size: 13px;
  font-weight: 600;
  white-space: nowrap;
}

.status-indicator:has(.status-connected) .status-text {
  color: #059669;
}

.status-indicator:has(.status-disconnected) .status-text {
  color: #6B7280;
}

.status-indicator:has(.status-error) .status-text {
  color: #DC2626;
}

.status-indicator:has(.status-loading) .status-text {
  color: #2563EB;
}

@keyframes pulse-green {
  0%, 100% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.6;
    transform: scale(1.1);
  }
}

@keyframes pulse-red {
  0%, 100% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.6;
    transform: scale(1.1);
  }
}

@keyframes pulse-blue {
  0%, 100% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.6;
    transform: scale(1.1);
  }
}
</style>
