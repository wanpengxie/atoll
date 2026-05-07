<template>
  <div class="tool-card" :class="{ disabled: !enabled }">
    <div class="tool-header">
      <el-icon class="tool-icon" :size="20">
        <component :is="iconComponent" />
      </el-icon>
      <div class="tool-info">
        <h3 class="tool-name">{{ tool.displayName }}</h3>
        <p class="tool-description">{{ tool.description }}</p>
      </div>
    </div>

    <div class="tool-actions">
      <el-button
        size="small"
        type="primary"
        :disabled="!enabled"
        :loading="loading"
        @click="handleExecute"
      >
        {{ loading ? '执行中...' : '执行' }}
      </el-button>

      <el-button size="small" text @click="showConfig = !showConfig" v-if="tool.hasConfig">
        <el-icon><Setting /></el-icon>
      </el-button>
    </div>

    <el-collapse-transition>
      <div v-if="showConfig" class="tool-config">
        <slot name="config"></slot>
      </div>
    </el-collapse-transition>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import {
  Document,
  Search,
  Upload,
  ChatLineRound,
  User,
  List,
  Setting,
} from '@element-plus/icons-vue';

interface ToolInfo {
  name: string;
  displayName: string;
  description: string;
  icon?: string;
  hasConfig?: boolean;
}

interface Props {
  tool: ToolInfo;
  enabled?: boolean;
  loading?: boolean;
}

const props = withDefaults(defineProps<Props>(), {
  enabled: true,
  loading: false,
});

const emit = defineEmits<{
  execute: [tool: string];
}>();

const showConfig = ref(false);

const iconMap: Record<string, any> = {
  'check-login': User,
  publish: Upload,
  list: List,
  search: Search,
  detail: Document,
  comment: ChatLineRound,
};

const iconComponent = computed(() => {
  return iconMap[props.tool.icon || 'default'] || Document;
});

const handleExecute = () => {
  emit('execute', props.tool.name);
};
</script>

<style scoped>
.tool-card {
  background: #FFFFFF;
  border-radius: 8px;
  padding: 20px;
  margin-bottom: 16px;
  border: 1px solid #E5E7EB;
  transition: all 0.2s;
}

.tool-card:hover:not(.disabled) {
  border-color: #D1D5DB;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
  transform: translateY(-1px);
}

.tool-card.disabled {
  opacity: 0.5;
  cursor: not-allowed;
  background: #F8F9FA;
}

.tool-header {
  display: flex;
  align-items: flex-start;
  gap: 14px;
  margin-bottom: 16px;
}

.tool-icon {
  color: #000000;
  margin-top: 2px;
}

.tool-info {
  flex: 1;
}

.tool-name {
  margin: 0 0 6px 0;
  font-size: 15px;
  font-weight: 600;
  color: #000000;
  letter-spacing: -0.01em;
}

.tool-description {
  margin: 0;
  font-size: 13px;
  color: #6B7280;
  line-height: 1.6;
}

.tool-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}

.tool-config {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid #F3F4F6;
}
</style>
