<template>
  <div class="top-bar">
    <div class="top-bar-left">
      <el-button class="nav-toggle" text :aria-label="$t('layout.toggleNavigation')" @click="emit('toggle')">
        <el-icon><Fold /></el-icon>
      </el-button>
      <el-breadcrumb separator="/">
        <el-breadcrumb-item :to="{ path: '/' }">{{ $t('layout.home') }}</el-breadcrumb-item>
        <el-breadcrumb-item v-if="title">{{ title }}</el-breadcrumb-item>
      </el-breadcrumb>
    </div>
    <div class="top-bar-right">
      <el-select
        :aria-label="$t('layout.language')"
        :model-value="currentLocale"
        @update:model-value="switchLocale"
        size="small"
        style="width: 120px"
      >
        <el-option label="中文" value="zh-CN" />
        <el-option label="English" value="en" />
      </el-select>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'

import { useElementPlusLocale } from '@/utils/locale'

const props = defineProps<{ title: string }>()
const emit = defineEmits<{ toggle: [] }>()

const { locale } = useI18n()
const currentLocale = computed(() => locale.value)

function switchLocale(val: string) {
  locale.value = val
  localStorage.setItem('locale', val)
  useElementPlusLocale(val)
}
</script>

<style scoped>
.top-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 64px;
  padding: 0 28px;
  background: rgb(255 255 255 / 94%);
  border-bottom: 1px solid var(--mc-border);
  box-shadow: 0 1px 0 rgb(35 64 60 / 3%);
  backdrop-filter: blur(12px);
}
.top-bar-left {
  display: flex;
  align-items: center;
  gap: 18px;
}
.top-bar-right {
  display: flex;
  align-items: center;
  gap: 12px;
}
.nav-toggle {
  width: 36px;
  min-height: 36px;
  padding: 0;
  color: var(--mc-text-secondary);
  font-size: 19px;
  background: var(--mc-surface-muted);
  border-radius: 10px;
}
:deep(.el-breadcrumb__inner) {
  color: var(--mc-text-secondary);
  font-weight: 550;
}
:deep(.el-breadcrumb__item:last-child .el-breadcrumb__inner) {
  color: var(--mc-text);
  font-weight: 700;
}
@media (max-width: 767px) {
  .top-bar { padding: 0 14px; }
  :deep(.el-breadcrumb__item:first-child) { display: none; }
}
</style>
