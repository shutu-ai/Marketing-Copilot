<template>
  <div class="top-bar">
    <div class="top-bar-left">
      <el-button text @click="emit('toggle')" style="font-size: 20px">
        <el-icon><Fold /></el-icon>
      </el-button>
      <el-breadcrumb separator="/" style="margin-left: 16px">
        <el-breadcrumb-item :to="{ path: '/' }">Home</el-breadcrumb-item>
        <el-breadcrumb-item v-if="title">{{ title }}</el-breadcrumb-item>
      </el-breadcrumb>
    </div>
    <div class="top-bar-right">
      <el-select
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
  height: 56px;
  padding: 0 20px;
  background: #fff;
  border-bottom: 1px solid #e4e7ed;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
}
.top-bar-left {
  display: flex;
  align-items: center;
}
.top-bar-right {
  display: flex;
  align-items: center;
  gap: 12px;
}
</style>
