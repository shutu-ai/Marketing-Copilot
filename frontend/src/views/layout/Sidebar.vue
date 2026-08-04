<template>
  <el-menu
    :default-active="route.path"
    :collapse="collapse"
    unique-opened
    router
    @select="emit('navigate')"
  >
    <el-menu-item index="/dashboard">
      <el-icon><DataBoard /></el-icon>
      <span>{{ $t('menu.dashboard') }}</span>
    </el-menu-item>
    <el-menu-item index="/data-monitor">
      <el-icon><Monitor /></el-icon>
      <span>{{ $t('menu.dataMonitor') }}</span>
    </el-menu-item>
    <el-sub-menu index="/campaign">
      <template #title>
        <el-icon><Promotion /></el-icon>
        <span>{{ $t('menu.campaign') }}</span>
      </template>
      <el-menu-item index="/campaign/batch">
        <el-icon><DocumentCopy /></el-icon>
        <span>{{ $t('menu.batchCampaign') }}</span>
      </el-menu-item>
      <el-menu-item index="/campaign/realtime">
        <el-icon><Lightning /></el-icon>
        <span>{{ $t('menu.realtimeCampaign') }}</span>
      </el-menu-item>
    </el-sub-menu>

    <el-menu-item index="/sms-content">
      <el-icon><ChatDotSquare /></el-icon>
      <span>{{ $t('menu.smsContent') }}</span>
    </el-menu-item>
    <el-menu-item v-if="role !== 'viewer'" index="/operations">
      <el-icon><Operation /></el-icon>
      <span>{{ $t('menu.operations') }}</span>
    </el-menu-item>
    <el-menu-item v-if="role === 'admin'" index="/db-query">
      <el-icon><Search /></el-icon>
      <span>{{ $t('menu.dbQuery') }}</span>
    </el-menu-item>
    <el-menu-item index="/settings">
      <el-icon><Tools /></el-icon>
      <span>{{ $t('menu.settings') }}</span>
    </el-menu-item>
  </el-menu>
</template>

<script setup lang="ts">
import { useRoute } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

defineProps<{ collapse: boolean }>()
const emit = defineEmits<{ navigate: [] }>()

const route = useRoute()
const authStore = useAuthStore()
const role = authStore.role
</script>

<style scoped>
.el-menu {
  --el-menu-bg-color: transparent;
  --el-menu-text-color: var(--mc-sidebar-text);
  --el-menu-hover-bg-color: rgb(255 255 255 / 8%);
  --el-menu-active-color: #273f3d;
  width: 100%;
  padding: 14px 10px 28px;
  border-right: 0;
  background: transparent;
}

.el-menu:not(.el-menu--collapse) {
  width: 248px;
}

:deep(.el-menu-item),
:deep(.el-sub-menu__title) {
  height: 46px;
  margin: 3px 0;
  padding: 0 14px !important;
  border-radius: 12px;
  color: var(--mc-sidebar-text);
  font-size: 13px;
  font-weight: 550;
  line-height: 46px;
}

:deep(.el-menu-item .el-icon),
:deep(.el-sub-menu__title .el-icon) {
  width: 22px;
  margin-right: 11px;
  color: #c7d5d3;
  font-size: 18px;
}

:deep(.el-menu-item:hover),
:deep(.el-sub-menu__title:hover) {
  color: #fff;
  background: rgb(255 255 255 / 8%);
}

:deep(.el-menu-item.is-active) {
  color: #273f3d;
  background: #f7faf9;
  box-shadow: 0 8px 20px rgb(27 43 41 / 15%);
}

:deep(.el-menu-item.is-active .el-icon) {
  color: var(--mc-primary);
}

:deep(.el-sub-menu .el-menu) {
  padding: 2px 0 4px 10px;
}

:deep(.el-sub-menu .el-menu-item) {
  min-width: 0;
  padding-left: 41px !important;
  color: #cfdddb;
  font-size: 12px;
}

:deep(.el-menu--collapse .el-menu-item),
:deep(.el-menu--collapse .el-sub-menu__title) {
  justify-content: center;
  padding: 0 !important;
}

:deep(.el-menu--collapse .el-icon) {
  margin-right: 0;
}
</style>
