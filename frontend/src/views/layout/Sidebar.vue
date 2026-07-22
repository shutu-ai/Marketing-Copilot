<template>
  <el-menu
    :default-active="route.path"
    :collapse="collapse"
    router
    background-color="#1d1e1f"
    text-color="#bfcbd9"
    active-text-color="#409eff"
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
    <el-menu-item index="/smpp-config">
      <el-icon><Connection /></el-icon>
      <span>{{ $t('menu.smppConfig') }}</span>
    </el-menu-item>
    <el-sub-menu index="/whitelist">
      <template #title>
        <el-icon><UserFilled /></el-icon>
        <span>{{ $t('menu.whitelist') }}</span>
      </template>
      <el-menu-item index="/whitelist/upload">
        <el-icon><Upload /></el-icon>
        <span>{{ $t('menu.whitelistUpload') }}</span>
      </el-menu-item>
      <el-menu-item index="/whitelist/history">
        <el-icon><Clock /></el-icon>
        <span>{{ $t('menu.uploadHistory') }}</span>
      </el-menu-item>
    </el-sub-menu>
    <el-menu-item index="/sms-content">
      <el-icon><ChatDotSquare /></el-icon>
      <span>{{ $t('menu.smsContent') }}</span>
    </el-menu-item>
    <el-menu-item index="/sms-tasks">
      <el-icon><List /></el-icon>
      <span>短信任务</span>
    </el-menu-item>
    <el-menu-item index="/operations">
      <el-icon><Operation /></el-icon>
      <span>运营中心</span>
    </el-menu-item>
    <el-menu-item index="/capacity">
      <el-icon><TrendCharts /></el-icon>
      <span>容量与高可用</span>
    </el-menu-item>
    <el-menu-item index="/db-query">
      <el-icon><Search /></el-icon>
      <span>{{ $t('menu.dbQuery') }}</span>
    </el-menu-item>
    <el-menu-item index="/db-config">
      <el-icon><Setting /></el-icon>
      <span>{{ $t('menu.dbConfig') }}</span>
    </el-menu-item>
    <el-menu-item index="/settings">
      <el-icon><Tools /></el-icon>
      <span>{{ $t('menu.settings') }}</span>
    </el-menu-item>
    <el-sub-menu index="/rabbitmq">
      <template #title>
        <el-icon><Connection /></el-icon>
        <span>{{ $t('menu.rabbitmq') }}</span>
      </template>
      <el-menu-item index="/rabbitmq">
        <el-icon><Setting /></el-icon>
        <span>{{ $t('menu.rabbitmqConfig') }}</span>
      </el-menu-item>
      <el-menu-item index="/sms-log">
        <el-icon><List /></el-icon>
        <span>{{ $t('menu.smsLog') }}</span>
      </el-menu-item>
    </el-sub-menu>
  </el-menu>
</template>

<script setup lang="ts">
import { useRoute } from 'vue-router'

defineProps<{ collapse: boolean }>()

const route = useRoute()
</script>
