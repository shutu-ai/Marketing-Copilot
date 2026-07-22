<template>
  <el-container style="height: 100vh">
    <el-aside :width="collapse ? '64px' : '220px'" class="aside">
      <div class="logo">
        <span v-if="!collapse">Marketing Copilot</span>
        <span v-else>MC</span>
      </div>
      <Sidebar :collapse="collapse" />
    </el-aside>
    <el-container>
      <el-header style="height: 56px; padding: 0">
        <TopBar :title="pageTitle" @toggle="collapse = !collapse" />
      </el-header>
      <el-main style="background: #f5f7fa; padding: 20px; overflow-y: auto">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRoute } from 'vue-router'
import { useI18n } from 'vue-i18n'
import Sidebar from './Sidebar.vue'
import TopBar from './TopBar.vue'

const collapse = ref(false)
const route = useRoute()
const { t } = useI18n()

const pageTitle = computed(() => {
  const map: Record<string, string> = {
    Dashboard: t('dashboard.title'),
    DataMonitor: t('monitor.title'),
    SmppConfig: t('smpp.title'),
    WhitelistUpload: t('whitelist.title'),
    UploadHistory: t('whitelist.history'),
    SmsContent: t('smsContent.title'),
    DbQuery: t('dbQuery.title'),
    DBConfig: t('dbConfig.title'),
    SmsLog: t('smsLog.title'),
    BatchCampaign: t('campaign.batchTitle'),
    RealtimeCampaign: t('campaign.realtimeTitle'),
    CampaignDetail: t('campaign.detailTitle'),
    AppSettings: t('settings.title'),
  }
  return map[route.name as string] || ''
})
</script>

<style scoped>
.aside {
  background-color: #1d1e1f;
  transition: width 0.3s;
  overflow: hidden;
}
.logo {
  height: 56px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #fff;
  font-size: 16px;
  font-weight: bold;
  border-bottom: 1px solid #333;
  white-space: nowrap;
}
</style>
