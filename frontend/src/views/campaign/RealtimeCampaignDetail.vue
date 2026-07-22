<template>
  <div class="campaign-detail">
    <div class="page-header">
      <el-button text @click="$router.back()">
        <el-icon><ArrowLeft /></el-icon> {{ $t('common.back') }}
      </el-button>
      <h2>{{ campaign?.name || $t('campaign.detailTitle') }}</h2>
    </div>

    <el-card shadow="never" style="margin-bottom: 12px">
      <template #header>{{ $t('campaign.basicInfo') }}</template>
      <el-descriptions :column="2" border size="small" v-if="campaign">
        <el-descriptions-item :label="$t('campaign.name')" :span="2">{{ campaign.name }}</el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.startTime')">{{ campaign.start_time || '-' }}</el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.endTime')">{{ campaign.end_time || '-' }}</el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.sendStartTime')">{{ campaign.send_start_time || '-' }}</el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.sendEndTime')">{{ campaign.send_end_time || '-' }}</el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.status')">
          <StatusTag :status="campaign.status">{{ $t('campaign.status_' + campaign.status) }}</StatusTag>
        </el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.customerList')">
          {{ campaign.customer_list_filename || '-' }}
          <el-tag v-if="campaign.row_count" size="small" type="info" style="margin-left: 4px">{{ campaign.row_count }} rows</el-tag>
        </el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.sendDays')">
          <template v-if="campaign.send_days?.length">
            <el-tag v-for="d in campaign.send_days" :key="d" size="small" style="margin: 1px">
              {{ $t('campaign.day_' + d) }}
            </el-tag>
          </template>
          <span v-else>-</span>
        </el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.dirRoot')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir || '-' }}</code></el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.dirWhitelist')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\whitelist\\') : '-' }}</code></el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.dirReceived')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\received\\') : '-' }}</code></el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.dirOutput')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\output\\') : '-' }}</code></el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-card shadow="never">
      <template #header>{{ $t('campaign.realtimeFiles') }}</template>
      <div class="summary-row">
        <div class="summary-card">
          <div class="summary-label">{{ $t('campaign.receivedFiles') }}</div>
          <div class="summary-value" style="color: #409eff">{{ stats.file_count }}</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">{{ $t('campaign.eventUsers') }}</div>
          <div class="summary-value" style="color: #409eff">{{ stats.total_msisdn_sum }}</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">{{ $t('campaign.matchedUsers') }}</div>
          <div class="summary-value" style="color: #67c23a">{{ stats.total_matched }}</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">{{ $t('campaign.reachedUsers') }}</div>
          <div class="summary-value" style="color: #e6a23c">{{ campaign?.reached_users ?? '-' }}</div>
        </div>
      </div>

      <el-table :data="files" stripe style="width: 100%" v-loading="filesLoading">
        <el-table-column prop="filename" :label="$t('campaign.filename')" min-width="200" />
        <el-table-column prop="total_msisdn" :label="$t('campaign.receiveFileTotal')" width="140" />
        <el-table-column prop="matched_count" :label="$t('campaign.receiveFileMatched')" width="140" />
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import StatusTag from '@/components/StatusTag.vue'
import * as api from '@/api/realtime-campaign'
import * as rtApi from '@/api/realtime'

const route = useRoute()
const campaign = ref<any>(null)
const files = ref<any[]>([])
const filesLoading = ref(false)
const stats = reactive({ file_count: 0, total_msisdn_sum: 0, total_matched: 0 })

async function loadDetail() {
  const id = Number(route.params.id)
  try {
    const res = await api.getRealtimeCampaignDetail(id)
    campaign.value = res.data
  } catch (e) {
    console.error(e)
  }
  try {
    const sr = await rtApi.getRealtimeStats(id)
    Object.assign(stats, sr.data)
  } catch { /* ignore */ }
  filesLoading.value = true
  try {
    const fr = await rtApi.getRealtimeFiles(id)
    files.value = fr.data || []
  } catch { /* ignore */ }
  filesLoading.value = false
}

onMounted(loadDetail)
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 12px;
}
.page-header h2 { margin: 0; font-size: 18px; font-weight: 600; }
.summary-row {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}
.summary-card {
  flex: 1;
  background: #f9fafb;
  border-radius: 6px;
  padding: 10px 12px;
  text-align: center;
}
.summary-label {
  font-size: 12px;
  color: #909399;
  margin-bottom: 4px;
}
.summary-value {
  font-size: 20px;
  font-weight: 700;
}
</style>
