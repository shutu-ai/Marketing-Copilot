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
        <el-descriptions-item :label="$t('campaign.watchDir')" :span="2"><code style="font-size: 12px">{{ campaign?.watch_dir || '-' }}</code></el-descriptions-item>
       <el-descriptions-item :label="$t('campaign.watchFilenamePrefix')" :span="2"><code style="font-size: 12px">{{ campaign?.watch_filename_prefix || '-' }}</code></el-descriptions-item>
       <el-descriptions-item :label="$t('campaign.phonePrefix')" :span="2"><code style="font-size: 12px">{{ campaign?.phone_prefix || '-' }}</code></el-descriptions-item>
       <el-descriptions-item :label="$t('campaign.dirRoot')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir || '-' }}</code></el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.dirWhitelist')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\whitelist\\') : '-' }}</code></el-descriptions-item>
        <el-descriptions-item label="Inbox" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\inbox\\') : '-' }}</code></el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.dirReceived')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\received\\') : '-' }}</code></el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.dirOutput')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\output\\') : '-' }}</code></el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-card shadow="never">
      <div class="tabs-with-refresh">
      <el-tabs v-model="activeTab">
        <el-tab-pane label="实时接收数据" name="received">
          <div class="summary-row">
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.receivedFiles') }}</div>
              <div class="summary-value primary">{{ stats.file_count }}</div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.eventUsers') }}</div>
              <div class="summary-value primary">{{ stats.total_msisdn_sum }}</div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.matchedUsers') }}</div>
              <div class="summary-value success">{{ stats.total_matched }}</div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.reachedUsers') }}</div>
              <div class="summary-value warning">{{ campaign?.reached_users ?? '-' }}</div>
            </div>
          </div>

          <el-table :data="receivedFiles" stripe style="width: 100%" v-loading="filesLoading">
            <el-table-column prop="filename" :label="$t('campaign.filename')" min-width="200" />
            <el-table-column prop="total_msisdn" :label="$t('campaign.receiveFileTotal')" width="140" />
            <el-table-column prop="matched_count" :label="$t('campaign.receiveFileMatched')" width="140" />
            <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
          </el-table>
          <div class="received-pagination">
            <el-pagination
              v-model:current-page="receivedPage"
              :page-size="receivedPageSize"
              :page-sizes="[10, 25, 50, 100]"
              :total="files.length"
              layout="total, sizes, prev, pager, next"
              @size-change="handleReceivedSizeChange"
              @current-change="handleReceivedCurrentChange"
            />
          </div>
        </el-tab-pane>
        <el-tab-pane label="已发送记录" name="sent">
          <el-table :data="sentRecordsDisplay" stripe style="width: 100%" v-loading="sentRecordsLoading">
            <el-table-column prop="date" label="日期" width="140" />
            <el-table-column prop="filename" label="文件名" min-width="220" />
            <el-table-column prop="user_count" label="用户数" width="100" />
            <el-table-column label="下载发送记录" width="150" fixed="right">
              <template #default="{ row }">
                <el-button text type="primary" size="small" @click="downloadSentRecord(row)">
                  下载
                </el-button>
              </template>
            </el-table-column>
          </el-table>
          <div class="received-pagination">
            <el-pagination
              v-model:current-page="sentRecordsPage"
              :page-size="sentRecordsPageSize"
              :page-sizes="[10, 25, 50, 100]"
              :total="sentRecords.length"
              layout="total, sizes, prev, pager, next"
              @size-change="handleSentRecordsSizeChange"
              @current-change="handleSentRecordsCurrentChange"
            />
          </div>
        </el-tab-pane>
      </el-tabs>
      <el-button class="tabs-refresh" size="small" :loading="activeTab === 'sent' ? sentRecordsLoading : filesLoading" @click="refreshActiveTab">
        <el-icon><Refresh /></el-icon>
        {{ $t('common.refresh') }}
      </el-button>
      </div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, reactive, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import StatusTag from '@/components/StatusTag.vue'
import * as api from '@/api/realtime-campaign'
import * as rtApi from '@/api/realtime'

const route = useRoute()
const campaign = ref<any>(null)
const files = ref<any[]>([])
const filesLoading = ref(false)
const sentRecords = ref<any[]>([])
const sentRecordsLoading = ref(false)
const activeTab = ref('received')
const stats = reactive({ file_count: 0, total_msisdn_sum: 0, total_matched: 0 })

function asArray(value: any): any[] {
  if (Array.isArray(value)) return value
  if (value && typeof value === 'object') {
    if (Array.isArray(value.items)) return value.items
    if (Number.isFinite(value.length)) return Array.from(value)
  }
  return []
}

const RECEIVED_PAGE_SIZE_KEY = 'mc.realtime-received.page-size'
const receivedPage = ref(1)
const receivedPageSize = ref(Number(localStorage.getItem(RECEIVED_PAGE_SIZE_KEY)) || 25)
const receivedFiles = computed(() => {
  const start = (receivedPage.value - 1) * receivedPageSize.value
  return files.value.slice(start, start + receivedPageSize.value)
})
const SENT_RECORDS_PAGE_SIZE_KEY = 'mc.realtime-sent.page-size'
const sentRecordsPage = ref(1)
const sentRecordsPageSize = ref(Number(localStorage.getItem(SENT_RECORDS_PAGE_SIZE_KEY)) || 25)
const sentRecordsDisplay = computed(() => {
  const start = (sentRecordsPage.value - 1) * sentRecordsPageSize.value
  return sentRecords.value.slice(start, start + sentRecordsPageSize.value)
})

function handleReceivedSizeChange(size: number) {
  receivedPageSize.value = size
  localStorage.setItem(RECEIVED_PAGE_SIZE_KEY, String(size))
  receivedPage.value = 1
}

function handleReceivedCurrentChange(page: number) {
  receivedPage.value = page
}

function handleSentRecordsSizeChange(size: number) {
  sentRecordsPageSize.value = size
  localStorage.setItem(SENT_RECORDS_PAGE_SIZE_KEY, String(size))
  sentRecordsPage.value = 1
}

function handleSentRecordsCurrentChange(page: number) {
  sentRecordsPage.value = page
}

async function loadSentRecords() {
  sentRecordsLoading.value = true
  try {
    const rr = await rtApi.getRealtimeSentRecords(Number(route.params.id))
    sentRecords.value = asArray(rr.data)
    sentRecordsPage.value = 1
  } catch { /* ignore */ }
  sentRecordsLoading.value = false
}

async function loadReceivedData() {
  const id = Number(route.params.id)
  try {
    const sr = await rtApi.getRealtimeStats(id)
    Object.assign(stats, sr.data)
  } catch { /* ignore */ }
  filesLoading.value = true
  try {
    const fr = await rtApi.getRealtimeFiles(id)
    files.value = asArray(fr.data)
    receivedPage.value = 1
  } catch { /* ignore */ }
  filesLoading.value = false
}

function refreshActiveTab() {
  if (activeTab.value === 'sent') void loadSentRecords()
  else void loadReceivedData()
}

async function loadDetail() {
  const id = Number(route.params.id)
  try {
    const res = await api.getRealtimeCampaignDetail(id)
    campaign.value = res.data
  } catch (e) {
    console.error(e)
  }
  await loadReceivedData()
  await loadSentRecords()
}

async function downloadSentRecord(row: any) {
  const id = Number(route.params.id)
  const response = await rtApi.downloadRealtimeSentRecord(id, row.filename)
  const url = URL.createObjectURL(response.data)
  const link = document.createElement('a')
  link.href = url
  link.download = row.filename
  link.click()
  URL.revokeObjectURL(url)
}

onMounted(loadDetail)
watch(activeTab, (tab) => {
  if (tab === 'sent') void loadSentRecords()
})
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 12px;
}
.page-header h2 { margin: 0; font-size: 18px; font-weight: 600; }
.tabs-with-refresh {
  position: relative;
}
.tabs-refresh {
  position: absolute;
  top: 6px;
  right: 8px;
  z-index: 2;
}
.summary-row {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}
.summary-card {
  flex: 1;
  background: var(--mc-surface-muted);
  border: 1px solid var(--mc-border);
  border-radius: var(--mc-radius);
  padding: 14px 12px;
  text-align: center;
}
.summary-label {
  font-size: 12px;
  color: var(--mc-text-tertiary);
  margin-bottom: 6px;
}
.summary-value {
  font-size: 20px;
  font-weight: 700;
}
.summary-value.primary { color: var(--mc-primary-hover); }
.summary-value.success { color: var(--mc-success); }
.summary-value.warning { color: var(--mc-warning); }
.received-pagination {
  display: flex;
  justify-content: flex-end;
  margin-top: 12px;
}
</style>
