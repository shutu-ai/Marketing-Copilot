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
        <el-descriptions-item :label="$t('campaign.whitelistIndex')" :span="2">
          <div class="whitelist-index-summary">
            <el-tag :type="whitelistIndexTagType(campaign.whitelist_index?.status)" size="small">
              {{ $t(`campaign.whitelistIndexStatus_${campaign.whitelist_index?.status || 'missing'}`) }}
            </el-tag>
            <span v-if="campaign.whitelist_index?.version_no">{{ $t('campaign.whitelistIndexVersion', { version: campaign.whitelist_index.version_no }) }}</span>
            <span v-if="campaign.whitelist_index?.status === 'ready'">
              {{ $t('campaign.whitelistIndexRows', { rows: campaign.whitelist_index.normalized_count || 0 }) }}
            </span>
            <el-tag v-if="campaign.whitelist_index?.waiting_file_count" type="warning" size="small">
              {{ $t('campaign.whitelistWaitingFiles', { count: campaign.whitelist_index.waiting_file_count }) }}
            </el-tag>
          </div>
          <div v-if="campaign.whitelist_index?.error_detail" class="whitelist-index-error">
            {{ campaign.whitelist_index.error_detail }}
          </div>
          <div v-if="campaign.whitelist_index?.index_path" class="whitelist-index-path">
            <code>{{ campaign.whitelist_index.index_path }}</code>
          </div>
        </el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.sendDays')">
          <template v-if="campaign.send_days?.length">
            <el-tag v-for="d in campaign.send_days" :key="d" size="small" style="margin: 1px">
              {{ $t('campaign.day_' + d) }}
            </el-tag>
          </template>
          <span v-else>-</span>
        </el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.watchSource')">{{ campaign?.watch_source === 'sftp' ? 'SSH / SFTP' : $t('campaign.localWatchSource') }}</el-descriptions-item>
        <el-descriptions-item v-if="campaign?.watch_source === 'sftp'" :label="$t('campaign.sftpHost')">{{ campaign?.sftp_username }}@{{ campaign?.sftp_host }}:{{ campaign?.sftp_port }}</el-descriptions-item>
        <el-descriptions-item :label="$t('campaign.watchDir')" :span="2"><code style="font-size: 12px">{{ campaign?.watch_dir || '-' }}</code></el-descriptions-item>
       <el-descriptions-item :label="$t('campaign.watchFilenamePrefix')" :span="2"><code style="font-size: 12px">{{ campaign?.watch_filename_prefix || '-' }}</code></el-descriptions-item>
       <el-descriptions-item :label="$t('campaign.phonePrefix')" :span="2"><code style="font-size: 12px">{{ campaign?.phone_prefix || '-' }}</code></el-descriptions-item>
       <el-descriptions-item :label="$t('campaign.associateSmpp')" :span="2">{{ smppName(campaign?.smpp_config_id) }}</el-descriptions-item>
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
        <el-tab-pane :label="$t('legacy.x911b40e5')" name="received">
          <div class="summary-row">
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.receivedFiles') }}</div>
              <div class="summary-value primary">
                <div><span class="summary-period">{{ $t('campaign.cumulative') }}</span>{{ stats.file_count }}</div>
                <div class="summary-today"><span class="summary-period">{{ $t('campaign.today') }}</span>{{ stats.today_file_count }}</div>
              </div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.eventUsers') }}</div>
              <div class="summary-value primary">
                <div><span class="summary-period">{{ $t('campaign.cumulative') }}</span>{{ stats.total_msisdn_sum }}</div>
                <div class="summary-today"><span class="summary-period">{{ $t('campaign.today') }}</span>{{ stats.today_msisdn_sum }}</div>
              </div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.matchedUsers') }}</div>
              <div class="summary-value success">
                <div><span class="summary-period">{{ $t('campaign.cumulative') }}</span>{{ stats.total_matched }}</div>
                <div class="summary-today"><span class="summary-period">{{ $t('campaign.today') }}</span>{{ stats.today_matched }}</div>
              </div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.taskCreatedUsers') }}</div>
              <div class="summary-value warning">
                <div><span class="summary-period">{{ $t('campaign.cumulative') }}</span>{{ stats.task_created_users }}</div>
                <div class="summary-today"><span class="summary-period">{{ $t('campaign.today') }}</span>{{ stats.today_task_created_users }}</div>
              </div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.acceptedUsers') }}</div>
              <div class="summary-value primary">
                <div><span class="summary-period">{{ $t('campaign.cumulative') }}</span>{{ stats.accepted_users }}</div>
                <div class="summary-today"><span class="summary-period">{{ $t('campaign.today') }}</span>{{ stats.today_accepted_users }}</div>
              </div>
            </div>
            <div class="summary-card">
              <div class="summary-label">{{ $t('campaign.deliveredUsers') }}</div>
              <div class="summary-value success">
                <div><span class="summary-period">{{ $t('campaign.cumulative') }}</span>{{ stats.delivered_users }}</div>
                <div class="summary-today"><span class="summary-period">{{ $t('campaign.today') }}</span>{{ stats.today_delivered_users }}</div>
              </div>
            </div>
          </div>

          <el-table :data="receivedFiles" stripe style="width: 100%" v-loading="filesLoading">
            <el-table-column prop="filename" :label="$t('campaign.filename')" min-width="200" />
            <el-table-column prop="total_msisdn" :label="$t('campaign.receiveFileTotal')" width="140" />
            <el-table-column prop="matched_count" :label="$t('campaign.receiveFileMatched')" width="140" />
            <el-table-column prop="task_count" :label="$t('campaign.taskCreatedUsers')" width="140" />
            <el-table-column prop="delivered_count" :label="$t('campaign.deliveredUsers')" width="120" />
            <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
          </el-table>
          <div class="received-pagination">
            <el-pagination
              v-model:current-page="receivedPage"
              :page-size="receivedPageSize"
              :page-sizes="[10, 25, 50, 100]"
              :total="receivedTotal"
              layout="total, sizes, prev, pager, next"
              @size-change="handleReceivedSizeChange"
              @current-change="handleReceivedCurrentChange"
            />
          </div>
        </el-tab-pane>
        <el-tab-pane :label="$t('legacy.xe7502eb1')" name="sent">
          <el-table :data="sentRecords" stripe style="width: 100%" v-loading="sentRecordsLoading">
            <el-table-column prop="date" :label="$t('campaign.date')" width="140" />
            <el-table-column prop="filename" :label="$t('legacy.x29139c2a')" min-width="220" />
            <el-table-column prop="task_count" :label="$t('campaign.taskCreatedUsers')" width="120" />
            <el-table-column prop="accepted_count" :label="$t('campaign.acceptedUsers')" width="120" />
            <el-table-column prop="delivered_count" :label="$t('campaign.deliveredUsers')" width="120" />
            <el-table-column :label="$t('legacy.x82623f9e')" width="150" fixed="right">
              <template #default="{ row }">
                <el-button text type="primary" size="small" @click="downloadSentRecord(row)">
                  {{ $t('campaign.downloadTaskSnapshot') }}
                </el-button>
              </template>
            </el-table-column>
          </el-table>
          <div class="received-pagination">
            <el-pagination
              v-model:current-page="sentRecordsPage"
              :page-size="sentRecordsPageSize"
              :page-sizes="[10, 25, 50, 100]"
              :total="sentRecordsTotal"
              layout="total, sizes, prev, pager, next"
              @size-change="handleSentRecordsSizeChange"
              @current-change="handleSentRecordsCurrentChange"
            />
          </div>
        </el-tab-pane>
        <el-tab-pane :label="$t('campaign.currentWatchFiles')" name="watch-files">
          <el-table :data="watchFiles" stripe style="width: 100%" v-loading="watchFilesLoading">
            <el-table-column prop="filename" :label="$t('campaign.filename')" min-width="220" />
            <el-table-column :label="$t('common.status')" width="130">
              <template #default="{ row }">
                <el-tag :type="watchStatusType(row.status)" size="small">{{ watchStatusLabel(row) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="skip_reason" :label="$t('campaign.watchFileReason')" min-width="260" show-overflow-tooltip />
            <el-table-column :label="$t('monitor.fileSize')" width="120">
              <template #default="{ row }">{{ formatFileSize(row.file_size || 0) }}</template>
            </el-table-column>
            <el-table-column prop="row_count" :label="$t('monitor.rowCount')" width="90" />
          </el-table>
          <div class="received-pagination">
            <el-pagination
              v-model:current-page="watchFilesPage"
              :page-size="watchFilesPageSize"
              :page-sizes="[10, 25, 50, 100]"
              :total="watchFilesTotal"
              layout="total, sizes, prev, pager, next"
              @size-change="handleWatchFilesSizeChange"
              @current-change="handleWatchFilesCurrentChange"
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
import { ref, reactive, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import StatusTag from '@/components/StatusTag.vue'
import * as api from '@/api/realtime-campaign'
import * as rtApi from '@/api/realtime'
import * as smppApi from '@/api/smpp'
import i18n from '@/i18n'
import { formatFileSize } from '@/utils/format'

const route = useRoute()
const campaign = ref<any>(null)
const receivedFiles = ref<any[]>([])
const receivedTotal = ref(0)
const filesLoading = ref(false)
const sentRecords = ref<any[]>([])
const sentRecordsTotal = ref(0)
const sentRecordsLoading = ref(false)
const watchFiles = ref<any[]>([])
const watchFilesTotal = ref(0)
const watchFilesLoading = ref(false)
const activeTab = ref('received')
const stats = reactive({
  file_count: 0,
  total_msisdn_sum: 0,
  total_matched: 0,
  task_created_users: 0,
  accepted_users: 0,
  delivered_users: 0,
  today_file_count: 0,
  today_msisdn_sum: 0,
  today_matched: 0,
  today_task_created_users: 0,
  today_accepted_users: 0,
  today_delivered_users: 0,
  today_reached_users: 0,
})
const smppOptions = ref<any[]>([])

function smppName(configId: number | null) {
  const cfg = smppOptions.value.find((item: any) => item.id === configId)
  return cfg ? `${cfg.name} (${cfg.host}:${cfg.port})` : (configId ? `#${configId}` : '-')
}

async function loadSmppOptions() {
  try {
    const res = await smppApi.getSmppConfigs()
    smppOptions.value = res.data
  } catch {
    // ignore
  }
}

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
const SENT_RECORDS_PAGE_SIZE_KEY = 'mc.realtime-sent.page-size'
const sentRecordsPage = ref(1)
const sentRecordsPageSize = ref(Number(localStorage.getItem(SENT_RECORDS_PAGE_SIZE_KEY)) || 25)
const WATCH_FILES_PAGE_SIZE_KEY = 'mc.realtime-watch-files.page-size'
const watchFilesPage = ref(1)
const watchFilesPageSize = ref(Number(localStorage.getItem(WATCH_FILES_PAGE_SIZE_KEY)) || 25)

function handleReceivedSizeChange(size: number) {
  receivedPageSize.value = size
  localStorage.setItem(RECEIVED_PAGE_SIZE_KEY, String(size))
  receivedPage.value = 1
  void loadReceivedData(false)
}

function handleReceivedCurrentChange(page: number) {
  receivedPage.value = page
  void loadReceivedData(false)
}

function handleSentRecordsSizeChange(size: number) {
  sentRecordsPageSize.value = size
  localStorage.setItem(SENT_RECORDS_PAGE_SIZE_KEY, String(size))
  sentRecordsPage.value = 1
  void loadSentRecords(false)
}

function handleSentRecordsCurrentChange(page: number) {
  sentRecordsPage.value = page
  void loadSentRecords(false)
}

function handleWatchFilesSizeChange(size: number) {
  watchFilesPageSize.value = size
  localStorage.setItem(WATCH_FILES_PAGE_SIZE_KEY, String(size))
  watchFilesPage.value = 1
  void loadWatchFiles(false)
}

function handleWatchFilesCurrentChange(page: number) {
  watchFilesPage.value = page
  void loadWatchFiles(false)
}

async function loadSentRecords(reset = true) {
  sentRecordsLoading.value = true
  try {
    if (reset) sentRecordsPage.value = 1
    const rr = await rtApi.getRealtimeSentRecords(Number(route.params.id), {
      page: sentRecordsPage.value,
      page_size: sentRecordsPageSize.value,
    })
    sentRecords.value = asArray(rr.data)
    sentRecordsTotal.value = rr.data.total ?? sentRecords.value.length
  } catch { /* ignore */ }
  sentRecordsLoading.value = false
}

function watchStatusType(status: string) {
  if (status === 'processed') return 'success' as const
  if (status === 'ignored') return 'info' as const
  return 'warning' as const
}

function whitelistIndexTagType(status?: string) {
  if (status === 'ready') return 'success' as const
  if (status === 'failed' || status === 'missing_index') return 'danger' as const
  if (status === 'disabled') return 'info' as const
  return 'warning' as const
}

function watchStatusLabel(row: any) {
  if (row.status === 'processed') return i18n.global.t('monitor.status_processed')
  if (row.status === 'ignored' && row.match_prefix === false) return i18n.global.t('monitor.status_prefix_mismatch')
  if (row.status === 'ignored') return i18n.global.t('monitor.status_schedule_mismatch')
  if (row.status_label === '写入中') return i18n.global.t('monitor.status_writing')
  return i18n.global.t('monitor.status_pending')
}

async function loadWatchFiles(reset = true) {
  watchFilesLoading.value = true
  try {
    if (reset) watchFilesPage.value = 1
    const wr = await rtApi.getRealtimeWatchFiles(Number(route.params.id), {
      page: watchFilesPage.value,
      page_size: watchFilesPageSize.value,
    })
    watchFiles.value = asArray(wr.data)
    watchFilesTotal.value = wr.data.total ?? watchFiles.value.length
  } catch { /* ignore */ }
  watchFilesLoading.value = false
}

async function loadReceivedData(reset = true) {
  const id = Number(route.params.id)
  try {
    const sr = await rtApi.getRealtimeStats(id)
    Object.assign(stats, sr.data)
  } catch { /* ignore */ }
  filesLoading.value = true
  try {
    if (reset) receivedPage.value = 1
    const fr = await rtApi.getRealtimeFiles(id, {
      page: receivedPage.value,
      page_size: receivedPageSize.value,
    })
    receivedFiles.value = asArray(fr.data)
    receivedTotal.value = fr.data.total ?? receivedFiles.value.length
  } catch { /* ignore */ }
  filesLoading.value = false
}

function refreshActiveTab() {
  void loadCampaignDetail()
  if (activeTab.value === 'sent') void loadSentRecords(true)
  else if (activeTab.value === 'watch-files') void loadWatchFiles(true)
  else void loadReceivedData(true)
}

async function loadCampaignDetail() {
  const id = Number(route.params.id)
  try {
    const res = await api.getRealtimeCampaignDetail(id)
    campaign.value = res.data
  } catch (e) {
    console.error(e)
  }
}

async function loadDetail() {
  await loadCampaignDetail()
  await loadSmppOptions()
  await loadReceivedData(true)
  await loadSentRecords(true)
  await loadWatchFiles(true)
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
  if (tab === 'sent') void loadSentRecords(true)
  if (tab === 'watch-files') void loadWatchFiles(true)
})
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 12px;
}
.page-header h2 { margin: 0; font-size: 18px; font-weight: 600; }
.whitelist-index-summary { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.whitelist-index-error { margin-top: 6px; color: var(--el-color-danger); white-space: pre-wrap; overflow-wrap: anywhere; }
.whitelist-index-path { margin-top: 6px; overflow-wrap: anywhere; }
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
  display: flex; flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 12px;
}
.summary-card {
  flex: 1 1 150px;
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
  line-height: 1.35;
}
.summary-period {
  display: inline-block;
  min-width: 34px;
  margin-right: 4px;
  color: var(--mc-text-tertiary);
  font-size: 11px;
  font-weight: 500;
  vertical-align: middle;
}
.summary-today {
  margin-top: 2px;
  font-size: 15px;
  font-weight: 600;
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
