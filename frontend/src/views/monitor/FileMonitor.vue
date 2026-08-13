<template>
  <div class="file-monitor">
    <div class="page-header">
      <h2>{{ $t('legacy.x1c5a42c1') }}</h2>
      <p>{{ $t('legacy.x53ebc52d') }}</p>
    </div>

    <el-card shadow="never" style="margin-bottom: 20px">
      <template #header>
        <div class="card-header">
          <span>{{ $t('legacy.x6ba56a23') }}</span>
          <el-tag :type="state.exists ? 'success' : 'warning'" effect="light">
            {{ state.exists ? $t('common.success') : $t('legacy.xd1e4a7e9') }}
          </el-tag>
        </div>
      </template>

      <el-form label-width="160px" size="small">
        <el-form-item :label="$t('legacy.xdc317fa4')">
          <el-select v-model="selectedCampaignId" filterable style="width: 360px" @change="handleCampaignChange">
            <el-option :value="0" :label="$t('common.all')" />
            <el-option
              v-for="campaign in state.campaigns"
              :key="campaign.id"
              :value="campaign.id"
              :label="`#${campaign.id} ${campaign.name}`"
            />
          </el-select>
        </el-form-item>

        <el-form-item :label="$t('campaign.watchDir')">
          <div v-if="state.watch_dirs.length" class="watch-dir-list">
            <div v-for="item in state.watch_dirs" :key="item.directory_path" class="watch-dir-item">
              <code class="watch-dir">{{ item.directory_path }}</code>
              <el-tag size="small" :type="item.exists ? 'success' : 'warning'" effect="light">
                {{ item.exists ? 'OK' : 'Missing' }}
              </el-tag>
              <el-tag v-for="campaign in item.campaigns" :key="campaign.id" size="small" type="info" effect="plain">
                #{{ campaign.id }} {{ campaign.name }} / {{ campaign.watch_filename_prefix || '-' }}
              </el-tag>
            </div>
          </div>
          <code v-else class="watch-dir">-</code>
          <div class="form-tip">{{ $t('legacy.x8b9154d0') }}</div>
        </el-form-item>

        <el-form-item :label="$t('legacy.xd2c06152')">
          <el-tag type="info">{{ state.active_campaigns }}</el-tag>
        </el-form-item>

        <el-form-item>
          <el-button :loading="scanning" @click="scanNow">
            <el-icon><Refresh /></el-icon>
            {{ selectedCampaignId ? $t('legacy.x56525d62') + ' ' + $t('legacy.x36c6f572') : $t('legacy.x56525d62') + ' ' + $t('common.all') + ' ' + $t('legacy.x36c6f572') }}
          </el-button>
          <el-switch
            v-model="autoRefresh"
            :active-text="$t('logViewer.autoRefresh')"
            style="margin-left: 12px"
            @change="handleAutoRefresh"
          />
        </el-form-item>
      </el-form>
    </el-card>

    <el-card shadow="never" style="margin-bottom: 20px">
      <template #header>
        <span>{{ $t('legacy.x6ad37add') }} <el-tag type="info" size="small">{{ state.source_files_total }}</el-tag></span>
      </template>
      <el-table :data="state.source_files" stripe v-loading="loading" style="width: 100%">
        <el-table-column prop="filename" :label="$t('legacy.x29139c2a')" min-width="260" />
        <el-table-column prop="directory_path" :label="$t('campaign.watchDir')" min-width="260" />
        <el-table-column v-if="selectedCampaignId" :label="$t('legacy.xed3388ce')" width="100">
          <template #default="{ row }">
            <el-tag :type="row.match_prefix ? 'success' : 'info'" size="small">
              {{ row.match_prefix ? i18n.global.t('common.yes') : i18n.global.t('common.no') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column v-if="selectedCampaignId" :label="$t('legacy.x0760d694')" width="120">
          <template #default="{ row }">
            <el-tag :type="row.match_schedule ? 'success' : 'info'" size="small">
              {{ row.match_schedule ? i18n.global.t('common.yes') : i18n.global.t('common.no') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="$t('legacy.x6f07f350')" width="120">
          <template #default="{ row }">
            <el-tag :type="row.status === 'processed' ? 'success' : row.status === 'ignored' ? 'info' : 'warning'" size="small">
              {{ statusLabel(row) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="skip_reason" :label="$t('legacy.x41dfb0bf')" min-width="220" show-overflow-tooltip />
        <el-table-column :label="$t('monitor.fileSize')" width="120">
          <template #default="{ row }">{{ formatFileSize(row.file_size || 0) }}</template>
        </el-table-column>
        <el-table-column prop="row_count" :label="$t('monitor.rowCount')" width="90" />
      </el-table>
      <el-pagination
        v-model:current-page="sourcePage"
        :page-size="sourcePageSize"
        :total="state.source_files_total"
        layout="total, prev, pager, next"
        class="pagination"
        @current-change="changeSourcePage"
      />
    </el-card>

    <el-card shadow="never">
      <template #header>
        <span>{{ $t('monitor.files') }} <el-tag type="info" size="small">{{ state.processed_files_total }}</el-tag></span>
      </template>
      <el-table :data="state.processed_files" stripe v-loading="loading" style="width: 100%">
        <el-table-column prop="filename" :label="$t('legacy.x29139c2a')" min-width="220" />
        <el-table-column prop="campaign_id" :label="$t('legacy.xbe3322c4')" width="90" />
        <el-table-column prop="file_path" :label="$t('legacy.x97293964')" min-width="260" />
        <el-table-column :label="$t('monitor.fileSize')" width="120">
          <template #default="{ row }">{{ formatFileSize(row.file_size || 0) }}</template>
        </el-table-column>
        <el-table-column prop="total_rows" :label="$t('monitor.rowCount')" width="90" />
        <el-table-column prop="created_tasks" :label="$t('legacy.x4d2daac9')" width="130" />
        <el-table-column :label="$t('common.status')" width="120">
          <template #default="{ row }">
            <StatusTag :status="row.status" />
          </template>
        </el-table-column>
        <el-table-column prop="detected_at" :label="$t('common.created_at')" width="180" />
      </el-table>
      <el-pagination
        v-model:current-page="processedPage"
        :page-size="processedPageSize"
        :total="state.processed_files_total"
        layout="total, prev, pager, next"
        class="pagination"
        @current-change="changeProcessedPage"
      />
    </el-card>
  </div>
</template>

<script setup lang="ts">
import i18n from '@/i18n'
import { onMounted, onUnmounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { getRealtimeMonitor, scanRealtimeMonitor } from '@/api/monitor'
import { formatFileSize } from '@/utils/format'
import StatusTag from '@/components/StatusTag.vue'

const loading = ref(false)
const scanning = ref(false)
const autoRefresh = ref(false)
const selectedCampaignId = ref(0)
const sourcePage = ref(1)
const sourcePageSize = ref(50)
const processedPage = ref(1)
const processedPageSize = ref(50)
const state = reactive({
  campaign_id: 0,
  watch_dir: '',
  watch_dirs: [] as any[],
  campaigns: [] as any[],
  exists: false,
  active_campaigns: 0,
  source_files: [] as any[],
  source_files_total: 0,
  source_files_page: 1,
  source_files_page_size: 50,
  processed_files: [] as any[],
  processed_files_total: 0,
  processed_files_page: 1,
  processed_files_page_size: 50,
})
let refreshTimer: ReturnType<typeof setInterval> | null = null

function applyState(next: any) {
  state.campaign_id = Number(next.campaign_id || 0)
  state.watch_dir = next.watch_dir || ''
  state.watch_dirs = next.watch_dirs || []
  state.campaigns = next.campaigns || []
  state.exists = Boolean(next.exists)
  state.active_campaigns = Number(next.active_campaigns || 0)
  state.source_files = next.source_files || []
  state.source_files_total = Number(next.source_files_total ?? state.source_files.length)
  state.source_files_page = Number(next.source_files_page || 1)
  state.source_files_page_size = Number(next.source_files_page_size || 50)
  state.processed_files = next.processed_files || []
  state.processed_files_total = Number(next.processed_files_total ?? state.processed_files.length)
  state.processed_files_page = Number(next.processed_files_page || 1)
  state.processed_files_page_size = Number(next.processed_files_page_size || 50)
}

function statusLabel(row: any) {
  if (row.status === 'watching') return i18n.global.t('monitor.status_watching')
  if (row.status === 'processed') return i18n.global.t('monitor.status_processed')
  if (row.status === 'ignored' && row.match_prefix === false) return i18n.global.t('monitor.status_prefix_mismatch')
  if (row.status === 'ignored') return i18n.global.t('monitor.status_schedule_mismatch')
  if (row.status === 'pending' && row.status_label === '写入中') return i18n.global.t('monitor.status_writing')
  if (row.status === 'pending') return i18n.global.t('monitor.status_pending')
  return row.status_label || row.status
}
async function load() {
  loading.value = true
  try {
    const res = await getRealtimeMonitor(selectedCampaignId.value || undefined, {
      source_page: sourcePage.value,
      source_page_size: sourcePageSize.value,
      processed_page: processedPage.value,
      processed_page_size: processedPageSize.value,
    })
    applyState(res.data)
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('remaining.x866b795e'))
  } finally {
    loading.value = false
  }
}

async function handleCampaignChange() {
  sourcePage.value = 1
  processedPage.value = 1
  await load()
}

function changeSourcePage(page: number) {
  sourcePage.value = page
  void load()
}

function changeProcessedPage(page: number) {
  processedPage.value = page
  void load()
}

async function scanNow() {
  scanning.value = true
  try {
    const campaignId = selectedCampaignId.value || undefined
    const res = await scanRealtimeMonitor(campaignId, {
      source_page: sourcePage.value,
      source_page_size: sourcePageSize.value,
      processed_page: processedPage.value,
      processed_page_size: processedPageSize.value,
    })
    applyState(res.data.monitor)
    ElMessage.success(i18n.global.t('dynamic.monitorScanResult',{copied:res.data.sync?.copied || 0,processed:res.data.processing?.processed || 0}))
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.x5ec14542'))
  } finally {
    scanning.value = false
  }
}

function handleAutoRefresh(val: boolean) {
  if (val) {
    refreshTimer = setInterval(() => {
      void load()
    }, 5000)
  } else {
    stopAutoRefresh()
  }
}

function stopAutoRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

onMounted(load)
onUnmounted(stopAutoRefresh)
</script>

<style scoped>
.page-header {
  margin-bottom: 16px;
}
.page-header h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 600;
}
.page-header p {
  margin: 6px 0 0;
  color: var(--mc-text-secondary);
  font-size: 13px;
}
.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.watch-dir-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 100%;
}
.watch-dir-item {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px;
}
.watch-dir {
  display: inline-block;
  max-width: 100%;
  word-break: break-all;
  font-size: 12px;
}
.form-tip {
  width: 100%;
  margin-top: 4px;
  color: var(--mc-text-tertiary);
  font-size: 12px;
}
.pagination {
  justify-content: flex-end;
  padding: 12px 0;
}
</style>
