<template>
  <div class="file-monitor">
    <div class="page-header">
      <h2>实时营销监听状态</h2>
      <p>查看任务级监听目录、文件名前缀、源目录当前文件和已处理记录。前端关闭不影响后台调度执行。</p>
    </div>

    <el-card shadow="never" style="margin-bottom: 20px">
      <template #header>
        <div class="card-header">
          <span>任务级监听目录</span>
          <el-tag :type="state.exists ? 'success' : 'warning'" effect="light">
            {{ state.exists ? '目录可用' : '目录未配置或不可用' }}
          </el-tag>
        </div>
      </template>

      <el-form label-width="160px" size="small">
        <el-form-item label="实时营销活动">
          <el-select v-model="selectedCampaignId" filterable style="width: 360px" @change="handleCampaignChange">
            <el-option :value="0" label="全部实时活动" />
            <el-option
              v-for="campaign in state.campaigns"
              :key="campaign.id"
              :value="campaign.id"
              :label="`#${campaign.id} ${campaign.name}`"
            />
          </el-select>
        </el-form-item>

        <el-form-item label="监听目录">
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
          <div class="form-tip">监听目录和文件名前缀在实时营销活动的新建/编辑界面维护；数据监控只负责查看和手动触发扫描。</div>
        </el-form-item>

        <el-form-item label="运行中实时活动">
          <el-tag type="info">{{ state.active_campaigns }}</el-tag>
        </el-form-item>

        <el-form-item>
          <el-button :loading="scanning" @click="scanNow">
            <el-icon><Refresh /></el-icon>
            {{ selectedCampaignId ? '扫描当前活动' : '扫描全部活动' }}
          </el-button>
          <el-switch
            v-model="autoRefresh"
            active-text="自动刷新"
            style="margin-left: 12px"
            @change="handleAutoRefresh"
          />
        </el-form-item>
      </el-form>
    </el-card>

    <el-card shadow="never" style="margin-bottom: 20px">
      <template #header>
        <span>监听目录当前文件 <el-tag type="info" size="small">{{ state.source_files_total }}</el-tag></span>
      </template>
      <el-table :data="state.source_files" stripe v-loading="loading" style="width: 100%">
        <el-table-column prop="filename" label="文件名" min-width="260" />
        <el-table-column prop="directory_path" label="监听目录" min-width="260" />
        <el-table-column v-if="selectedCampaignId" label="匹配前缀" width="100">
          <template #default="{ row }">
            <el-tag :type="row.match_prefix ? 'success' : 'info'" size="small">
              {{ row.match_prefix ? '是' : '否' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column v-if="selectedCampaignId" label="匹配发送规则" width="120">
          <template #default="{ row }">
            <el-tag :type="row.match_schedule ? 'success' : 'info'" size="small">
              {{ row.match_schedule ? '是' : '否' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="处理判断" width="120">
          <template #default="{ row }">
            <el-tag :type="row.status === 'processed' ? 'success' : row.status === 'ignored' ? 'info' : 'warning'" size="small">
              {{ row.status_label || row.status }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="skip_reason" label="原因" min-width="220" show-overflow-tooltip />
        <el-table-column label="文件大小" width="120">
          <template #default="{ row }">{{ formatFileSize(row.file_size || 0) }}</template>
        </el-table-column>
        <el-table-column prop="row_count" label="行数" width="90" />
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
        <span>已处理实时营销文件记录 <el-tag type="info" size="small">{{ state.processed_files_total }}</el-tag></span>
      </template>
      <el-table :data="state.processed_files" stripe v-loading="loading" style="width: 100%">
        <el-table-column prop="filename" label="文件名" min-width="220" />
        <el-table-column prop="campaign_id" label="活动ID" width="90" />
        <el-table-column prop="file_path" label="任务文件路径" min-width="260" />
        <el-table-column label="文件大小" width="120">
          <template #default="{ row }">{{ formatFileSize(row.file_size || 0) }}</template>
        </el-table-column>
        <el-table-column prop="total_rows" label="行数" width="90" />
        <el-table-column prop="created_tasks" label="生成短信任务数" width="130" />
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <StatusTag :status="row.status" />
          </template>
        </el-table-column>
        <el-table-column prop="detected_at" label="创建时间" width="180" />
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
    ElMessage.error(e.response?.data?.detail || '加载失败')
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
    ElMessage.success(`扫描完成，复制 ${res.data.sync?.copied || 0} 个文件，处理 ${res.data.processing?.processed || 0} 个文件`)
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '扫描失败')
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
