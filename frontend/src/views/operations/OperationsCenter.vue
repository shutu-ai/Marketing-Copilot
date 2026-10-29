<template>
  <div class="operations-center">
    <div class="page-header">
      <div><h2>{{ $t('menu.operations') }}</h2><p>{{ $t('legacy.xe2928306') }}</p></div>
      <el-button v-if="showGlobalRefresh" :loading="loading" @click="refreshActive"><el-icon><Refresh /></el-icon>{{ $t('common.refresh') }}</el-button>
    </div>

    <el-tabs v-model="activeTab" class="operations-tabs" @tab-change="changeTab">
      <el-tab-pane :label="$t('legacy.x640de727')" name="progress">
        <el-table v-loading="loading" :data="progressRows" stripe>
          <el-table-column prop="campaign_type" :label="$t('legacy.x226b0912')" width="90" />
          <el-table-column prop="name" :label="$t('legacy.x36c6f572')" min-width="180" />
          <el-table-column prop="status" :label="$t('common.status')" width="110"><template #default="{ row }"><StatusTag :status="row.status" /></template></el-table-column>
          <el-table-column :label="$t('legacy.x7baf4795')" min-width="220">
            <template #default="{ row }"><el-progress :percentage="row.tasks.percent" /><small>{{ row.tasks.finished }}/{{ row.tasks.total }}</small></template>
          </el-table-column>
          <el-table-column :label="$t('legacy.x2a0c4740')" width="120"><template #default="{ row }">{{ row.files.finished }}/{{ row.files.total }}</template></el-table-column>
          <el-table-column :label="$t('legacy.xe683517f')" width="120"><template #default="{ row }">{{ row.batches ? `${row.batches.finished}/${row.batches.total}` : '—' }}</template></el-table-column>
          <el-table-column :label="$t('common.action')" width="270" fixed="right">
            <template #default="{ row }">
              <template v-if="canOperate">
                <el-button v-if="row.status === 'not_started'" link type="primary" @click="campaignAction(row, 'start')">{{ $t('legacy.x8e54ddfe') }}</el-button>
                <el-button v-if="row.status === 'in_progress'" link @click="campaignAction(row, 'pause')">{{ $t('legacy.x8d63ef38') }}</el-button>
                <el-button v-if="row.status === 'paused'" link type="primary" @click="campaignAction(row, 'resume')">{{ $t('legacy.xc7db6d4f') }}</el-button>
                <el-button v-if="['in_progress','paused'].includes(row.status)" link type="success" @click="campaignAction(row, 'complete')">{{ $t('legacy.x769d88e4') }}</el-button>
                <el-button v-if="!['completed','cancelled'].includes(row.status)" link type="danger" @click="campaignAction(row, 'cancel')">{{ $t('common.cancel') }}</el-button>
              </template>
            </template>
          </el-table-column>
        </el-table>
        <el-pagination
          v-model:current-page="progressPage"
          :page-size="progressPageSize"
          :total="progressTotal"
          layout="total, prev, pager, next"
          class="pagination"
          @current-change="changeProgressPage"
        />
      </el-tab-pane>

      <el-tab-pane :label="$t('menu.smsTasks')" name="sms-tasks">
        <SmsTasks v-if="activeTab === 'sms-tasks'" embedded />
      </el-tab-pane>

      <el-tab-pane :label="$t('legacy.x01bdebc2')" name="failures">
        <div class="toolbar">
          <template v-if="canOperate">
            <el-button type="primary" :disabled="!selectedTasks.length" @click="bulkAction('retry')">{{ $t('legacy.xb2f87a97') }}</el-button>
            <el-button type="warning" :disabled="!selectedTasks.length" @click="bulkAction('archive')">{{ $t('legacy.x3bd00c55') }}</el-button>
          </template>
          <el-button @click="download('failures')">{{ $t('legacy.x0fd54dfd') }}</el-button>
        </div>
        <el-table v-loading="loading" :data="failedTasks" @selection-change="selectedTasks = $event">
          <el-table-column type="selection" width="48" />
          <el-table-column prop="id" label="ID" width="70" />
          <el-table-column prop="campaign_name" :label="$t('legacy.x36c6f572')" />
          <el-table-column prop="msisdn" :label="$t('legacy.x3eb7800d')" width="150" />
          <el-table-column prop="status" :label="$t('common.status')" width="100" />
          <el-table-column prop="failure_class" :label="$t('legacy.x4e43ded7')" width="150" />
          <el-table-column prop="error_msg" :label="$t('legacy.x7030ff64')" show-overflow-tooltip />
          <el-table-column :label="$t('legacy.x4404a8da')" width="90"><template #default="{ row }"><el-button link type="primary" @click="openTimeline(row.id)">{{ $t('legacy.x607e7a4f') }}</el-button></template></el-table-column>
        </el-table>
        <el-pagination
          v-model:current-page="failedPage"
          :page-size="failedPageSize"
          :total="failedTotal"
          layout="total, prev, pager, next"
          class="pagination"
          @current-change="changeFailedPage"
        />
        <h3 class="section-title">{{ $t('legacy.x909f823d') }}</h3>
        <el-table :data="archivedTasks" stripe>
          <el-table-column prop="task_id" :label="$t('legacy.x65615545')" width="90" />
          <el-table-column prop="campaign_name" :label="$t('legacy.x36c6f572')" />
          <el-table-column prop="msisdn" :label="$t('legacy.x3eb7800d')" width="150" />
          <el-table-column prop="final_status" :label="$t('legacy.x4927712e')" width="120" />
          <el-table-column prop="failure_class" :label="$t('legacy.x4e43ded7')" width="150" />
          <el-table-column prop="error_msg" :label="$t('legacy.x7030ff64')" show-overflow-tooltip />
          <el-table-column prop="archived_at" :label="$t('legacy.x8094cd50')" width="180" />
        </el-table>
        <el-pagination
          v-model:current-page="archivePage"
          :page-size="archivePageSize"
          :total="archiveTotal"
          layout="total, prev, pager, next"
          class="pagination"
          @current-change="changeArchivePage"
        />
      </el-tab-pane>

      <el-tab-pane :label="$t('legacy.xf0868962')" name="exceptions">
        <div class="toolbar"><el-button v-if="canOperate" type="primary" @click="runHealthCheck">{{ $t('legacy.x3b2222ed') }}</el-button></div>
        <el-row :gutter="12" class="health-grid">
          <el-col v-for="check in health" :key="check.code + check.message" :span="8">
            <el-alert :type="check.ok ? 'success' : check.severity" :title="check.code" :description="check.ok ? i18n.global.t('legacy.xfd6e80f1') : check.message" :closable="false" show-icon />
          </el-col>
        </el-row>
        <el-table v-loading="loading" :data="exceptions">
          <el-table-column prop="severity" :label="$t('legacy.xe78e4b2d')" width="90" />
          <el-table-column prop="category" :label="$t('legacy.x383a6d16')" width="120" />
          <el-table-column prop="code" :label="$t('legacy.x06e004ef')" width="150" />
          <el-table-column prop="message" :label="$t('update.historyDetail')" />
          <el-table-column prop="created_at" :label="$t('logViewer.time')" width="180" />
          <el-table-column :label="$t('common.action')" width="90"><template #default="{ row }"><el-button v-if="canOperate" link type="primary" @click="resolve(row.id)">{{ $t('legacy.x4b86211f') }}</el-button></template></el-table-column>
        </el-table>
        <el-pagination
          v-model:current-page="exceptionPage"
          :page-size="exceptionPageSize"
          :total="exceptionTotal"
          layout="total, prev, pager, next"
          class="pagination"
          @current-change="changeExceptionPage"
        />
      </el-tab-pane>

      <el-tab-pane :label="$t('menu.capacity')" name="capacity">
        <CapacityCenter v-if="activeTab === 'capacity'" embedded />
      </el-tab-pane>

      <el-tab-pane :label="$t('legacy.xd2eca24b')" name="dlr">
        <el-table :data="dlrRows" stripe>
          <el-table-column prop="code" :label="$t('legacy.x83f63f73')" width="160" />
          <el-table-column prop="label" :label="$t('legacy.xb91b84f1')" />
          <el-table-column prop="status" :label="$t('legacy.xbc7e74f7')" width="130" />
          <el-table-column prop="retryable" :label="$t('legacy.x0965d120')" width="100"><template #default="{ row }">{{ row.retryable ? i18n.global.t('common.yes') : i18n.global.t('common.no') }}</template></el-table-column>
        </el-table>
      </el-tab-pane>

      <el-tab-pane v-if="isAdmin" :label="$t('legacy.xa4231334')" name="audits">
        <div class="toolbar">
          <el-input v-model="auditAction" :placeholder="$t('legacy.x1d85b707')" clearable style="width:220px" @change="handleAuditFilter" />
          <el-button v-for="report in reports.filter(item => item.value !== 'audits' || isAdmin)" :key="report.value" @click="download(report.value)">{{ $t('legacy.x55405ea6') }}{{ report.label }}</el-button>
        </div>
        <el-table v-loading="loading" :data="audits">
          <el-table-column prop="created_at" :label="$t('logViewer.time')" width="180" />
          <el-table-column prop="username" :label="$t('legacy.x6b0bc643')" width="110" />
          <el-table-column prop="action" :label="$t('legacy.xc500cfab')" width="190" />
          <el-table-column prop="resource" :label="$t('legacy.xeee83a92')" width="150" />
          <el-table-column prop="resource_id" :label="$t('legacy.x044449b9')" width="100" />
          <el-table-column prop="detail" :label="$t('campaign.detail')" show-overflow-tooltip />
        </el-table>
        <el-pagination
          v-model:current-page="auditPage"
          :page-size="auditPageSize"
          :total="auditTotal"
          layout="total, prev, pager, next"
          class="pagination"
          @current-change="changeAuditPage"
        />
      </el-tab-pane>
    </el-tabs>

    <el-drawer v-model="timelineVisible" :title="$t('legacy.x5411b1f3')" size="520px">
      <el-descriptions v-if="timeline.task" :column="1" border><el-descriptions-item :label="$t('legacy.x0e46d8d6')">#{{ timeline.task.id }} · {{ timeline.task.msisdn }}</el-descriptions-item><el-descriptions-item :label="$t('update.currentStatus')">{{ timeline.task.status }}</el-descriptions-item></el-descriptions>
      <el-timeline class="timeline"><el-timeline-item v-for="event in timeline.events" :key="event.id" :timestamp="event.created_at" placement="top"><strong>{{ event.event_type }}</strong><div>{{ event.from_status }} → {{ event.to_status }}</div><small>{{ event.message }}</small></el-timeline-item></el-timeline>
    </el-drawer>
  </div>
</template>

<script setup lang="ts">
import i18n from '@/i18n'
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import StatusTag from '@/components/StatusTag.vue'
import SmsTasks from '@/views/sms-tasks/SmsTasks.vue'
import CapacityCenter from './CapacityCenter.vue'
import * as ops from '@/api/operations'
import { useAuthStore } from '@/stores/auth'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()
const canOperate = computed(() => ['admin', 'marketing'].includes(authStore.role))
const isAdmin = computed(() => authStore.role === 'admin')
const validTabs = ['progress', 'sms-tasks', 'failures', 'exceptions', 'capacity', 'dlr', 'audits']
const activeTab = ref(validTabs.includes(String(route.query.tab)) ? String(route.query.tab) : 'progress')
const showGlobalRefresh = computed(() => !['sms-tasks', 'capacity'].includes(activeTab.value))
const loading = ref(false)
const progressRows = ref<any[]>([])
const progressPage = ref(1)
const progressPageSize = ref(50)
const progressTotal = ref(0)
const failedTasks = ref<any[]>([])
const failedPage = ref(1)
const failedPageSize = ref(50)
const failedTotal = ref(0)
const archivedTasks = ref<any[]>([])
const archivePage = ref(1)
const archivePageSize = ref(50)
const archiveTotal = ref(0)
const selectedTasks = ref<any[]>([])
const exceptions = ref<any[]>([])
const exceptionPage = ref(1)
const exceptionPageSize = ref(50)
const exceptionTotal = ref(0)
const health = ref<any[]>([])
const dlrRows = ref<any[]>([])
const audits = ref<any[]>([])
const auditPage = ref(1)
const auditPageSize = ref(50)
const auditTotal = ref(0)
const auditAction = ref('')
const timelineVisible = ref(false)
const timeline = ref<any>({ task: null, events: [] })
const reports = [{ value: 'tasks', label: i18n.global.t('legacy.x0e46d8d6') }, { value: 'files', label: i18n.global.t('legacy.x2a0c4740') }, { value: 'failures', label: i18n.global.t('common.fail') }, { value: 'audits', label: i18n.global.t('legacy.x771dc11a') }]

async function withLoading(work: () => Promise<void>) { loading.value = true; try { await work() } finally { loading.value = false } }
const loadProgress = () => withLoading(async () => {
  const res = await ops.getOperationsProgress(progressPage.value, progressPageSize.value)
  progressRows.value = res.data.items || []
  progressTotal.value = res.data.total ?? progressRows.value.length
})
const loadFailures = () => withLoading(async () => {
  const [failed, archived] = await Promise.all([
    ops.getFailedTasks(failedPage.value, failedPageSize.value),
    ops.getFailureArchive(archivePage.value, archivePageSize.value),
  ])
  failedTasks.value = failed.data.items || []
  failedTotal.value = failed.data.total ?? failedTasks.value.length
  archivedTasks.value = archived.data.items || []
  archiveTotal.value = archived.data.total ?? archivedTasks.value.length
})
const loadExceptions = () => withLoading(async () => {
  const [events, checks] = await Promise.all([
    ops.getExceptions({ resolved: false, page: exceptionPage.value, page_size: exceptionPageSize.value }),
    ops.getConfigurationHealth(),
  ])
  exceptions.value = events.data.items || []
  exceptionTotal.value = events.data.total ?? exceptions.value.length
  health.value = checks.data
})
const loadDlr = () => withLoading(async () => { const data = (await ops.getDlrCodes()).data; dlrRows.value = Object.entries(data).map(([code, value]: any) => ({ code, ...value })) })
const loadAudits = () => withLoading(async () => {
  const res = await ops.getAuditLogs(auditPage.value, auditPageSize.value, auditAction.value)
  audits.value = res.data.items || []
  auditTotal.value = res.data.total ?? audits.value.length
})

function changeProgressPage(page: number) { progressPage.value = page; void loadProgress() }
function changeFailedPage(page: number) { failedPage.value = page; void loadFailures() }
function changeArchivePage(page: number) { archivePage.value = page; void loadFailures() }
function changeExceptionPage(page: number) { exceptionPage.value = page; void loadExceptions() }
function changeAuditPage(page: number) { auditPage.value = page; void loadAudits() }
function handleAuditFilter() { auditPage.value = 1; void loadAudits() }

function refreshActive() {
  const loaders: Record<string, () => Promise<void>> = { progress: loadProgress, failures: loadFailures, exceptions: loadExceptions, dlr: loadDlr, audits: loadAudits }
  return loaders[activeTab.value]?.()
}
function changeTab() {
  router.replace({ query: { ...route.query, tab: activeTab.value === 'progress' ? undefined : activeTab.value } })
  if (showGlobalRefresh.value) refreshActive()
}
async function campaignAction(row: any, action: string) { await ElMessageBox.confirm(i18n.global.t('dynamic.confirmCampaignAction',{action,name:row.name}), i18n.global.t('legacy.x6811882f')); await ops.runCampaignAction(row.campaign_type, row.campaign_id, action); ElMessage.success(i18n.global.t('legacy.xc0204968')); await loadProgress() }
async function bulkAction(action: 'retry' | 'archive') { await ops.bulkFailedTasks(action, selectedTasks.value.map(item => item.id)); ElMessage.success(i18n.global.t('dynamic.bulkProcessedTasks',{count:selectedTasks.value.length})); selectedTasks.value = []; await loadFailures() }
async function runHealthCheck() { await ops.getConfigurationHealth(true); await loadExceptions(); ElMessage.success(i18n.global.t('legacy.xe8c924d5')) }
async function resolve(id: number) { await ops.resolveException(id); await loadExceptions() }
async function openTimeline(id: number) { timeline.value = (await ops.getTaskTimeline(id)).data; timelineVisible.value = true }
async function download(report: string) {
  try {
    const response = await ops.exportReport(report)
    const url = URL.createObjectURL(response.data)
    const link = document.createElement('a')
    link.href = url
    link.download = `marketing-copilot-${report}.csv`
    link.click()
    URL.revokeObjectURL(url)
  } catch (error: any) {
    const detail = error.response?.data?.detail
    const message = typeof detail === 'object' ? detail.message : detail
    ElMessage.error(message || i18n.global.t('common.connectionFailed'))
  }
}
watch(() => route.query.tab, value => {
  const nextTab = validTabs.includes(String(value)) ? String(value) : 'progress'
  if (nextTab !== activeTab.value) {
    activeTab.value = nextTab
    if (!['sms-tasks', 'capacity'].includes(nextTab)) refreshActive()
  }
})
onMounted(refreshActive)
</script>

<style scoped>
.page-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px}.page-header h2{margin:0}.page-header p{margin:6px 0 0;color:var(--mc-text-secondary)}.toolbar{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px}.health-grid{margin-bottom:14px}.health-grid .el-col{margin-bottom:10px}.timeline{margin-top:24px}.section-title{margin:24px 0 12px;color:var(--mc-text);font-size:16px}small{color:var(--mc-text-tertiary)}.operations-center :deep(.el-tabs__header){overflow:hidden}.operations-center :deep(.el-tabs__nav-wrap){overflow-x:auto;scrollbar-width:none}.operations-center :deep(.el-tabs__nav-wrap::-webkit-scrollbar){display:none}.operations-center :deep(.el-tabs__content){padding-top:10px}.pagination{justify-content:flex-end;padding:12px 0}@media(max-width:767px){.operations-center :deep(.el-tabs__item){padding:0 14px;white-space:nowrap}.page-header{flex-direction:column;gap:10px}}
</style>
