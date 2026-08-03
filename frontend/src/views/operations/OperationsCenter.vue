<template>
  <div class="operations-center">
    <div class="page-header">
      <div><h2>运营中心</h2><p>统一管理活动进度、短信任务、异常、容量和审计</p></div>
      <el-button v-if="showGlobalRefresh" :loading="loading" @click="refreshActive"><el-icon><Refresh /></el-icon>刷新</el-button>
    </div>

    <el-tabs v-model="activeTab" class="operations-tabs" @tab-change="changeTab">
      <el-tab-pane label="活动与进度" name="progress">
        <el-table v-loading="loading" :data="progressRows" stripe>
          <el-table-column prop="campaign_type" label="类型" width="90" />
          <el-table-column prop="name" label="活动" min-width="180" />
          <el-table-column prop="status" label="状态" width="110"><template #default="{ row }"><StatusTag :status="row.status" /></template></el-table-column>
          <el-table-column label="任务进度" min-width="220">
            <template #default="{ row }"><el-progress :percentage="row.tasks.percent" /><small>{{ row.tasks.finished }}/{{ row.tasks.total }}</small></template>
          </el-table-column>
          <el-table-column label="文件" width="120"><template #default="{ row }">{{ row.files.finished }}/{{ row.files.total }}</template></el-table-column>
          <el-table-column label="批次" width="120"><template #default="{ row }">{{ row.batches ? `${row.batches.finished}/${row.batches.total}` : '—' }}</template></el-table-column>
          <el-table-column label="操作" width="270" fixed="right">
            <template #default="{ row }">
              <el-button v-if="row.status === 'not_started'" link type="primary" @click="campaignAction(row, 'start')">启动</el-button>
              <el-button v-if="row.status === 'in_progress'" link @click="campaignAction(row, 'pause')">暂停</el-button>
              <el-button v-if="row.status === 'paused'" link type="primary" @click="campaignAction(row, 'resume')">恢复</el-button>
              <el-button v-if="['in_progress','paused'].includes(row.status)" link type="success" @click="campaignAction(row, 'complete')">完成</el-button>
              <el-button v-if="!['completed','cancelled'].includes(row.status)" link type="danger" @click="campaignAction(row, 'cancel')">取消</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>

      <el-tab-pane label="短信任务" name="sms-tasks">
        <SmsTasks v-if="activeTab === 'sms-tasks'" embedded />
      </el-tab-pane>

      <el-tab-pane label="失败处理" name="failures">
        <div class="toolbar">
          <el-button type="primary" :disabled="!selectedTasks.length" @click="bulkAction('retry')">批量重试</el-button>
          <el-button type="warning" :disabled="!selectedTasks.length" @click="bulkAction('archive')">批量归档</el-button>
          <el-button @click="download('failures')">导出失败归档</el-button>
        </div>
        <el-table v-loading="loading" :data="failedTasks" @selection-change="selectedTasks = $event">
          <el-table-column type="selection" width="48" />
          <el-table-column prop="id" label="ID" width="70" />
          <el-table-column prop="campaign_name" label="活动" />
          <el-table-column prop="msisdn" label="号码" width="150" />
          <el-table-column prop="status" label="状态" width="100" />
          <el-table-column prop="failure_class" label="失败类型" width="150" />
          <el-table-column prop="error_msg" label="错误" show-overflow-tooltip />
          <el-table-column label="时间线" width="90"><template #default="{ row }"><el-button link type="primary" @click="openTimeline(row.id)">查看</el-button></template></el-table-column>
        </el-table>
        <h3 class="section-title">失败归档</h3>
        <el-table :data="archivedTasks" stripe>
          <el-table-column prop="task_id" label="任务 ID" width="90" />
          <el-table-column prop="campaign_name" label="活动" />
          <el-table-column prop="msisdn" label="号码" width="150" />
          <el-table-column prop="final_status" label="归档前状态" width="120" />
          <el-table-column prop="failure_class" label="失败类型" width="150" />
          <el-table-column prop="error_msg" label="错误" show-overflow-tooltip />
          <el-table-column prop="archived_at" label="归档时间" width="180" />
        </el-table>
      </el-tab-pane>

      <el-tab-pane label="异常中心" name="exceptions">
        <div class="toolbar"><el-button type="primary" @click="runHealthCheck">检查配置并记录异常</el-button></div>
        <el-row :gutter="12" class="health-grid">
          <el-col v-for="check in health" :key="check.code + check.message" :span="8">
            <el-alert :type="check.ok ? 'success' : check.severity" :title="check.code" :description="check.ok ? '正常' : check.message" :closable="false" show-icon />
          </el-col>
        </el-row>
        <el-table v-loading="loading" :data="exceptions">
          <el-table-column prop="severity" label="级别" width="90" />
          <el-table-column prop="category" label="类别" width="120" />
          <el-table-column prop="code" label="代码" width="150" />
          <el-table-column prop="message" label="说明" />
          <el-table-column prop="created_at" label="时间" width="180" />
          <el-table-column label="操作" width="90"><template #default="{ row }"><el-button link type="primary" @click="resolve(row.id)">解决</el-button></template></el-table-column>
        </el-table>
      </el-tab-pane>

      <el-tab-pane label="容量与高可用" name="capacity">
        <CapacityCenter v-if="activeTab === 'capacity'" embedded />
      </el-tab-pane>

      <el-tab-pane label="DLR 映射" name="dlr">
        <el-table :data="dlrRows" stripe>
          <el-table-column prop="code" label="DLR 状态码" width="160" />
          <el-table-column prop="label" label="含义" />
          <el-table-column prop="status" label="任务状态" width="130" />
          <el-table-column prop="retryable" label="可重试" width="100"><template #default="{ row }">{{ row.retryable ? '是' : '否' }}</template></el-table-column>
        </el-table>
      </el-tab-pane>

      <el-tab-pane label="审计与报表" name="audits">
        <div class="toolbar">
          <el-input v-model="auditAction" placeholder="按动作筛选" clearable style="width:220px" @change="loadAudits" />
          <el-button v-for="report in reports" :key="report.value" @click="download(report.value)">导出{{ report.label }}</el-button>
        </div>
        <el-table v-loading="loading" :data="audits">
          <el-table-column prop="created_at" label="时间" width="180" />
          <el-table-column prop="username" label="操作者" width="110" />
          <el-table-column prop="action" label="动作" width="190" />
          <el-table-column prop="resource" label="资源" width="150" />
          <el-table-column prop="resource_id" label="资源 ID" width="100" />
          <el-table-column prop="detail" label="详情" show-overflow-tooltip />
        </el-table>
      </el-tab-pane>
    </el-tabs>

    <el-drawer v-model="timelineVisible" title="任务时间线" size="520px">
      <el-descriptions v-if="timeline.task" :column="1" border><el-descriptions-item label="任务">#{{ timeline.task.id }} · {{ timeline.task.msisdn }}</el-descriptions-item><el-descriptions-item label="当前状态">{{ timeline.task.status }}</el-descriptions-item></el-descriptions>
      <el-timeline class="timeline"><el-timeline-item v-for="event in timeline.events" :key="event.id" :timestamp="event.created_at" placement="top"><strong>{{ event.event_type }}</strong><div>{{ event.from_status }} → {{ event.to_status }}</div><small>{{ event.message }}</small></el-timeline-item></el-timeline>
    </el-drawer>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import StatusTag from '@/components/StatusTag.vue'
import SmsTasks from '@/views/sms-tasks/SmsTasks.vue'
import CapacityCenter from './CapacityCenter.vue'
import { getSmsTasks } from '@/api/smsTasks'
import * as ops from '@/api/operations'

const route = useRoute()
const router = useRouter()
const validTabs = ['progress', 'sms-tasks', 'failures', 'exceptions', 'capacity', 'dlr', 'audits']
const activeTab = ref(validTabs.includes(String(route.query.tab)) ? String(route.query.tab) : 'progress')
const showGlobalRefresh = computed(() => !['sms-tasks', 'capacity'].includes(activeTab.value))
const loading = ref(false)
const progressRows = ref<any[]>([])
const failedTasks = ref<any[]>([])
const archivedTasks = ref<any[]>([])
const selectedTasks = ref<any[]>([])
const exceptions = ref<any[]>([])
const health = ref<any[]>([])
const dlrRows = ref<any[]>([])
const audits = ref<any[]>([])
const auditAction = ref('')
const timelineVisible = ref(false)
const timeline = ref<any>({ task: null, events: [] })
const reports = [{ value: 'tasks', label: '任务' }, { value: 'files', label: '文件' }, { value: 'failures', label: '失败' }, { value: 'audits', label: '审计' }]

async function withLoading(work: () => Promise<void>) { loading.value = true; try { await work() } finally { loading.value = false } }
const loadProgress = () => withLoading(async () => { progressRows.value = (await ops.getOperationsProgress()).data })
const loadFailures = () => withLoading(async () => {
  const [failed, expired, archived] = await Promise.all([
    getSmsTasks({ page: 1, page_size: 200, status: 'failed' }),
    getSmsTasks({ page: 1, page_size: 200, status: 'expired' }),
    ops.getFailureArchive(),
  ])
  failedTasks.value = [...failed.data.items, ...expired.data.items]
  archivedTasks.value = archived.data.items
})
const loadExceptions = () => withLoading(async () => { const [events, checks] = await Promise.all([ops.getExceptions({ resolved: false }), ops.getConfigurationHealth()]); exceptions.value = events.data.items; health.value = checks.data })
const loadDlr = () => withLoading(async () => { const data = (await ops.getDlrCodes()).data; dlrRows.value = Object.entries(data).map(([code, value]: any) => ({ code, ...value })) })
const loadAudits = () => withLoading(async () => { audits.value = (await ops.getAuditLogs(1, auditAction.value)).data.items })

function refreshActive() {
  const loaders: Record<string, () => Promise<void>> = { progress: loadProgress, failures: loadFailures, exceptions: loadExceptions, dlr: loadDlr, audits: loadAudits }
  return loaders[activeTab.value]?.()
}
function changeTab() {
  router.replace({ query: { ...route.query, tab: activeTab.value === 'progress' ? undefined : activeTab.value } })
  if (showGlobalRefresh.value) refreshActive()
}
async function campaignAction(row: any, action: string) { await ElMessageBox.confirm(`确认${action}活动“${row.name}”？`, '活动操作'); await ops.runCampaignAction(row.campaign_type, row.campaign_id, action); ElMessage.success('活动状态已更新'); await loadProgress() }
async function bulkAction(action: 'retry' | 'archive') { await ops.bulkFailedTasks(action, selectedTasks.value.map(item => item.id)); ElMessage.success(`已处理 ${selectedTasks.value.length} 个任务`); selectedTasks.value = []; await loadFailures() }
async function runHealthCheck() { await ops.getConfigurationHealth(true); await loadExceptions(); ElMessage.success('配置检查完成') }
async function resolve(id: number) { await ops.resolveException(id); await loadExceptions() }
async function openTimeline(id: number) { timeline.value = (await ops.getTaskTimeline(id)).data; timelineVisible.value = true }
async function download(report: string) { const response = await ops.exportReport(report); const url = URL.createObjectURL(response.data); const link = document.createElement('a'); link.href = url; link.download = `marketing-copilot-${report}.csv`; link.click(); URL.revokeObjectURL(url) }
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
.page-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px}.page-header h2{margin:0}.page-header p{margin:6px 0 0;color:var(--mc-text-secondary)}.toolbar{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:16px}.health-grid{margin-bottom:14px}.health-grid .el-col{margin-bottom:10px}.timeline{margin-top:24px}.section-title{margin:24px 0 12px;color:var(--mc-text);font-size:16px}small{color:var(--mc-text-tertiary)}.operations-center :deep(.el-tabs__header){overflow:hidden}.operations-center :deep(.el-tabs__nav-wrap){overflow-x:auto;scrollbar-width:none}.operations-center :deep(.el-tabs__nav-wrap::-webkit-scrollbar){display:none}.operations-center :deep(.el-tabs__content){padding-top:10px}@media(max-width:767px){.operations-center :deep(.el-tabs__item){padding:0 14px;white-space:nowrap}.page-header{flex-direction:column;gap:10px}}
</style>
