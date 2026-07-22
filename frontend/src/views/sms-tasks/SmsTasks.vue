<template>
  <div>
    <div class="page-header">
      <h2>短信任务实时状态</h2>
      <div class="header-actions">
        <el-button @click="openExclusions">排除名单</el-button>
        <el-switch v-model="autoRefresh" active-text="自动刷新" />
      </div>
    </div>

    <el-row :gutter="12" class="stats">
      <el-col v-for="item in statItems" :key="item.key" :span="4">
        <el-card shadow="never">
          <div class="stat-label">{{ item.label }}</div>
          <div class="stat-value">{{ stats[item.key] || 0 }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" class="filters">
      <el-form inline>
        <el-form-item label="状态">
          <el-select v-model="status" clearable style="width: 150px" @change="reload">
            <el-option v-for="value in statuses" :key="value" :label="value" :value="value" />
          </el-select>
        </el-form-item>
        <el-form-item label="活动类型">
          <el-select v-model="campaignType" clearable style="width: 140px" @change="reload">
            <el-option label="批量" value="batch" />
            <el-option label="实时" value="realtime" />
          </el-select>
        </el-form-item>
        <el-button @click="fetchData"><el-icon><Refresh /></el-icon>刷新</el-button>
      </el-form>
    </el-card>

    <el-card shadow="never">
      <el-table :data="items" stripe v-loading="loading">
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="campaign_name" label="活动" min-width="150" />
        <el-table-column prop="campaign_type" label="类型" width="90" />
        <el-table-column prop="msisdn" label="手机号" width="150" />
        <el-table-column prop="language" label="语言" width="100" />
        <el-table-column prop="content" label="内容" min-width="220" show-overflow-tooltip />
        <el-table-column label="状态" width="110">
          <template #default="{ row }"><StatusTag :status="row.status" /></template>
        </el-table-column>
        <el-table-column prop="attempts" label="尝试" width="70" />
        <el-table-column prop="smsc_message_ids" label="SMSC Message ID" min-width="170" show-overflow-tooltip />
        <el-table-column prop="error_msg" label="错误" min-width="150" show-overflow-tooltip />
        <el-table-column prop="updated_at" label="更新时间" width="180" />
        <el-table-column label="操作" width="90" fixed="right">
          <template #default="{ row }">
            <el-button v-if="['failed', 'expired'].includes(row.status)" link type="primary" @click="retry(row.id)">重试</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-pagination
        v-model:current-page="page" :page-size="pageSize" :total="total"
        layout="total, prev, pager, next" class="pagination" @current-change="fetchData"
      />
    </el-card>

    <el-dialog v-model="exclusionVisible" title="排除名单" width="620px">
      <el-form inline>
        <el-form-item label="手机号"><el-input v-model="exclusionForm.msisdn" /></el-form-item>
        <el-form-item label="原因"><el-input v-model="exclusionForm.reason" /></el-form-item>
        <el-button type="primary" @click="createExclusion">添加</el-button>
      </el-form>
      <el-table :data="exclusions" max-height="360">
        <el-table-column prop="msisdn" label="手机号" />
        <el-table-column prop="reason" label="原因" />
        <el-table-column prop="created_at" label="创建时间" width="180" />
        <el-table-column label="操作" width="80">
          <template #default="{ row }"><el-button link type="danger" @click="removeExclusion(row.id)">删除</el-button></template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import StatusTag from '@/components/StatusTag.vue'
import { addExclusion, deleteExclusion, getExclusions, getSmsTasks, getSmsTaskStats, retrySmsTask } from '@/api/smsTasks'

const items = ref<any[]>([])
const stats = ref<Record<string, number>>({})
const loading = ref(false)
const total = ref(0)
const page = ref(1)
const pageSize = 50
const status = ref('')
const campaignType = ref('')
const autoRefresh = ref(true)
const exclusionVisible = ref(false)
const exclusions = ref<any[]>([])
const exclusionForm = ref({ msisdn: '', reason: '' })
const statuses = ['queued', 'sending', 'submitted', 'delivered', 'retrying', 'failed', 'expired']
const statItems = computed(() => [
  { key: 'total', label: '总任务' }, { key: 'queued', label: '排队中' },
  { key: 'submitted', label: '已提交' }, { key: 'delivered', label: '已送达' },
  { key: 'retrying', label: '重试中' }, { key: 'failed', label: '失败' },
])
let timer: number | undefined

async function fetchData(silent = false) {
  if (!silent) loading.value = true
  try {
    const [tasks, summary] = await Promise.all([
      getSmsTasks({ page: page.value, page_size: pageSize, status: status.value || undefined, campaign_type: campaignType.value || undefined }),
      getSmsTaskStats(),
    ])
    items.value = tasks.data.items
    total.value = tasks.data.total
    stats.value = summary.data
  } finally {
    loading.value = false
  }
}

function reload() { page.value = 1; fetchData() }
async function retry(id: number) {
  await retrySmsTask(id)
  ElMessage.success('任务已重新入队')
  fetchData()
}
async function openExclusions() {
  exclusionVisible.value = true
  exclusions.value = (await getExclusions()).data
}
async function createExclusion() {
  if (!exclusionForm.value.msisdn) return ElMessage.warning('请输入手机号')
  await addExclusion(exclusionForm.value.msisdn, exclusionForm.value.reason)
  exclusionForm.value = { msisdn: '', reason: '' }
  await openExclusions()
}
async function removeExclusion(id: number) {
  await deleteExclusion(id)
  await openExclusions()
}

onMounted(() => {
  fetchData()
  timer = window.setInterval(() => { if (autoRefresh.value) fetchData(true) }, 3000)
})
onBeforeUnmount(() => { if (timer) window.clearInterval(timer) })
</script>

<style scoped>
.page-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:16px; }
.page-header h2 { margin:0; font-size:22px; }
.header-actions { display:flex; align-items:center; gap:16px; }
.stats, .filters { margin-bottom:16px; }
.stat-label { color:#909399; font-size:13px; }
.stat-value { margin-top:8px; font-size:26px; font-weight:600; }
.pagination { justify-content:flex-end; margin-top:16px; }
</style>
