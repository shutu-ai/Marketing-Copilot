<template>
  <div class="sms-content-config">
    <div class="page-header">
      <h2>{{ $t('smsContent.title') }}</h2>
      <el-button v-if="canManage" type="primary" @click="$router.push('/sms-content/campaign/new')">
        <el-icon><Plus /></el-icon>
        {{ $t('smsContent.addCampaign') }}
      </el-button>
    </div>

    <el-table :data="tableData" stripe v-loading="loading" style="width: 100%" :span-method="spanMethod" border>
      <el-table-column prop="campaign_id" label="ID" width="70" />
      <el-table-column :label="$t('smsContent.marketingType')" width="100">
        <template #default="{ row }">
          {{ row.marketing_type === 'realtime' ? i18n.global.t('campaign.realtimeTitle') : i18n.global.t('campaign.batchTitle') }}
        </template>
      </el-table-column>
      <el-table-column prop="campaign_name" :label="$t('smsContent.campaignName')" min-width="160" />
      <el-table-column prop="language" :label="$t('common.language')" width="120" />
      <el-table-column :label="$t('smsContent.content')" min-width="300" show-overflow-tooltip>
        <template #default="{ row }">
          <span style="font-size:12px;color:#606266;white-space:pre-wrap;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden" :title="row.content">{{ row.content }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('common.action')" width="230" fixed="right">
        <template #default="{ row }">
          <el-button v-if="canManage" text type="primary" size="small" @click="$router.push('/sms-content/campaign/' + row.campaign_id)">
            {{ $t('common.edit') }}
          </el-button>
          <span :title="deleteDisabledReason(row)">
            <el-button v-if="canManage" text type="danger" size="small" :disabled="!canDeleteCampaign(row)" @click="handleDeleteCampaign(row)">
              {{ $t('common.delete') }}
            </el-button>
          </span>
          <el-button text type="primary" size="small" @click="openVersions(row)">{{ $t('update.historyVersion') }}</el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-pagination
      v-model:current-page="page"
      :page-size="pageSize"
      :total="total"
      layout="total, prev, pager, next"
      class="pagination"
      @current-change="changePage"
    />

    <el-dialog v-model="versionVisible" :title="$t('legacy.x569e45b8')" width="760px">
      <el-alert v-if="validation" :type="validation.valid ? 'success' : 'error'" :closable="false" show-icon>
        <template #title>{{ $t('legacy.x92069a3c') }}{{ validation.variables?.join(', ') || i18n.global.t('legacy.xd81bb206') }}</template>
        <div v-if="validation.warnings?.length">{{ validation.warnings.join('；') }}</div>
      </el-alert>
      <el-table :data="versions" style="margin-top:12px" max-height="420">
        <el-table-column prop="version_no" :label="$t('update.historyVersion')" width="80" />
        <el-table-column prop="language" :label="$t('common.language')" width="100" />
        <el-table-column prop="content" :label="$t('legacy.x2d711b09')" show-overflow-tooltip />
        <el-table-column prop="change_note" :label="$t('legacy.xa06b0305')" width="150" />
        <el-table-column prop="created_at" :label="$t('logViewer.time')" width="180" />
        <el-table-column v-if="canManage" :label="$t('common.action')" width="90"><template #default="{ row }"><el-button link type="primary" @click="restoreVersion(row)">{{ $t('legacy.xc7db6d4f') }}</el-button></template></el-table-column>
      </el-table>
      <el-pagination
        v-model:current-page="versionPage"
        :page-size="versionPageSize"
        :total="versionTotal"
        layout="total, prev, pager, next"
        class="pagination"
        @current-change="changeVersionPage"
      />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import i18n from '@/i18n'
import { ref, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import * as api from '@/api/smsContent'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()
const canManage = computed(() => ['admin', 'marketing'].includes(authStore.role))

const loading = ref(false)
const tableData = ref<any[]>([])
const page = ref(1)
const pageSize = ref(50)
const total = ref(0)
const versionVisible = ref(false)
const versions = ref<any[]>([])
const versionPage = ref(1)
const versionPageSize = ref(50)
const versionTotal = ref(0)
const activeTemplate = ref<any>(null)
const validation = ref<any>(null)

const spanMap = ref<Record<number, { rowspan: number; colspan: number }[]>>({})

function buildSpanMap(data: any[]) {
  const map: Record<number, { rowspan: number; colspan: number }[]> = {}
  let idx = 0
  while (idx < data.length) {
    const campaignId = data[idx].campaign_id
    let count = 1
    while (idx + count < data.length && data[idx + count].campaign_id === campaignId) {
      count++
    }
    for (let i = 0; i < count; i++) {
      const key = idx + i
      if (!map[key]) map[key] = []
      const spanVal = i === 0 ? count : 0
      map[key][0] = { rowspan: spanVal, colspan: 1 }
      map[key][1] = { rowspan: spanVal, colspan: 1 }
      map[key][2] = { rowspan: spanVal, colspan: 1 }
      map[key][5] = { rowspan: spanVal, colspan: 1 }
    }
    idx += count
  }
  spanMap.value = map
}

function spanMethod({ rowIndex, columnIndex }: { rowIndex: number; columnIndex: number }) {
  const row = spanMap.value[rowIndex]
  if (row && row[columnIndex]) {
    return row[columnIndex]
  }
  return { rowspan: 1, colspan: 1 }
}

async function fetchTable() {
  loading.value = true
  try {
    const res = await api.getTemplatesPage({ page: page.value, page_size: pageSize.value })
    tableData.value = res.data.items || []
    total.value = res.data.total ?? tableData.value.length
    buildSpanMap(tableData.value)
  } catch { /* ignore */ }
  loading.value = false
}

onMounted(fetchTable)

function changePage(nextPage: number) {
  page.value = nextPage
  void fetchTable()
}

async function handleDeleteCampaign(row: any) {
  if (!canDeleteCampaign(row)) return
  try {
    await ElMessageBox.confirm(i18n.global.t('legacy.x577c6ede'), i18n.global.t('legacy.x02d9819d'))
    await api.deleteCampaign(row.campaign_id)
    ElMessage.success(i18n.global.t('legacy.x0007d170'))
    await fetchTable()
  } catch { /* ignore */ }
}

function canDeleteCampaign(row: any) {
  return Number(row.activity_reference_count || 0) === 0 && Number(row.task_reference_count || 0) === 0
}

function deleteDisabledReason(row: any) {
  if (Number(row.activity_reference_count || 0) > 0) return i18n.global.t('legacy.x31f649e9')
  if (Number(row.task_reference_count || 0) > 0) return i18n.global.t('legacy.xaa6ae74c')
  return ''
}

async function openVersions(row: any) {
  activeTemplate.value = row
  versionPage.value = 1
  const [history, check] = await Promise.all([
    api.getCampaignVersions(row.campaign_id, { page: versionPage.value, page_size: versionPageSize.value }),
    api.validateTemplate(row.content || ''),
  ])
  versions.value = history.data.items || []
  versionTotal.value = history.data.total ?? versions.value.length
  validation.value = check.data
  versionVisible.value = true
}

async function loadVersions() {
  if (!activeTemplate.value) return
  const history = await api.getCampaignVersions(activeTemplate.value.campaign_id, {
    page: versionPage.value,
    page_size: versionPageSize.value,
  })
  versions.value = history.data.items || []
  versionTotal.value = history.data.total ?? versions.value.length
}

function changeVersionPage(nextPage: number) {
  if (!activeTemplate.value) return
  versionPage.value = nextPage
  void loadVersions()
}

async function restoreVersion(version: any) {
  await ElMessageBox.confirm(i18n.global.t('dynamic.restoreTemplateConfirm',{language:version.language,version:version.version_no}), i18n.global.t('legacy.x8bf6f6f4'))
  await api.restoreTemplateVersion(version.template_id, version.version_no)
  ElMessage.success(i18n.global.t('legacy.x696384a9'))
  await fetchTable()
  const row = tableData.value.find(item => item.campaign_id === activeTemplate.value.campaign_id)
  if (row) await openVersions(row)
}
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 16px;
}
.page-header h2 { margin: 0; font-size: 22px; font-weight: 600; }
.pagination { justify-content: flex-end; padding: 12px 0; }
</style>
