<template>
  <div class="batch-campaign">
    <PageHeader :title="$t('campaign.batchTitle')" :description="$t('campaign.batchDesc')">
      <template #actions>
        <el-button type="primary" @click="showDialog(null)">
        <el-icon><Plus /></el-icon>{{ $t('campaign.addCampaign') }}
        </el-button>
      </template>
    </PageHeader>

    <div class="table-shell">
    <el-table :data="campaigns" stripe v-loading="loading" style="width: 100%">
      <el-table-column prop="id" label="ID" width="70" />
      <el-table-column prop="name" :label="$t('campaign.name')" min-width="140" />
      <el-table-column prop="start_time" :label="$t('campaign.startTime')" width="170" />
      <el-table-column prop="end_time" :label="$t('campaign.endTime')" width="170" />
      <el-table-column :label="$t('campaign.sendStartTime')" width="90">
        <template #default="{ row }">
          {{ row.send_start_time || '-' }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.sendEndTime')" width="90">
        <template #default="{ row }">
          {{ row.send_end_time || '-' }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.sendDays')" min-width="240">
        <template #default="{ row }">
          <template v-if="row.send_days?.length">
            <el-tag v-for="d in row.send_days" :key="d" size="small" style="margin: 1px">
              {{ $t('campaign.day_' + d) }}
            </el-tag>
          </template>
          <span v-else style="color: #999">-</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.status')" width="110">
        <template #default="{ row }">
          <StatusTag :status="row.status">{{ $t('campaign.status_' + row.status) }}</StatusTag>
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.targetList')" min-width="180">
        <template #default="{ row }">
          <template v-if="row.customer_list_filename">
            <span>{{ row.customer_list_filename }}</span>
            <el-tag size="small" type="info" style="margin-left: 4px">{{ row.row_count }} rows</el-tag>
          </template>
          <span v-else style="color: #999">-</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.dirRoot')" min-width="200" class-name="desktop-only" label-class-name="desktop-only">
        <template #default="{ row }">
          <code style="font-size: 11px">{{ row.campaign_dir || '-' }}</code>
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.dirWhitelist')" min-width="140">
        <template #default="{ row }">
          <code style="font-size: 11px">{{ row.campaign_dir ? row.campaign_dir + '\\whitelist\\' : '-' }}</code>
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.dirOutput')" min-width="140">
        <template #default="{ row }">
          <code style="font-size: 11px">{{ row.campaign_dir ? row.campaign_dir + '\\output\\' : '-' }}</code>
        </template>
      </el-table-column>
  <el-table-column :label="$t('campaign.batchSize')" width="90">
    <template #default="{ row }">
      {{ row.batch_size || 500 }}
    </template>
  </el-table-column>
  <el-table-column :label="$t('campaign.associateSmpp')" width="170">
    <template #default="{ row }">
      <span v-if="row.smpp_config_id" style="font-size:12px">{{ smppName(row.smpp_config_id) }}</span>
      <span v-else style="color:#ccc">-</span>
    </template>
  </el-table-column>
  <el-table-column :label="$t('campaign.readiness')" width="130" fixed="right">
    <template #default="{ row }">
      <el-popover placement="left" :width="280" trigger="hover">
        <template #reference>
          <div class="readiness">
            <el-tag :type="isReady(row) ? 'success' : 'warning'" effect="light">
              {{ isReady(row) ? $t('campaign.ready') : $t('campaign.notReady') }}
            </el-tag>
            <span class="readiness__summary">{{ readinessItems(row).filter(item => item.ok).length }}/{{ readinessItems(row).length }}</span>
          </div>
        </template>
        <strong>{{ $t('campaign.checklist') }}</strong>
        <div v-for="item in readinessItems(row)" :key="item.label" class="readiness-item">
          <el-icon :class="{ ok: item.ok }"><CircleCheck v-if="item.ok" /><Warning v-else /></el-icon>
          <span>{{ item.label }}</span>
        </div>
      </el-popover>
    </template>
  </el-table-column>
  <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
      <el-table-column :label="$t('common.action')" width="210" fixed="right">
        <template #default="{ row }">
          <div class="campaign-actions">
          <el-button text type="primary" size="small" @click="$router.push('/campaign/batch/' + row.id)">
            {{ $t('campaign.detail') }}
          </el-button>
          <template v-if="row.status === 'not_started' || row.status === 'ended' || row.status === 'completed'">
            <span :title="startHint(row)">
              <el-button text type="warning" size="small" :disabled="!isReady(row)" @click="handleStart(row)">
                {{ row.status === 'ended' ? $t('campaign.restart') : $t('campaign.start') }}
              </el-button>
            </span>
          </template>
          <template v-else-if="row.status === 'in_progress'">
            <el-button text type="danger" size="small" @click="handleEnd(row)">
              {{ $t('campaign.end') }}
            </el-button>
          </template>
          <el-dropdown trigger="click" @command="handleMore($event, row)"><el-button text size="small">{{ $t('common.more') }}<el-icon><ArrowDown /></el-icon></el-button><template #dropdown><el-dropdown-menu><el-dropdown-item command="edit">{{ $t('common.edit') }}</el-dropdown-item><el-dropdown-item command="upload">{{ row.status === 'in_progress' ? '更新目标清单' : $t('campaign.uploadTargetList') }}</el-dropdown-item><el-dropdown-item command="sms">{{ $t('smsContent.configSms') }}</el-dropdown-item><el-dropdown-item command="copy">{{ $t('campaign.copy') }}</el-dropdown-item><el-dropdown-item v-if="row.status!=='in_progress'" command="delete" divided>{{ $t('common.delete') }}</el-dropdown-item></el-dropdown-menu></template></el-dropdown>
          </div>
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
    </div>

    <el-dialog
      :model-value="dialogVisible"
      :title="editForm.id ? $t('common.edit') : $t('campaign.addCampaign')"
      width="520px"
      align-center
      @close="dialogVisible = false"
    >
      <el-form :model="editForm" label-width="120px" size="small">
        <template v-if="!inProgressEdit">
        <el-form-item :label="$t('campaign.name')" required>
          <el-input v-model="editForm.name" />
        </el-form-item>
        <div v-if="editForm.status !== 'ended'" class="advanced-toggle">
          <el-switch v-model="customDirectory" :active-text="$t('campaign.customDirectory')" @change="handleCustomDirectoryChange" />
          <el-tag size="small" type="info" effect="plain">{{ $t('campaign.optional') }}</el-tag>
        </div>
        <el-form-item v-if="customDirectory" :label="$t('campaign.campaignDir')">
          <el-input v-model="editForm.campaign_dir" :placeholder="$t('campaign.campaignDirPlaceholder')">
            <template #append><el-button @click="directoryPickerVisible = true">{{ $t('campaign.browseDirectory') }}</el-button></template>
          </el-input>
          <div style="font-size:12px;color:#909399;margin-top:4px">{{ $t('campaign.campaignDirTip') }}</div>
        </el-form-item>
        <el-form-item :label="$t('campaign.startTime')">
          <el-date-picker
            v-model="editForm.start_time"
            type="datetime"
            :placeholder="$t('campaign.selectTime')"
            value-format="YYYY-MM-DD HH:mm:ss"
            :disabled="editForm.status === 'ended'"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item :label="$t('campaign.endTime')">
          <el-date-picker
            v-model="editForm.end_time"
            type="datetime"
            :placeholder="$t('campaign.selectTime')"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item :label="$t('campaign.sendStartTime')">
          <el-time-picker
            v-model="editForm.send_start_time"
            :placeholder="$t('campaign.selectTime')"
            value-format="HH:mm"
            format="HH:mm"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item :label="$t('campaign.sendEndTime')">
          <el-time-picker
            v-model="editForm.send_end_time"
            :placeholder="$t('campaign.selectTime')"
            value-format="HH:mm"
            format="HH:mm"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item :label="$t('campaign.batchSize')">
          <el-input-number v-model="editForm.batch_size" :min="1" :max="10000" :disabled="editForm.status === 'ended'" style="width: 100%" />
        </el-form-item>
        <el-form-item :label="$t('campaign.sendDays')">
          <el-checkbox-group v-model="editForm.send_days">
            <el-checkbox v-for="d in weekDays" :key="d.value" :label="d.value" size="small">
              {{ $t('campaign.day_' + d.value) }}
            </el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item :label="$t('campaign.phonePrefix')">
          <el-input v-model="editForm.phone_prefix" placeholder="251" />
          <div class="form-tip">{{ $t('campaign.phonePrefixTip') }}</div>
        </el-form-item>
        </template>
        <el-alert
          v-if="inProgressEdit"
          :title="$t('campaign.inProgressSmppOnly')"
          type="info"
          show-icon
          :closable="false"
          style="margin-bottom: 12px"
        />
        <el-form-item :label="$t('campaign.associateSmpp')" required>
          <el-select v-model="editForm.smpp_config_id" clearable filterable :placeholder="$t('campaign.associateSmppPlaceholder')" style="width:100%">
            <el-option v-for="cfg in enabledSmppOptions" :key="cfg.id" :value="cfg.id" :label="`${cfg.name} (${cfg.host}:${cfg.port})`" />
          </el-select>
          <div class="form-tip">{{ $t('campaign.associateSmppTip') }}</div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">{{ $t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="saving" @click="handleSave">{{ $t('common.save') }}</el-button>
      </template>
    </el-dialog>

    <DirectoryPicker v-model="directoryPickerVisible" :initial-path="editForm.campaign_dir" @select="selectDirectory" />
    <el-dialog
      :model-value="uploadVisible"
      :title="$t('campaign.uploadTargetList')"
      width="480px"
      @close="uploadVisible = false"
    >
      <div style="margin-bottom: 12px; text-align: right">
        <el-button size="small" @click="downloadProspectTemplate">
          <el-icon><Download /></el-icon>
          {{ $t('campaign.downloadTemplate') }}
        </el-button>
      </div>
      <el-upload
        drag
        :auto-upload="false"
        :show-file-list="true"
        :on-change="handleFileChange"
        accept=".csv"
      >
        <el-icon class="el-icon--upload"><UploadFilled /></el-icon>
        <div class="el-upload__text">{{ $t('campaign.dropHint') }}</div>
      </el-upload>
      <template #footer>
        <el-button @click="uploadVisible = false">{{ $t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="uploading" :disabled="!selectedFile" @click="handleUploadSubmit">
          {{ $t('common.confirm') }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import StatusTag from '@/components/StatusTag.vue'
import PageHeader from '@/components/PageHeader.vue'
import DirectoryPicker from '@/components/DirectoryPicker.vue'
import * as api from '@/api/campaign'
import * as smsApi from '@/api/smsContent'
import * as smppApi from '@/api/smpp'
import { useI18n } from 'vue-i18n'

const { t } = useI18n()
const router = useRouter()

const weekDays = [
  { value: '1', labelKey: 'day_mon' },
  { value: '2', labelKey: 'day_tue' },
  { value: '3', labelKey: 'day_wed' },
  { value: '4', labelKey: 'day_thu' },
  { value: '5', labelKey: 'day_fri' },
  { value: '6', labelKey: 'day_sat' },
  { value: '0', labelKey: 'day_sun' },
]

interface Campaign {
  id: number
  name: string
  start_time: string
  end_time: string
  send_start_time: string
  send_end_time: string
  send_days: string[]
  status: string
  batch_size: number
  customer_list_filename: string
  customer_list_path: string
  row_count: number
  campaign_dir: string
  use_custom_directory: boolean
  sms_template_id: number | null
  smpp_config_id: number | null
  phone_prefix: string
  created_at: string
}

const campaigns = ref<Campaign[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(50)
const loading = ref(false)
const dialogVisible = ref(false)
const saving = ref(false)
const editForm = ref<{
  id: number; name: string; start_time: string; end_time: string;
  send_start_time: string; send_end_time: string; send_days: string[];
  batch_size: number; campaign_dir: string; status: string;
  smpp_config_id: number | null; phone_prefix: string
}>({
  id: 0, name: '', start_time: '', end_time: '',
  send_start_time: '', send_end_time: '', send_days: [],
  batch_size: 500, campaign_dir: '', status: 'not_started',
  smpp_config_id: null, phone_prefix: '',
})
const uploadVisible = ref(false)
const uploading = ref(false)
const uploadCampaignId = ref(0)
const selectedFile = ref<File | null>(null)
const customDirectory = ref(false)
const inProgressEdit = ref(false)
const directoryPickerVisible = ref(false)
const smppOptions = ref<any[]>([])
const enabledSmppOptions = computed(() => smppOptions.value.filter((item: any) => item.enabled === 1))

function smppName(configId: number | null) {
  const cfg = smppOptions.value.find((item: any) => item.id === configId)
  return cfg ? `${cfg.name} (${cfg.host}:${cfg.port})` : `#${configId}`
}

async function fetchSmppConfigs() {
  try {
    const res = await smppApi.getSmppConfigs()
    smppOptions.value = res.data
  } catch {
    // ignore
  }
}

async function fetchList() {
  loading.value = true
  try {
    const res = await api.getCampaigns({ page: page.value, page_size: pageSize.value })
    campaigns.value = res.data.items || []
    total.value = res.data.total ?? campaigns.value.length
  } finally {
    loading.value = false
  }
}

function changePage(nextPage: number) {
  page.value = nextPage
  void fetchList()
}

function showDialog(row: Campaign | null) {
  if (row) {
    inProgressEdit.value = row.status !== 'not_started' && row.status !== 'ended'
    customDirectory.value = Boolean(row.use_custom_directory)
    editForm.value = {
      id: row.id, name: row.name, start_time: row.start_time, end_time: row.end_time,
      send_start_time: row.send_start_time || '', send_end_time: row.send_end_time || '',
      send_days: row.send_days || [], batch_size: row.batch_size || 500,
      campaign_dir: row.campaign_dir || '', status: row.status,
      smpp_config_id: row.smpp_config_id ?? null, phone_prefix: row.phone_prefix || '',
    }
  } else {
    inProgressEdit.value = false
    customDirectory.value = false
    editForm.value = {
      id: 0, name: '', start_time: '', end_time: '',
      send_start_time: '', send_end_time: '', send_days: [], batch_size: 500, campaign_dir: '', status: 'not_started',
      smpp_config_id: null, phone_prefix: '',
    }
  }
  dialogVisible.value = true
}

function readinessItems(row: Campaign) {
  const scheduleReady = Boolean(row.start_time && row.end_time && row.send_start_time && row.send_end_time)
  return [
    { ok: row.row_count > 0, label: t(row.row_count > 0 ? 'campaign.listReady' : 'campaign.listMissing') },
    { ok: Boolean(row.sms_template_id), label: t(row.sms_template_id ? 'campaign.templateReady' : 'campaign.templateMissing') },
    { ok: Boolean(row.smpp_config_id), label: t(row.smpp_config_id ? 'campaign.smppReady' : 'campaign.smppMissing') },
    { ok: scheduleReady, label: t(scheduleReady ? 'campaign.scheduleReady' : 'campaign.scheduleMissing') },
  ]
}

function handleCustomDirectoryChange(enabled: boolean) {
  if (!enabled) editForm.value.campaign_dir = ''
}

function selectDirectory(path: string) {
  editForm.value.campaign_dir = path
  customDirectory.value = true
}

function isReady(row: Campaign) {
  return readinessItems(row).every(item => item.ok)
}

function startHint(row: Campaign) {
  return isReady(row) ? t('campaign.ready') : readinessItems(row).filter(item => !item.ok).map(item => item.label).join('；')
}

async function handleSave() {
  if (!editForm.value.name) {
    ElMessage.warning('请输入活动名称')
    return
  }
  saving.value = true
  try {
    let payload: any = {
      name: editForm.value.name,
      start_time: editForm.value.start_time,
      end_time: editForm.value.end_time,
      send_start_time: editForm.value.send_start_time,
      send_end_time: editForm.value.send_end_time,
      send_days: editForm.value.send_days,
      batch_size: editForm.value.batch_size,
      campaign_dir: customDirectory.value ? editForm.value.campaign_dir : '',
      use_custom_directory: customDirectory.value,
      smpp_config_id: editForm.value.smpp_config_id || undefined,
      phone_prefix: editForm.value.phone_prefix || undefined,
    }
    if (editForm.value.id && editForm.value.status !== 'not_started' && editForm.value.status !== 'ended') {
      payload = { smpp_config_id: editForm.value.smpp_config_id || undefined }
    } else if (editForm.value.id && editForm.value.status === 'ended') {
      payload = {
        name: editForm.value.name,
        end_time: editForm.value.end_time,
        send_start_time: editForm.value.send_start_time,
        send_end_time: editForm.value.send_end_time,
        send_days: editForm.value.send_days,
        smpp_config_id: editForm.value.smpp_config_id || undefined,
      }
    }
    if (editForm.value.id) {
      await api.updateCampaign(editForm.value.id, payload)
    } else {
      await api.createCampaign(payload)
    }
    dialogVisible.value = false
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '操作失败')
  } finally {
    saving.value = false
  }
}

function handleUpload(row: Campaign) {
  uploadCampaignId.value = row.id
  selectedFile.value = null
  uploadVisible.value = true
}

function downloadProspectTemplate() {
  const blob = new Blob(
    ['msisdn,region,language\n66812345678,TH,th\n'],
    { type: 'text/csv;charset=utf-8' }
  )
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'prospect_template.csv'
  a.click()
  URL.revokeObjectURL(url)
}

function handleFileChange(file: any) {
  selectedFile.value = file.raw
}

async function handleUploadSubmit() {
  if (!selectedFile.value) return
  uploading.value = true
  try {
    await api.uploadCustomerList(uploadCampaignId.value, selectedFile.value)
    ElMessage.success('上传成功')
    uploadVisible.value = false
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '上传失败')
  } finally {
    uploading.value = false
  }
}

async function handleStart(row: Campaign) {
  try {
    await api.updateCampaign(row.id, { status: 'in_progress' })
    ElMessage.success('已开始')
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '操作失败')
  }
}

async function handleEnd(row: Campaign) {
  try {
    await api.updateCampaign(row.id, { status: 'ended' })
    ElMessage.success('已结束')
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '操作失败')
  }
}

async function handleCopy(id: number) {
  try {
    await api.copyCampaign(id)
    ElMessage.success('复制成功')
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '复制失败')
  }
}

async function handleDelete(id: number) {
  try {
    await ElMessageBox.confirm('确认删除该活动？', '提示')
  } catch {
    return
  }

  try {
    await api.deleteCampaign(id)
    campaigns.value = campaigns.value.filter(item => item.id !== id)
    ElMessage.success('删除成功')
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '删除失败')
    // Reconcile with the server even when an older backend deleted the row but
    // incorrectly returned an error response.
    await fetchList()
  }
}

async function openSmsConfig(row: Campaign) {
  try {
    const res = await smsApi.getCampaignByMarketing('batch', row.id)
    let smsCampaignId = res.data?.id
    if (!smsCampaignId && row.sms_template_id) {
      const templates = await smsApi.getAllTemplates()
      smsCampaignId = templates.data.find((item: any) => item.template_id === row.sms_template_id)?.campaign_id
    }
    await router.push({
      path: smsCampaignId ? `/sms-content/campaign/${smsCampaignId}` : '/sms-content/campaign/new',
      query: {
        from: 'batch',
        campaignId: String(row.id),
        campaignName: row.name,
        smsTemplateId: row.sms_template_id ? String(row.sms_template_id) : undefined,
      },
    })
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '加载短信配置失败')
  }
}

function handleMore(command: string, row: Campaign) {
  if (command === 'edit') showDialog(row)
  if (command === 'upload') handleUpload(row)
  if (command === 'sms') void openSmsConfig(row)
  if (command === 'copy') handleCopy(row.id)
  if (command === 'delete') handleDelete(row.id)
}

onMounted(() => {
  void fetchList()
  void fetchSmppConfigs()
})
</script>

<style scoped>
.campaign-actions {
  display: flex;
  align-items: center;
  flex-wrap: nowrap;
  white-space: nowrap;
}
.page-header {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 20px;
}
.page-header h2 { margin: 0; font-size: 22px; font-weight: 600; }
.advanced-toggle { display:flex; align-items:center; gap:8px; }
.readiness-item { display:flex; align-items:center; gap:8px; margin-top:10px; color:var(--mc-text-secondary); }
.readiness-item .el-icon.ok { color:var(--mc-success); }
.readiness-item .el-icon:not(.ok) { color:var(--mc-warning); }
.pagination { justify-content:flex-end; padding:12px 0; }

</style>
