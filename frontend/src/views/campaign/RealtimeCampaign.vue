<template>
  <div class="batch-campaign">
    <div class="page-header">
      <h2>{{ $t('campaign.realtimeTitle') }}</h2>
      <el-button type="primary" @click="showDialog(null)">
        <el-icon><Plus /></el-icon>{{ $t('campaign.addCampaign') }}
      </el-button>
    </div>

    <el-table :data="campaigns" stripe v-loading="loading" style="width: 100%">
      <el-table-column prop="id" label="ID" width="70" />
      <el-table-column prop="name" :label="$t('campaign.name')" min-width="140" />
      <el-table-column :label="$t('campaign.receivedFiles')" width="100">
        <template #default="{ row }">
          {{ rtStats[row.id]?.file_count ?? 0 }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.matchedUsers')" width="100">
        <template #default="{ row }">
          {{ rtStats[row.id]?.total_matched ?? 0 }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.eventUsers')" width="100">
        <template #default="{ row }">
          {{ rtStats[row.id]?.total_msisdn_sum ?? 0 }}
        </template>
      </el-table-column>
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
      <el-table-column :label="$t('campaign.customerList')" min-width="180">
        <template #default="{ row }">
          <template v-if="row.customer_list_filename">
            <span>{{ row.customer_list_filename }}</span>
            <el-tag size="small" type="info" style="margin-left: 4px">{{ row.row_count }} rows</el-tag>
          </template>
          <span v-else style="color: #999">-</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('campaign.dirRoot')" min-width="200">
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
      <el-table-column :label="$t('smsContent.template')" min-width="160">
        <template #default="{ row }">
          <span v-if="row.sms_template_id" style="font-size:12px">#{{ row.sms_template_id }}</span>
          <span v-else style="color:#999">-</span>
        </template>
      </el-table-column>
      <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
      <el-table-column :label="$t('common.action')" width="420" fixed="right">
        <template #default="{ row }">
          <el-button text type="primary" size="small" @click="$router.push('/campaign/realtime/' + row.id)">
            {{ $t('campaign.detail') }}
          </el-button>
          <el-button v-if="row.status === 'not_started'" text type="primary" size="small" @click="showDialog(row)">
            {{ $t('common.edit') }}
          </el-button>
          <el-button text type="success" size="small" @click="handleUpload(row)">
            {{ $t('campaign.uploadList') }}
          </el-button>
          <el-button text type="warning" size="small" @click="handleReceiveFile(row)">
            {{ $t('campaign.receiveFile') }}
          </el-button>
          <el-button text type="warning" size="small" @click="$router.push({ path: '/sms-content/campaign/new', query: { from: 'realtime', campaignId: String(row.id), campaignName: row.name } })">
            {{ $t('smsContent.configSms') }}
          </el-button>
          <template v-if="row.status === 'not_started' && row.row_count > 0">
            <el-button text type="warning" size="small" @click="handleStart(row)">
              {{ $t('campaign.start') }}
            </el-button>
          </template>
          <template v-else-if="row.status === 'in_progress'">
            <el-button text type="danger" size="small" @click="handleEnd(row)">
              {{ $t('campaign.end') }}
            </el-button>
          </template>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog
      :model-value="dialogVisible"
      :title="editForm.id ? $t('common.edit') : $t('campaign.addCampaign')"
      width="520px"
      @close="dialogVisible = false"
    >
      <el-form :model="editForm" label-width="120px" size="small">
        <el-form-item :label="$t('campaign.name')" required>
          <el-input v-model="editForm.name" />
        </el-form-item>
        <el-form-item :label="$t('campaign.campaignDir')">
          <el-input v-model="editForm.campaign_dir" :placeholder="$t('campaign.campaignDirPlaceholder')" />
          <div style="font-size:12px;color:#909399;margin-top:4px">{{ $t('campaign.campaignDirTip') }}</div>
        </el-form-item>
        <el-form-item :label="$t('campaign.startTime')">
          <el-date-picker
            v-model="editForm.start_time"
            type="datetime"
            :placeholder="$t('campaign.selectTime')"
            value-format="YYYY-MM-DD HH:mm:ss"
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
        <el-form-item :label="$t('campaign.sendDays')">
          <el-checkbox-group v-model="editForm.send_days">
            <el-checkbox v-for="d in weekDays" :key="d.value" :label="d.value" size="small">
              {{ $t('campaign.day_' + d.value) }}
            </el-checkbox>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item :label="$t('smsContent.template')">
          <el-select v-model="editForm.sms_template_id" clearable filterable placeholder="选择短信模板" style="width:100%">
            <el-option v-for="t in templateOptions" :key="t.template_id" :value="t.template_id" :label="`#${t.template_id} ${t.campaign_name} (${t.language})`" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">{{ $t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="saving" @click="handleSave">{{ $t('common.save') }}</el-button>
      </template>
    </el-dialog>

    <el-dialog
      :model-value="uploadVisible"
      :title="$t('campaign.uploadList')"
      width="480px"
      @close="uploadVisible = false"
    >
      <el-upload
        drag
        :auto-upload="false"
        :show-file-list="true"
        :on-change="handleFileChange"
        accept=".csv,.xlsx,.xls"
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

    <el-dialog
      :model-value="receiveVisible"
      :title="$t('campaign.receiveFile')"
      width="480px"
      @close="receiveVisible = false"
    >
      <p style="margin-bottom: 12px; color: #606266">
        {{ $t('campaign.receiveFileHint') }}
      </p>
      <el-upload
        drag
        :auto-upload="false"
        :show-file-list="true"
        :on-change="handleReceiveFileChange"
        accept=".csv,.xlsx,.xls"
      >
        <el-icon class="el-icon--upload"><UploadFilled /></el-icon>
        <div class="el-upload__text">{{ $t('campaign.dropHint') }}</div>
      </el-upload>
      <div v-if="receiveResult" style="margin-top: 12px">
        <el-alert
          :title="`${$t('campaign.receiveFileTotal')}: ${receiveResult.total_msisdn}, ${$t('campaign.receiveFileMatched')}: ${receiveResult.matched_count}`"
          type="success"
          show-icon
          :closable="false"
        />
      </div>
      <template #footer>
        <el-button @click="receiveVisible = false">{{ $t('common.close') }}</el-button>
        <el-button type="primary" :loading="receiving" :disabled="!receiveFile" @click="handleReceiveSubmit">
          {{ $t('common.confirm') }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import StatusTag from '@/components/StatusTag.vue'
import * as api from '@/api/realtime-campaign'
import * as rtApi from '@/api/realtime'
import * as smsApi from '@/api/smsContent'

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
  customer_list_filename: string
  customer_list_path: string
  row_count: number
  campaign_dir: string
  sms_template_id: number | null
  created_at: string
}

const campaigns = ref<Campaign[]>([])
const rtStats = reactive<Record<number, { file_count: number; total_msisdn_sum: number; total_matched: number }>>({})
const loading = ref(false)
const dialogVisible = ref(false)
const saving = ref(false)
const templateOptions = ref<any[]>([])
const editForm = ref<{
  id: number; name: string; start_time: string; end_time: string;
  send_start_time: string; send_end_time: string; send_days: string[];
  campaign_dir: string; sms_template_id: number | null
}>({
  id: 0, name: '', start_time: '', end_time: '',
  send_start_time: '', send_end_time: '', send_days: [],
  campaign_dir: '', sms_template_id: null
})
const uploadVisible = ref(false)
const uploading = ref(false)
const uploadCampaignId = ref(0)
const selectedFile = ref<File | null>(null)
const receiveVisible = ref(false)
const receiving = ref(false)
const receiveCampaignId = ref(0)
const receiveFile = ref<File | null>(null)
const receiveResult = ref<{ total_msisdn: number; matched_count: number } | null>(null)

async function fetchList() {
  loading.value = true
  try {
    const res = await api.getRealtimeCampaigns()
    campaigns.value = res.data
    for (const c of res.data) {
      try {
        const sr = await rtApi.getRealtimeStats(c.id)
        rtStats[c.id] = sr.data
      } catch { /* ignore */ }
    }
  } finally {
    loading.value = false
  }
}

function showDialog(row: Campaign | null) {
  if (row) {
    editForm.value = {
      id: row.id, name: row.name, start_time: row.start_time, end_time: row.end_time,
      send_start_time: row.send_start_time || '', send_end_time: row.send_end_time || '',
      send_days: row.send_days || [],
      campaign_dir: row.campaign_dir || '', sms_template_id: row.sms_template_id ?? null,
    }
  } else {
    editForm.value = {
      id: 0, name: '', start_time: '', end_time: '',
      send_start_time: '', send_end_time: '', send_days: [], campaign_dir: '', sms_template_id: null,
    }
  }
  dialogVisible.value = true
}

async function handleSave() {
  if (!editForm.value.name) {
    ElMessage.warning('请输入活动名称')
    return
  }
  saving.value = true
  try {
    const payload = {
      name: editForm.value.name,
      start_time: editForm.value.start_time,
      end_time: editForm.value.end_time,
      send_start_time: editForm.value.send_start_time,
      send_end_time: editForm.value.send_end_time,
      send_days: editForm.value.send_days,
      campaign_dir: editForm.value.campaign_dir || undefined,
      sms_template_id: editForm.value.sms_template_id || undefined,
    }
    if (editForm.value.id) {
      await api.updateRealtimeCampaign(editForm.value.id, payload)
    } else {
      await api.createRealtimeCampaign(payload)
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

function handleFileChange(file: any) {
  selectedFile.value = file.raw
}

async function handleUploadSubmit() {
  if (!selectedFile.value) return
  uploading.value = true
  try {
    await api.uploadRealtimeCustomerList(uploadCampaignId.value, selectedFile.value)
    ElMessage.success('上传成功')
    uploadVisible.value = false
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '上传失败')
  } finally {
    uploading.value = false
  }
}

function handleReceiveFile(row: Campaign) {
  receiveCampaignId.value = row.id
  receiveFile.value = null
  receiveResult.value = null
  receiveVisible.value = true
}

function handleReceiveFileChange(file: any) {
  receiveFile.value = file.raw
}

async function handleReceiveSubmit() {
  if (!receiveFile.value) return
  receiving.value = true
  receiveResult.value = null
  try {
    const res = await rtApi.receiveRealtimeFile(receiveCampaignId.value, receiveFile.value)
    receiveResult.value = { total_msisdn: res.data.total_msisdn, matched_count: res.data.matched_count }
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '接收失败')
  } finally {
    receiving.value = false
  }
}

async function handleStart(row: Campaign) {
  try {
    await api.updateRealtimeCampaign(row.id, { status: 'in_progress' })
    ElMessage.success('已开始')
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '操作失败')
  }
}

async function handleEnd(row: Campaign) {
  try {
    await api.updateRealtimeCampaign(row.id, { status: 'ended' })
    ElMessage.success('已结束')
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '操作失败')
  }
}

async function fetchTemplates() {
  try {
    const res = await smsApi.getTemplatesForSelect()
    templateOptions.value = res.data
  } catch {}
}

onMounted(() => { fetchList(); fetchTemplates() })
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 20px;
}
.page-header h2 { margin: 0; font-size: 22px; font-weight: 600; }
</style>
