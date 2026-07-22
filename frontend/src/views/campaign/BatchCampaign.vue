<template>
  <div class="batch-campaign">
    <div class="page-header">
      <h2>{{ $t('campaign.batchTitle') }}</h2>
      <el-button type="primary" @click="showDialog(null)">
        <el-icon><Plus /></el-icon>{{ $t('campaign.addCampaign') }}
      </el-button>
    </div>

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
  <el-table-column :label="$t('campaign.batchSize')" width="90">
    <template #default="{ row }">
      {{ row.batch_size || 500 }}
    </template>
  </el-table-column>
  <el-table-column :label="$t('smsContent.template')" width="90">
    <template #default="{ row }">
      <span v-if="row.sms_template_id">#{{ row.sms_template_id }}</span>
      <span v-else style="color:#ccc">-</span>
    </template>
  </el-table-column>
  <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
      <el-table-column :label="$t('common.action')" width="380" fixed="right">
        <template #default="{ row }">
          <el-button text type="primary" size="small" @click="$router.push('/campaign/batch/' + row.id)">
            {{ $t('campaign.detail') }}
          </el-button>
          <el-button v-if="row.status === 'not_started'" text type="primary" size="small" @click="showDialog(row)">
            {{ $t('common.edit') }}
          </el-button>
          <template v-if="row.status === 'not_started'">
            <el-button text type="success" size="small" @click="handleUpload(row)">
              {{ $t('campaign.uploadList') }}
            </el-button>
            <el-button text type="warning" size="small" @click="$router.push({ path: '/sms-content/campaign/new', query: { from: 'batch', campaignId: String(row.id), campaignName: row.name } })">
              {{ $t('smsContent.configSms') }}
            </el-button>
            <el-button v-if="row.row_count > 0 && row.sms_template_id" text type="warning" size="small" @click="handleStart(row)">
              {{ $t('campaign.start') }}
            </el-button>
          </template>
          <template v-else-if="row.status === 'in_progress'">
            <el-button text type="danger" size="small" @click="handleEnd(row)">
              {{ $t('campaign.end') }}
            </el-button>
          </template>
          <template v-else-if="row.status === 'ended'">
            <el-button text type="success" size="small" @click="handleUpload(row)">
              {{ $t('campaign.uploadList') }}
            </el-button>
            <el-button text type="warning" size="small" @click="$router.push({ path: '/sms-content/campaign/new', query: { from: 'batch', campaignId: String(row.id), campaignName: row.name } })">
              {{ $t('smsContent.configSms') }}
            </el-button>
          </template>
          <el-button text type="info" size="small" @click="handleCopy(row.id)">
            {{ $t('campaign.copy') }}
          </el-button>
          <el-button v-if="row.status !== 'in_progress'" text type="danger" size="small" @click="handleDelete(row.id)">
            {{ $t('common.delete') }}
          </el-button>
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
          <el-input v-model="editForm.campaign_dir" placeholder="$t('campaign.campaignDirPlaceholder')" />
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
        <el-form-item :label="$t('campaign.batchSize')">
          <el-input-number v-model="editForm.batch_size" :min="1" :max="10000" style="width: 100%" />
        </el-form-item>
        <el-form-item :label="$t('campaign.sendDays')">
          <el-checkbox-group v-model="editForm.send_days">
            <el-checkbox v-for="d in weekDays" :key="d.value" :label="d.value" size="small">
              {{ $t('campaign.day_' + d.value) }}
            </el-checkbox>
          </el-checkbox-group>
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
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import StatusTag from '@/components/StatusTag.vue'
import * as api from '@/api/campaign'

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
  created_at: string
}

const campaigns = ref<Campaign[]>([])
const loading = ref(false)
const dialogVisible = ref(false)
const saving = ref(false)
const editForm = ref<{
  id: number; name: string; start_time: string; end_time: string;
  send_start_time: string; send_end_time: string; send_days: string[];
  batch_size: number; campaign_dir: string
}>({
  id: 0, name: '', start_time: '', end_time: '',
  send_start_time: '', send_end_time: '', send_days: [],
  batch_size: 500, campaign_dir: ''
})
const uploadVisible = ref(false)
const uploading = ref(false)
const uploadCampaignId = ref(0)
const selectedFile = ref<File | null>(null)

async function fetchList() {
  loading.value = true
  try {
    const res = await api.getCampaigns()
    campaigns.value = res.data
  } finally {
    loading.value = false
  }
}

function showDialog(row: Campaign | null) {
  if (row) {
    editForm.value = {
      id: row.id, name: row.name, start_time: row.start_time, end_time: row.end_time,
      send_start_time: row.send_start_time || '', send_end_time: row.send_end_time || '',
      send_days: row.send_days || [], batch_size: row.batch_size || 500,
      campaign_dir: row.campaign_dir || '',
    }
  } else {
    editForm.value = {
      id: 0, name: '', start_time: '', end_time: '',
      send_start_time: '', send_end_time: '', send_days: [], batch_size: 500, campaign_dir: '',
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
      batch_size: editForm.value.batch_size,
      campaign_dir: editForm.value.campaign_dir || undefined,
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
    await api.deleteCampaign(id)
    ElMessage.success('删除成功')
    await fetchList()
  } catch {}
}

onMounted(() => fetchList())
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 20px;
}
.page-header h2 { margin: 0; font-size: 22px; font-weight: 600; }

</style>
