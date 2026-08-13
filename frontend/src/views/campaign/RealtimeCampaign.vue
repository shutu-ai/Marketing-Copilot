<template>
  <div class="batch-campaign">
    <PageHeader :title="$t('campaign.realtimeTitle')" :description="$t('campaign.realtimeDesc')">
      <template #actions>
        <el-button :loading="loading" @click="fetchList">
          <el-icon><Refresh /></el-icon>{{ $t('common.refresh') }}
        </el-button>
        <el-button type="primary" @click="showDialog(null)">
          <el-icon><Plus /></el-icon>{{ $t('campaign.addCampaign') }}
        </el-button>
      </template>
    </PageHeader>

    <div class="table-shell">
      <el-table :data="campaigns" stripe v-loading="loading" style="width: 100%">
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="name" :label="$t('campaign.name')" min-width="140" />
        <el-table-column :label="$t('campaign.receivedFiles')" width="100">
          <template #default="{ row }">{{ rtStats[row.id]?.file_count ?? 0 }}</template>
        </el-table-column>
        <el-table-column :label="$t('campaign.matchedUsers')" width="100">
          <template #default="{ row }">{{ rtStats[row.id]?.total_matched ?? 0 }}</template>
        </el-table-column>
        <el-table-column :label="$t('campaign.eventUsers')" width="100">
          <template #default="{ row }">{{ rtStats[row.id]?.total_msisdn_sum ?? 0 }}</template>
        </el-table-column>
        <el-table-column prop="start_time" :label="$t('campaign.startTime')" width="170" />
        <el-table-column prop="end_time" :label="$t('campaign.endTime')" width="170" />
        <el-table-column :label="$t('campaign.sendStartTime')" width="90">
          <template #default="{ row }">{{ row.send_start_time || '-' }}</template>
        </el-table-column>
        <el-table-column :label="$t('campaign.sendEndTime')" width="90">
          <template #default="{ row }">{{ row.send_end_time || '-' }}</template>
        </el-table-column>
        <el-table-column :label="$t('campaign.sendDays')" min-width="220">
          <template #default="{ row }">
            <template v-if="row.send_days?.length">
              <el-tag v-for="d in row.send_days" :key="d" size="small" style="margin: 1px">
                {{ $t('campaign.day_' + d) }}
              </el-tag>
            </template>
            <span v-else class="muted">-</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('campaign.status')" width="110">
          <template #default="{ row }">
            <StatusTag :status="row.status">{{ $t('campaign.status_' + row.status) }}</StatusTag>
          </template>
        </el-table-column>
        <el-table-column :label="$t('campaign.activityWhitelist')" min-width="180">
          <template #default="{ row }">
            <template v-if="row.customer_list_filename">
              <span>{{ row.customer_list_filename }}</span>
              <el-tag size="small" type="info" style="margin-left: 4px">{{ row.row_count }} rows</el-tag>
            </template>
            <span v-else class="muted">-</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('campaign.watchDir')" min-width="220">
          <template #default="{ row }">
            <code class="path-code">{{ row.watch_dir || '-' }}</code>
          </template>
        </el-table-column>
        <el-table-column :label="$t('campaign.watchFilenamePrefix')" min-width="220">
          <template #default="{ row }">
            <code class="path-code">{{ row.watch_filename_prefix || '-' }}</code>
          </template>
        </el-table-column>
        <el-table-column :label="$t('campaign.dirRoot')" min-width="220" class-name="desktop-only" label-class-name="desktop-only">
          <template #default="{ row }">
            <code class="path-code">{{ row.campaign_dir || '-' }}</code>
          </template>
        </el-table-column>
        <el-table-column :label="$t('campaign.associateSmpp')" min-width="180">
          <template #default="{ row }">
            <span v-if="row.smpp_config_id" style="font-size:12px">{{ smppName(row.smpp_config_id) }}</span>
            <span v-else class="muted">-</span>
          </template>
        </el-table-column>
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
        <el-table-column :label="$t('campaign.readiness')" width="130" fixed="right">
          <template #default="{ row }">
            <el-popover placement="left" :width="300" trigger="hover">
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
        <el-table-column :label="$t('common.action')" width="150" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="$router.push('/campaign/realtime/' + row.id)">
              {{ $t('campaign.detail') }}
            </el-button>
            <template v-if="row.status === 'not_started' || row.status === 'ended'">
              <span :title="startHint(row)">
                <el-button text type="warning" size="small" :disabled="!isReady(row)" @click="handleStart(row)">
                  {{ row.status === 'ended' ? $t('campaign.restart') : $t('campaign.start') }}
                </el-button>
              </span>
            </template>
            <template v-else-if="row.status === 'in_progress'">
              <el-button text type="danger" size="small" @click="handleEnd(row)">
                {{ $t('campaign.stop') }}
              </el-button>
            </template>
            <el-dropdown trigger="click" @command="handleMore($event, row)">
              <el-button text size="small">{{ $t('common.more') }}<el-icon><ArrowDown /></el-icon></el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="edit">{{ $t('common.edit') }}</el-dropdown-item>
                  <el-dropdown-item command="upload" :disabled="row.use_whitelist === false">
                    {{ row.status === 'in_progress' ? i18n.global.t('legacy.xecf4ee8a') : $t('campaign.uploadWhitelist') }}
                  </el-dropdown-item>
                  <el-dropdown-item command="sms">
                    {{ row.status === 'in_progress' ? i18n.global.t('legacy.x2c4cd0cb') : $t('smsContent.configSms') }}
                  </el-dropdown-item>
                  <el-dropdown-item command="copy">{{ $t('campaign.copy') }}</el-dropdown-item>
                  <el-dropdown-item v-if="row.status !== 'in_progress'" command="delete" divided>{{ $t('common.delete') }}</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
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
      width="560px"
      align-center
      @close="dialogVisible = false"
    >
      <el-form :model="editForm" label-width="120px" size="small">
        <template v-if="!inProgressEdit">
        <el-form-item :label="$t('campaign.name')" required>
          <el-input v-model="editForm.name" />
        </el-form-item>
        <el-form-item :label="$t('legacy.xb1b4be1b')">
          <el-switch v-model="editForm.use_whitelist" :active-text="$t('legacy.xdf16ffce')" in:active-text="$t('legacy.x8755a53c')" />
        </el-form-item>
        <div v-if="editForm.status !== 'ended'" class="advanced-toggle">
          <el-switch v-model="customDirectory" :active-text="$t('campaign.customDirectory')" @change="handleCustomDirectoryChange" />
          <el-tag size="small" type="info" effect="plain">{{ $t('campaign.optional') }}</el-tag>
        </div>
        <el-form-item v-if="customDirectory" :label="$t('campaign.campaignDir')">
          <el-input v-model="editForm.campaign_dir" :placeholder="$t('campaign.campaignDirPlaceholder')">
            <template #append><el-button @click="openDirectoryPicker('campaign')">{{ $t('campaign.browseDirectory') }}</el-button></template>
          </el-input>
          <div class="form-tip">{{ $t('campaign.campaignDirTip') }}</div>
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
        <el-form-item :label="$t('campaign.watchDir')" required>
          <el-input v-model="editForm.watch_dir" :placeholder="$t('campaign.watchDirPlaceholder')" :disabled="editForm.status === 'in_progress'">
            <template #append>
              <el-button :disabled="editForm.status === 'in_progress'" @click="openDirectoryPicker('watch')">{{ $t('campaign.browseDirectory') }}</el-button>
            </template>
          </el-input>
          <div class="form-tip">{{ $t('campaign.watchDirTip') }}</div>
        </el-form-item>
        <el-form-item :label="$t('campaign.watchFilenamePrefix')" required>
          <el-input v-model="editForm.watch_filename_prefix" placeholder="Android_4G_Switch_Somali_" :disabled="editForm.status === 'in_progress'" />
          <div class="form-tip">{{ $t('campaign.watchFilenamePrefixTip') }}</div>
        </el-form-item>
        <el-form-item :label="$t('campaign.phonePrefix')" required>
          <el-input v-model="editForm.phone_prefix" placeholder="251" :disabled="editForm.status === 'in_progress'" />
          <div class="form-tip">{{ $t('campaign.phonePrefixTip') }}</div>
        </el-form-item>
       <el-form-item :label="$t('campaign.sendStartTime')">
          <el-time-picker v-model="editForm.send_start_time" :placeholder="$t('campaign.selectTime')" value-format="HH:mm" format="HH:mm" style="width: 100%" />
        </el-form-item>
        <el-form-item :label="$t('campaign.sendEndTime')">
          <el-time-picker v-model="editForm.send_end_time" :placeholder="$t('campaign.selectTime')" value-format="HH:mm" format="HH:mm" style="width: 100%" />
        </el-form-item>
        <el-form-item :label="$t('campaign.sendDays')">
          <el-checkbox-group v-model="editForm.send_days">
            <el-checkbox v-for="d in weekDays" :key="d.value" :label="d.value" size="small">
              {{ $t('campaign.day_' + d.value) }}
            </el-checkbox>
          </el-checkbox-group>
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

    <DirectoryPicker v-model="directoryPickerVisible" :initial-path="directoryPickerTarget === 'watch' ? editForm.watch_dir : editForm.campaign_dir" @select="selectDirectory" />

    <el-dialog
      :model-value="uploadVisible"
      :title="$t('campaign.uploadWhitelist')"
      width="480px"
      @close="uploadVisible = false"
    >
      <div style="margin-bottom: 12px; text-align: right">
        <el-button size="small" @click="downloadWhitelistTemplate">
          <el-icon><Download /></el-icon>
          {{ $t('campaign.downloadTemplate') }}
        </el-button>
      </div>
     <el-upload
       drag
       :key="uploadKey"
       :auto-upload="false"
       :show-file-list="true"
       :on-change="handleFileChange"
       accept=".csv,.xlsx,.xls,.txt"
     >
        <el-icon class="el-icon--upload"><UploadFilled /></el-icon>
        <div class="el-upload__text">{{ $t('campaign.dropHint') }}</div>
      </el-upload>
      <el-progress
        v-if="uploadPercent > 0"
        :percentage="uploadPercent"
        :stroke-width="6"
        :show-text="true"
        style="margin-top: 12px"
      />
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
import i18n from '@/i18n'
import { ref, reactive, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import StatusTag from '@/components/StatusTag.vue'
import PageHeader from '@/components/PageHeader.vue'
import DirectoryPicker from '@/components/DirectoryPicker.vue'
import * as api from '@/api/realtime-campaign'
import * as rtApi from '@/api/realtime'
import * as smsApi from '@/api/smsContent'
import * as smppApi from '@/api/smpp'
import { useI18n } from 'vue-i18n'
import { uploadRealtimeCustomerListWithProgress } from '@/api/realtime-campaign'
import { useRouter } from 'vue-router'

const { t } = useI18n()
const router = useRouter()

const weekDays = [
  { value: '1' },
  { value: '2' },
  { value: '3' },
  { value: '4' },
  { value: '5' },
  { value: '6' },
  { value: '0' },
]

interface Campaign {
  id: number
  name: string
  use_whitelist: boolean
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
  use_custom_directory: boolean
  watch_dir: string
 watch_filename_prefix: string
 phone_prefix: string
 sms_template_id: number | null
 smpp_config_id: number | null
  created_at: string
}

const campaigns = ref<Campaign[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(50)
const rtStats = reactive<Record<number, { file_count: number; total_msisdn_sum: number; total_matched: number }>>({})
const loading = ref(false)
const dialogVisible = ref(false)
const saving = ref(false)
const smppOptions = ref<any[]>([])
const enabledSmppOptions = computed(() => smppOptions.value.filter((item: any) => item.enabled === 1))
const editForm = ref<{
  id: number
  name: string
  use_whitelist: boolean
  start_time: string
  end_time: string
  send_start_time: string
  send_end_time: string
  send_days: string[]
  campaign_dir: string
  watch_dir: string
 watch_filename_prefix: string
 phone_prefix: string
 smpp_config_id: number | null
 status: string
}>({
  id: 0,
  name: '',
  use_whitelist: true,
  start_time: '',
  end_time: '',
  send_start_time: '',
  send_end_time: '',
  send_days: [],
  campaign_dir: '',
  watch_dir: '',
 watch_filename_prefix: 'Android_4G_Switch_Somali_',
 phone_prefix: '',
 smpp_config_id: null,
 status: 'not_started',
})
const uploadVisible = ref(false)
const uploading = ref(false)
const uploadCampaignId = ref(0)
const selectedFile = ref<File | null>(null)
const uploadPercent = ref(0)
const uploadKey = ref(0)
const customDirectory = ref(false)
const inProgressEdit = ref(false)
const directoryPickerVisible = ref(false)
const directoryPickerTarget = ref<'campaign' | 'watch'>('campaign')

async function fetchList() {
  loading.value = true
  try {
    const res = await api.getRealtimeCampaigns({ page: page.value, page_size: pageSize.value })
    campaigns.value = res.data.items || []
    total.value = res.data.total ?? campaigns.value.length
    for (const campaign of campaigns.value) {
      try {
        const sr = await rtApi.getRealtimeStats(campaign.id)
        rtStats[campaign.id] = sr.data
      } catch {
        // ignore per-row stats failures
      }
    }
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
      id: row.id,
      name: row.name,
      use_whitelist: row.use_whitelist !== false,
      start_time: row.start_time,
      end_time: row.end_time,
      send_start_time: row.send_start_time || '',
      send_end_time: row.send_end_time || '',
      send_days: row.send_days || [],
      campaign_dir: row.campaign_dir || '',
      watch_dir: row.watch_dir || '',
     watch_filename_prefix: row.watch_filename_prefix || 'Android_4G_Switch_Somali_',
     phone_prefix: row.phone_prefix || '',
     smpp_config_id: row.smpp_config_id ?? null,
      status: row.status,
    }
  } else {
    inProgressEdit.value = false
    customDirectory.value = false
    editForm.value = {
      id: 0,
      name: '',
      use_whitelist: true,
      start_time: '',
      end_time: '',
      send_start_time: '',
      send_end_time: '',
      send_days: [],
      campaign_dir: '',
      watch_dir: '',
     watch_filename_prefix: 'Android_4G_Switch_Somali_',
     phone_prefix: '',
     smpp_config_id: null,
     status: 'not_started',
   }
 }
 dialogVisible.value = true
}

function readinessItems(row: Campaign) {
  const scheduleReady = Boolean(row.start_time && row.end_time && row.send_start_time && row.send_end_time)
  const watchFilenamePrefix = row.watch_filename_prefix || 'Android_4G_Switch_Somali_'
  return [
    { ok: row.use_whitelist === false || row.row_count > 0, label: row.use_whitelist === false ? i18n.global.t('legacy.xc9b0bc31') : t(row.row_count > 0 ? 'campaign.whitelistReady' : 'campaign.whitelistMissing') },
    { ok: Boolean(row.sms_template_id), label: t(row.sms_template_id ? 'campaign.templateReady' : 'campaign.templateMissing') },
    { ok: Boolean(row.smpp_config_id), label: t(row.smpp_config_id ? 'campaign.smppReady' : 'campaign.smppMissing') },
    { ok: scheduleReady, label: t(scheduleReady ? 'campaign.scheduleReady' : 'campaign.scheduleMissing') },
    { ok: Boolean(row.watch_dir), label: t(row.watch_dir ? 'campaign.watchDirReady' : 'campaign.watchDirMissing') },
    { ok: Boolean(watchFilenamePrefix), label: t(watchFilenamePrefix ? 'campaign.watchFilenamePrefixReady' : 'campaign.watchFilenamePrefixMissing') },
  ]
}

function handleCustomDirectoryChange(enabled: boolean) {
  if (!enabled) editForm.value.campaign_dir = ''
}

function openDirectoryPicker(target: 'campaign' | 'watch') {
  directoryPickerTarget.value = target
  directoryPickerVisible.value = true
}

function selectDirectory(path: string) {
  if (directoryPickerTarget.value === 'watch') {
    editForm.value.watch_dir = path
  } else {
    editForm.value.campaign_dir = path
    customDirectory.value = true
  }
}

function isReady(row: Campaign) {
  return readinessItems(row).every(item => item.ok)
}

function startHint(row: Campaign) {
  return isReady(row) ? t('campaign.ready') : readinessItems(row).filter(item => !item.ok).map(item => item.label).join('，')
}

async function handleSave() {
  if (!editForm.value.name) {
    ElMessage.warning(i18n.global.t('legacy.x7528b32e'))
    return
  }
  if (!editForm.value.id && !editForm.value.phone_prefix.trim()) {
    ElMessage.warning('\u8bf7\u8f93\u5165\u53f7\u7801\u524d\u7f00')
    return
  }
  saving.value = true
  try {
    let payload: any = {
      name: editForm.value.name,
      use_whitelist: editForm.value.use_whitelist,
      start_time: editForm.value.start_time,
      end_time: editForm.value.end_time,
      send_start_time: editForm.value.send_start_time,
      send_end_time: editForm.value.send_end_time,
      send_days: editForm.value.send_days,
      campaign_dir: customDirectory.value ? editForm.value.campaign_dir : '',
      use_custom_directory: customDirectory.value,
      watch_dir: editForm.value.watch_dir,
     watch_filename_prefix: editForm.value.watch_filename_prefix,
     phone_prefix: editForm.value.phone_prefix,
     smpp_config_id: editForm.value.smpp_config_id || undefined,
   }
    if (editForm.value.id && editForm.value.status !== 'not_started' && editForm.value.status !== 'ended') {
      payload = { smpp_config_id: editForm.value.smpp_config_id || undefined }
    } else if (editForm.value.id && editForm.value.status === 'ended') {
      payload = {
        end_time: editForm.value.end_time,
       send_start_time: editForm.value.send_start_time,
       send_end_time: editForm.value.send_end_time,
       send_days: editForm.value.send_days,
       watch_dir: editForm.value.watch_dir,
       watch_filename_prefix: editForm.value.watch_filename_prefix,
       phone_prefix: editForm.value.phone_prefix,
       smpp_config_id: editForm.value.smpp_config_id || undefined,
      }
    }
    if (editForm.value.id) {
      await api.updateRealtimeCampaign(editForm.value.id, payload)
    } else {
      await api.createRealtimeCampaign(payload)
    }
    dialogVisible.value = false
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.x5fa802be'))
  } finally {
    saving.value = false
  }
}

function handleUpload(row: Campaign) {
  if (row.use_whitelist === false) return
  uploadCampaignId.value = row.id
  selectedFile.value = null
  uploadKey.value++
  uploadVisible.value = true
}

function handleFileChange(file: any) {
  selectedFile.value = file.raw
}

async function handleUploadSubmit() {
  if (!selectedFile.value) return
  uploadPercent.value = 0
  uploading.value = true
  try {
    await uploadRealtimeCustomerListWithProgress(
      uploadCampaignId.value,
      selectedFile.value,
      (percent) => { uploadPercent.value = percent },
    )
    ElMessage.success(i18n.global.t('legacy.xa7699ba7'))
    uploadVisible.value = false
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.x54e5de42'))
  } finally {
    uploadPercent.value = 0
    uploading.value = false
  }
}

async function handleStart(row: Campaign) {
  try {
    await api.updateRealtimeCampaign(row.id, { status: 'in_progress' })
    ElMessage.success(row.status === 'ended' ? i18n.global.t('legacy.x5a684a25') : i18n.global.t('legacy.xde05b7cf'))
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.x5fa802be'))
  }
}

async function handleEnd(row: Campaign) {
  try {
    await api.updateRealtimeCampaign(row.id, { status: 'ended' })
    ElMessage.success(i18n.global.t('legacy.x82977854'))
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.x5fa802be'))
  }
}

async function handleDelete(id: number) {
  try {
    await ElMessageBox.confirm(i18n.global.t('smsContent.confirmDeleteCampaign'), i18n.global.t('legacy.x02d9819d'))
  } catch {
    return
  }

  try {
    await api.deleteRealtimeCampaign(id)
    campaigns.value = campaigns.value.filter(item => item.id !== id)
    ElMessage.success(i18n.global.t('legacy.x0007d170'))
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.xacf0664a'))
    await fetchList()
  }
}

async function handleCopy(row: Campaign) {
  try {
    await api.copyRealtimeCampaign(row.id)
    ElMessage.success(i18n.global.t('legacy.x20a49536'))
    await fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.x5154ae17'))
  }
}

function downloadWhitelistTemplate() {
  const blob = new Blob(
    ['msisdn,language\n251911111111,English\n'],
    { type: 'text/csv;charset=utf-8' }
  )
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'whitelist_template.csv'
  a.click()
  URL.revokeObjectURL(url)
}

async function fetchSmppConfigs() {
  try {
    const res = await smppApi.getSmppConfigs()
    smppOptions.value = res.data
  } catch {
    // ignore
  }
}

function smppName(configId: number | null) {
  const cfg = smppOptions.value.find((item: any) => item.id === configId)
  return cfg ? `${cfg.name} (${cfg.host}:${cfg.port})` : `#${configId}`
}

async function openSmsConfig(row: Campaign) {
  try {
    const res = await smsApi.getCampaignByMarketing('realtime', row.id)
    let smsCampaignId = res.data?.id
    if (!smsCampaignId && row.sms_template_id) {
      const templates = await smsApi.getAllTemplates()
      smsCampaignId = templates.data.find((item: any) => item.template_id === row.sms_template_id)?.campaign_id
    }
    await router.push({
      path: smsCampaignId ? `/sms-content/campaign/${smsCampaignId}` : '/sms-content/campaign/new',
      query: {
        from: 'realtime',
        campaignId: String(row.id),
        campaignName: row.name,
        smsTemplateId: row.sms_template_id ? String(row.sms_template_id) : undefined,
      },
    })
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || i18n.global.t('legacy.x93347d5b'))
  }
}

function handleMore(command: string, row: Campaign) {
  if (command === 'edit') showDialog(row)
  if (command === 'upload') handleUpload(row)
  if (command === 'sms') void openSmsConfig(row)
  if (command === 'copy') void handleCopy(row)
  if (command === 'delete') void handleDelete(row.id)
}

onMounted(() => {
  void fetchList()
  void fetchSmppConfigs()
})
</script>

<style scoped>
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 20px;
}
.page-header h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 600;
}
.advanced-toggle {
  display: flex;
  align-items: center;
  gap: 8px;
}
.form-tip {
  font-size: 12px;
  color: var(--mc-text-tertiary);
  margin-top: 4px;
}
.readiness {
  display: flex;
  align-items: center;
  gap: 6px;
}
.readiness__summary {
  color: var(--mc-text-tertiary);
  font-size: 12px;
}
.readiness-item {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 10px;
  color: var(--mc-text-secondary);
}
.readiness-item .el-icon.ok {
  color: var(--mc-success);
}
.readiness-item .el-icon:not(.ok) {
  color: var(--mc-warning);
}
.path-code {
  font-size: 11px;
  word-break: break-all;
}
.muted {
  color: #999;
}
.pagination {
  justify-content: flex-end;
  padding: 12px 0;
}
</style>
