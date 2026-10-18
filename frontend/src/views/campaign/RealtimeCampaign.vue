<template>
  <div class="batch-campaign">
    <PageHeader :title="$t('campaign.realtimeTitle')" :description="$t('campaign.realtimeDesc')">
      <template #actions>
        <el-button :loading="loading" @click="fetchList">
          <el-icon><Refresh /></el-icon>{{ $t('common.refresh') }}
        </el-button>
        <el-button v-if="canManage" type="primary" @click="showDialog(null)">
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
            <el-tag v-if="row.watch_source === 'sftp'" size="small" type="warning">SFTP</el-tag>
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
                <el-button v-if="canManage" text type="warning" size="small" :disabled="!isReady(row)" @click="handleStart(row)">
                  {{ row.status === 'ended' ? $t('campaign.restart') : $t('campaign.start') }}
                </el-button>
              </span>
            </template>
            <template v-else-if="row.status === 'in_progress'">
              <el-button v-if="canManage" text type="danger" size="small" @click="handleEnd(row)">
                {{ $t('campaign.stop') }}
              </el-button>
            </template>
            <el-dropdown v-if="canManage" trigger="click" @command="handleMore($event, row)">
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
      v-model="dialogVisible"
      class="realtime-editor-dialog"
      :title="editForm.id ? $t('common.edit') : $t('campaign.addCampaign')"
      width="860px"
      align-center
      :close-on-click-modal="false"
      @close="dialogVisible = false"
    >
      <div class="editor-intro">
        <div>
          <strong>{{ editForm.id ? $t('common.edit') : $t('campaign.addCampaign') }}</strong>
          <span>{{ inProgressEdit ? $t('campaign.inProgressSmppOnly') : $t('campaign.editorBasicsHint') }}</span>
        </div>
        <el-tag v-if="editForm.id" size="small" effect="plain" type="info">
          {{ $t('campaign.status_' + editForm.status) }}
        </el-tag>
      </div>

      <el-form :model="editForm" label-position="top" size="small" class="campaign-editor-form">
        <el-tabs v-model="activeEditorTab" class="campaign-editor-tabs" stretch>
          <el-tab-pane v-if="!inProgressEdit" name="basics" :label="$t('campaign.basicInfo')">
            <div class="editor-section-intro">
              <div>
                <strong>{{ $t('campaign.basicInfo') }}</strong>
                <span>{{ $t('campaign.editorBasicsHint') }}</span>
              </div>
            </div>
            <div class="form-grid">
              <el-form-item class="form-grid__full" :label="$t('campaign.name')" required>
                <el-input v-model="editForm.name" />
              </el-form-item>
              <div class="field-card">
                <span class="field-card__label">{{ $t('legacy.xb1b4be1b') }}</span>
                <el-switch v-model="editForm.use_whitelist" :active-text="$t('legacy.xdf16ffce')" :inactive-text="$t('legacy.x8755a53c')" />
              </div>
              <div v-if="editForm.status !== 'ended'" class="field-card">
                <span class="field-card__label">{{ $t('campaign.customDirectory') }}</span>
                <div class="field-card__control">
                  <el-switch v-model="customDirectory" @change="handleCustomDirectoryChange" />
                  <el-tag size="small" type="info" effect="plain">{{ $t('campaign.optional') }}</el-tag>
                </div>
              </div>
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
              <el-form-item v-if="customDirectory" class="form-grid__full" :label="$t('campaign.campaignDir')">
                <el-input v-model="editForm.campaign_dir" :placeholder="$t('campaign.campaignDirPlaceholder')">
                  <template #append><el-button @click="openDirectoryPicker('campaign')">{{ $t('campaign.browseDirectory') }}</el-button></template>
                </el-input>
                <div class="form-tip">{{ $t('campaign.campaignDirTip') }}</div>
              </el-form-item>
            </div>
          </el-tab-pane>

          <el-tab-pane v-if="!inProgressEdit" name="source" :label="$t('campaign.watchSource')">
            <div class="editor-section-intro">
              <div>
                <strong>{{ $t('campaign.watchSource') }}</strong>
                <span>{{ $t('campaign.editorSourceHint') }}</span>
              </div>
              <el-tag size="small" :type="editForm.watch_source === 'sftp' ? 'warning' : 'success'" effect="plain">
                {{ editForm.watch_source === 'sftp' ? 'SSH / SFTP' : $t('campaign.localWatchSource') }}
              </el-tag>
            </div>
            <div class="form-grid">
              <el-form-item class="form-grid__full" :label="$t('campaign.watchSource')" required>
                <el-radio-group v-model="editForm.watch_source" class="source-switch" :disabled="editForm.status === 'in_progress'">
                  <el-radio-button value="local">{{ $t('campaign.localWatchSource') }}</el-radio-button>
                  <el-radio-button value="sftp">SSH / SFTP</el-radio-button>
                </el-radio-group>
              </el-form-item>
              <el-form-item class="form-grid__full" :label="$t('campaign.watchDir')" required>
                <el-input v-model="editForm.watch_dir" :placeholder="editForm.watch_source === 'sftp' ? $t('campaign.sftpPathPlaceholder') : $t('campaign.watchDirPlaceholder')" :disabled="editForm.status === 'in_progress'">
                  <template v-if="editForm.watch_source === 'local'" #append>
                    <el-button :disabled="editForm.status === 'in_progress'" @click="openDirectoryPicker('watch')">{{ $t('campaign.browseDirectory') }}</el-button>
                  </template>
                </el-input>
                <div class="form-tip">{{ editForm.watch_source === 'sftp' ? $t('campaign.sftpPathTip') : $t('campaign.watchDirTip') }}</div>
              </el-form-item>
              <div v-if="editForm.watch_source === 'sftp'" class="sftp-panel form-grid__full">
                <div class="sftp-panel__header">
                  <div>
                    <strong>SSH / SFTP</strong>
                    <span>{{ $t('campaign.editorSftpHint') }}</span>
                  </div>
                  <el-button type="primary" plain :loading="testingSftp" :disabled="editForm.status === 'in_progress'" @click="testSftpConnection">
                    {{ $t('campaign.testSftpConnection') }}
                  </el-button>
                </div>
                <div class="form-grid">
                  <el-form-item :label="$t('campaign.sftpHost')" required>
                    <el-input v-model="editForm.sftp_host" placeholder="sftp.example.com" :disabled="editForm.status === 'in_progress'" />
                  </el-form-item>
                  <el-form-item :label="$t('campaign.sftpPort')" required>
                    <el-input-number v-model="editForm.sftp_port" :min="1" :max="65535" :disabled="editForm.status === 'in_progress'" style="width:100%" />
                  </el-form-item>
                  <el-form-item :label="$t('campaign.sftpUsername')" required>
                    <el-input v-model="editForm.sftp_username" :disabled="editForm.status === 'in_progress'" />
                  </el-form-item>
                  <el-form-item :label="$t('campaign.sftpPassword')" required>
                    <el-input v-model="editForm.sftp_password" type="password" show-password autocomplete="new-password" :placeholder="$t('campaign.sftpPasswordPlaceholder')" :disabled="editForm.status === 'in_progress'" />
                    <div class="form-tip">{{ $t('campaign.sftpPasswordTip') }}</div>
                  </el-form-item>
                </div>
              </div>
              <el-form-item :label="$t('campaign.watchFilenamePrefix')" required>
                <el-input v-model="editForm.watch_filename_prefix" placeholder="Android_4G_Switch_Somali_" :disabled="editForm.status === 'in_progress'" />
                <div class="form-tip">{{ $t('campaign.watchFilenamePrefixTip') }}</div>
              </el-form-item>
              <el-form-item :label="$t('campaign.phonePrefix')" required>
                <el-input v-model="editForm.phone_prefix" placeholder="251" :disabled="editForm.status === 'in_progress'" />
                <div class="form-tip">{{ $t('campaign.phonePrefixTip') }}</div>
              </el-form-item>
            </div>
          </el-tab-pane>

          <el-tab-pane name="delivery" :label="$t('campaign.editorDelivery')">
            <div class="editor-section-intro">
              <div>
                <strong>{{ $t('campaign.editorDelivery') }}</strong>
                <span>{{ $t('campaign.editorDeliveryHint') }}</span>
              </div>
            </div>
            <el-alert
              v-if="inProgressEdit"
              :title="$t('campaign.inProgressSmppOnly')"
              type="info"
              show-icon
              :closable="false"
              class="editor-alert"
            />
            <div class="form-grid">
              <el-form-item :label="$t('campaign.sendStartTime')">
                <el-time-picker v-model="editForm.send_start_time" :placeholder="$t('campaign.selectTime')" value-format="HH:mm" format="HH:mm" style="width: 100%" />
              </el-form-item>
              <el-form-item :label="$t('campaign.sendEndTime')">
                <el-time-picker v-model="editForm.send_end_time" :placeholder="$t('campaign.selectTime')" value-format="HH:mm" format="HH:mm" style="width: 100%" />
              </el-form-item>
              <el-form-item class="form-grid__full" :label="$t('campaign.sendDays')">
                <el-checkbox-group v-model="editForm.send_days" class="send-days">
                  <el-checkbox v-for="d in weekDays" :key="d.value" :value="d.value" size="small">
                    {{ $t('campaign.day_' + d.value) }}
                  </el-checkbox>
                </el-checkbox-group>
              </el-form-item>
              <el-form-item class="form-grid__full" :label="$t('campaign.associateSmpp')" required>
                <el-select v-model="editForm.smpp_config_id" clearable filterable :placeholder="$t('campaign.associateSmppPlaceholder')" style="width:100%">
                  <el-option v-for="cfg in enabledSmppOptions" :key="cfg.id" :value="cfg.id" :label="`${cfg.name} (${cfg.host}:${cfg.port})`" />
                </el-select>
                <div class="form-tip">{{ $t('campaign.associateSmppTip') }}</div>
              </el-form-item>
              <div class="field-card form-grid__full">
                <span class="field-card__label">{{ $t('campaign.flashSms') }}</span>
                <div>
                  <el-checkbox v-model="editForm.flash_sms" :disabled="inProgressEdit || !editForm.smpp_config_id">
                    {{ $t('campaign.flashSms') }}
                  </el-checkbox>
                  <div class="form-tip">{{ $t('campaign.flashSmsTip') }}</div>
                </div>
              </div>
            </div>
          </el-tab-pane>
        </el-tabs>
      </el-form>
      <template #footer>
        <div class="editor-footer">
          <div class="editor-footer__status">
            <span>{{ activeEditorTabLabel }}</span>
            <el-tag v-if="editForm.watch_source === 'sftp' && !inProgressEdit" size="small" type="warning" effect="plain">SSH / SFTP</el-tag>
          </div>
          <div class="editor-footer__actions">
            <el-button v-if="!inProgressEdit && activeEditorTab !== 'basics'" @click="goPrevious">{{ $t('common.back') }}</el-button>
            <el-button v-if="!inProgressEdit && activeEditorTab !== 'delivery'" @click="goNext">{{ $t('common.next') }}</el-button>
            <el-button @click="dialogVisible = false">{{ $t('common.cancel') }}</el-button>
            <el-button v-if="canManage" type="primary" :loading="saving" @click="handleSave">{{ $t('common.save') }}</el-button>
          </div>
        </div>
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
import { useAuthStore } from '@/stores/auth'
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
const authStore = useAuthStore()
const canManage = computed(() => ['admin', 'marketing'].includes(authStore.role))

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
  watch_source: 'local' | 'sftp'
  watch_dir: string
  sftp_host: string
  sftp_port: number
  sftp_username: string
  sftp_password: string
 watch_filename_prefix: string
 phone_prefix: string
 sms_template_id: number | null
 smpp_config_id: number | null
 flash_sms: boolean
  created_at: string
}

const campaigns = ref<Campaign[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(50)
const rtStats = reactive<Record<number, { file_count: number; total_msisdn_sum: number; total_matched: number }>>({})
const loading = ref(false)
const testingSftp = ref(false)
const hasStoredSftpPassword = ref(false)
const dialogVisible = ref(false)
const activeEditorTab = ref<'basics' | 'source' | 'delivery'>('basics')
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
  watch_source: 'local' | 'sftp'
  watch_dir: string
  sftp_host: string
  sftp_port: number
  sftp_username: string
  sftp_password: string
 watch_filename_prefix: string
 phone_prefix: string
 smpp_config_id: number | null
 flash_sms: boolean
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
  watch_source: 'local',
  watch_dir: '',
  sftp_host: '',
  sftp_port: 22,
  sftp_username: '',
  sftp_password: '',
 watch_filename_prefix: 'Android_4G_Switch_Somali_',
 phone_prefix: '',
  smpp_config_id: null,
  flash_sms: false,
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
    hasStoredSftpPassword.value = Boolean(row.sftp_password)
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
      watch_source: row.watch_source || 'local',
      watch_dir: row.watch_dir || '',
      sftp_host: row.sftp_host || '',
      sftp_port: row.sftp_port || 22,
      sftp_username: row.sftp_username || '',
      sftp_password: row.sftp_password || '',
     watch_filename_prefix: row.watch_filename_prefix || 'Android_4G_Switch_Somali_',
     phone_prefix: row.phone_prefix || '',
     smpp_config_id: row.smpp_config_id ?? null,
     flash_sms: Boolean(row.flash_sms),
      status: row.status,
    }
  } else {
    hasStoredSftpPassword.value = false
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
      watch_source: 'local',
      watch_dir: '',
      sftp_host: '',
      sftp_port: 22,
      sftp_username: '',
      sftp_password: '',
     watch_filename_prefix: 'Android_4G_Switch_Somali_',
     phone_prefix: '',
     smpp_config_id: null,
     flash_sms: false,
     status: 'not_started',
   }
 }
 activeEditorTab.value = inProgressEdit.value ? 'delivery' : 'basics'
 dialogVisible.value = true
}

const activeEditorTabLabel = computed(() => {
  if (activeEditorTab.value === 'source') return t('campaign.watchSource')
  if (activeEditorTab.value === 'delivery') return t('campaign.editorDelivery')
  return t('campaign.basicInfo')
})

function goNext() {
  if (activeEditorTab.value === 'basics') activeEditorTab.value = 'source'
  else if (activeEditorTab.value === 'source') activeEditorTab.value = 'delivery'
}

function goPrevious() {
  if (activeEditorTab.value === 'delivery') activeEditorTab.value = 'source'
  else if (activeEditorTab.value === 'source') activeEditorTab.value = 'basics'
}

function readinessItems(row: Campaign) {
  const scheduleReady = Boolean(row.start_time && row.end_time && row.send_start_time && row.send_end_time)
  const watchFilenamePrefix = row.watch_filename_prefix || 'Android_4G_Switch_Somali_'
  return [
    { ok: row.use_whitelist === false || row.row_count > 0, label: row.use_whitelist === false ? i18n.global.t('legacy.xc9b0bc31') : t(row.row_count > 0 ? 'campaign.whitelistReady' : 'campaign.whitelistMissing') },
    { ok: Boolean(row.sms_template_id), label: t(row.sms_template_id ? 'campaign.templateReady' : 'campaign.templateMissing') },
    { ok: Boolean(row.smpp_config_id), label: t(row.smpp_config_id ? 'campaign.smppReady' : 'campaign.smppMissing') },
    { ok: scheduleReady, label: t(scheduleReady ? 'campaign.scheduleReady' : 'campaign.scheduleMissing') },
    { ok: Boolean(row.watch_dir && (row.watch_source !== 'sftp' || (row.sftp_host && row.sftp_username && row.sftp_password))), label: t(row.watch_dir && (row.watch_source !== 'sftp' || (row.sftp_host && row.sftp_username && row.sftp_password)) ? 'campaign.watchDirReady' : 'campaign.watchDirMissing') },
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

async function testSftpConnection() {
  if (!editForm.value.sftp_host || !editForm.value.sftp_username
      || (!editForm.value.sftp_password && !hasStoredSftpPassword.value) || !editForm.value.watch_dir) {
    ElMessage.warning(t('campaign.sftpFieldsRequired'))
    return
  }
  testingSftp.value = true
  try {
    const result = await api.testRealtimeSftpConnection({
      campaign_id: editForm.value.id || undefined,
      watch_dir: editForm.value.watch_dir,
      sftp_host: editForm.value.sftp_host,
      sftp_port: editForm.value.sftp_port,
      sftp_username: editForm.value.sftp_username,
      sftp_password: editForm.value.sftp_password,
    })
    ElMessage.success(t('campaign.sftpConnectionSuccess', { count: result.data.entry_count }))
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || t('campaign.sftpConnectionFailed'))
  } finally {
    testingSftp.value = false
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
  if (!inProgressEdit.value && !editForm.value.watch_dir.trim()) {
    ElMessage.warning(t('campaign.watchDirMissing'))
    return
  }
  if (!inProgressEdit.value && editForm.value.watch_source === 'sftp'
      && (!editForm.value.sftp_host.trim() || !editForm.value.sftp_username.trim()
        || (!editForm.value.sftp_password && !hasStoredSftpPassword.value))) {
    ElMessage.warning(t('campaign.sftpFieldsRequired'))
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
      watch_source: editForm.value.watch_source,
      sftp_host: editForm.value.sftp_host,
      sftp_port: editForm.value.sftp_port,
      sftp_username: editForm.value.sftp_username,
      sftp_password: editForm.value.sftp_password,
     watch_filename_prefix: editForm.value.watch_filename_prefix,
     phone_prefix: editForm.value.phone_prefix,
     smpp_config_id: editForm.value.smpp_config_id || undefined,
     flash_sms: editForm.value.flash_sms,
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
       watch_dir: editForm.value.watch_dir,
       watch_source: editForm.value.watch_source,
       sftp_host: editForm.value.sftp_host,
       sftp_port: editForm.value.sftp_port,
       sftp_username: editForm.value.sftp_username,
       sftp_password: editForm.value.sftp_password,
       watch_filename_prefix: editForm.value.watch_filename_prefix,
       phone_prefix: editForm.value.phone_prefix,
       smpp_config_id: editForm.value.smpp_config_id || undefined,
       flash_sms: editForm.value.flash_sms,
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
.realtime-editor-dialog :deep(.el-dialog) {
  max-width: calc(100vw - 32px);
  margin-top: 5vh;
  margin-bottom: 5vh;
}
.realtime-editor-dialog :deep(.el-dialog__body) {
  padding: 0 24px 4px;
}
.realtime-editor-dialog :deep(.el-dialog__footer) {
  padding: 14px 24px 18px;
  border-top: 1px solid var(--mc-border);
}
.editor-intro,
.editor-section-intro,
.sftp-panel__header,
.editor-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
}
.editor-intro {
  padding: 2px 0 14px;
  color: var(--mc-text-secondary);
}
.editor-intro > div,
.editor-section-intro > div,
.sftp-panel__header > div {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.editor-intro strong,
.editor-section-intro strong,
.sftp-panel__header strong {
  color: var(--mc-text);
  font-size: 15px;
}
.editor-intro span,
.editor-section-intro span,
.sftp-panel__header span {
  color: var(--mc-text-tertiary);
  font-size: 12px;
  line-height: 1.5;
}
.campaign-editor-tabs :deep(.el-tabs__header) {
  margin-bottom: 0;
}
.campaign-editor-tabs :deep(.el-tabs__item) {
  height: 42px;
  font-weight: 600;
}
.campaign-editor-tabs :deep(.el-tabs__content) {
  padding-top: 18px;
}
.editor-section-intro {
  align-items: flex-start;
  margin-bottom: 16px;
}
.campaign-editor-form :deep(.el-form-item) {
  margin-bottom: 16px;
}
.form-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0 18px;
}
.form-grid__full {
  grid-column: 1 / -1;
}
.field-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 54px;
  padding: 10px 12px;
  margin-bottom: 16px;
  border: 1px solid var(--mc-border);
  border-radius: 8px;
  background: var(--mc-surface-soft, #f8fafc);
}
.field-card__label {
  color: var(--mc-text-secondary);
  font-size: 13px;
  font-weight: 600;
}
.field-card__control {
  display: flex;
  align-items: center;
  gap: 8px;
}
.source-switch {
  display: flex;
  width: 100%;
}
.source-switch :deep(.el-radio-button) {
  flex: 1;
}
.source-switch :deep(.el-radio-button__inner) {
  width: 100%;
}
.sftp-panel {
  padding: 14px 16px 2px;
  margin-bottom: 16px;
  border: 1px solid color-mix(in srgb, var(--el-color-warning) 32%, var(--mc-border));
  border-radius: 10px;
  background: color-mix(in srgb, var(--el-color-warning-light-9) 58%, var(--mc-surface, #fff));
}
.sftp-panel__header {
  align-items: flex-start;
  margin-bottom: 14px;
}
.sftp-panel__header .el-button {
  flex: none;
}
.editor-alert {
  margin-bottom: 16px;
}
.send-days {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 18px;
}
.editor-footer__status {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--mc-text-tertiary);
  font-size: 12px;
}
.editor-footer__actions {
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
@media (max-width: 700px) {
  .realtime-editor-dialog :deep(.el-dialog__body),
  .realtime-editor-dialog :deep(.el-dialog__footer) {
    padding-left: 14px;
    padding-right: 14px;
  }
  .form-grid {
    grid-template-columns: 1fr;
    gap: 0;
  }
  .form-grid__full {
    grid-column: auto;
  }
  .editor-intro,
  .sftp-panel__header,
  .editor-footer {
    align-items: flex-start;
    flex-direction: column;
  }
  .editor-footer__actions {
    width: 100%;
    flex-wrap: wrap;
  }
  .editor-footer__actions .el-button:last-child {
    margin-left: auto;
  }
  .sftp-panel__header .el-button {
    width: 100%;
  }
}
</style>
