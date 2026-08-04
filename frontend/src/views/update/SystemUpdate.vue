<template>
  <div class="update-page">
    <PageHeader v-if="!embedded" :title="$t('update.title')" :description="$t('update.description')" />

    <el-row :gutter="18">
      <el-col :xs="24" :lg="10">
        <el-card class="status-card" shadow="never">
          <template #header>
            <div class="card-title">
              <el-icon><Monitor /></el-icon>
              <span>{{ $t('update.currentStatus') }}</span>
            </div>
          </template>
          <el-descriptions :column="1" size="small">
            <el-descriptions-item :label="$t('update.currentVersion')">
              <b>{{ status?.version || '-' }}</b>
            </el-descriptions-item>
            <el-descriptions-item :label="$t('update.runMode')">
              <el-tag size="small" :type="status?.mode === 'systemd' ? 'primary' : 'warning'">
                {{ status?.mode === 'systemd' ? $t('update.modeSystemd') : $t('update.modeManual') }}
              </el-tag>
            </el-descriptions-item>
            <el-descriptions-item :label="$t('update.updateState')">
              <el-tag size="small" :type="stateType">{{ $t(stateLabel) }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item :label="$t('update.lastResult')">
              <span v-if="lastResult">
                <el-tag size="small" :type="lastResult.ok ? 'success' : 'danger'">
                  {{ lastResult.ok ? $t('update.resultSuccess') : $t('update.resultFailed') }}
                </el-tag>
                <span class="result-message">{{ lastResult.message }}</span>
              </span>
              <span v-else>-</span>
            </el-descriptions-item>
            <el-descriptions-item :label="$t('update.backupAvailable')">
              <el-tag size="small" :type="status?.has_backups ? 'success' : 'info'">
                {{ status?.has_backups ? $t('common.yes') : $t('common.no') }}
              </el-tag>
            </el-descriptions-item>
          </el-descriptions>

          <el-alert
            v-if="hasActiveBusiness"
            class="business-alert"
            type="warning"
            :closable="false"
            show-icon
            :title="$t('update.activeBusinessWarning')"
          >
            <template #default>
              {{ $t('update.activeBusinessDetail', {
                batch: status?.active_business?.batch_in_progress || 0,
                realtime: status?.active_business?.realtime_in_progress || 0,
              }) }}
            </template>
          </el-alert>
        </el-card>

        <el-card class="action-card" shadow="never">
          <template #header>
            <div class="card-title">
              <el-icon><RefreshRight /></el-icon>
              <span>{{ $t('update.applyCard') }}</span>
            </div>
          </template>
          <el-checkbox v-model="forceApply" class="force-checkbox">
            {{ $t('update.forceApply') }}
          </el-checkbox>
          <p class="force-tip">{{ $t('update.forceApplyTip') }}</p>
          <div class="action-buttons">
            <el-button
              type="primary"
              :disabled="!verifiedPackage || isRunning"
              :loading="applying"
              @click="confirmApply"
            >
              {{ $t('update.apply') }}
            </el-button>
            <el-button
              type="danger"
              plain
              :disabled="!status?.has_backups || isRunning"
              :loading="rollingBack"
              @click="confirmRollback"
            >
              {{ $t('update.rollback') }}
            </el-button>
          </div>
        </el-card>
      </el-col>

      <el-col :xs="24" :lg="14">
        <el-card class="upload-card" shadow="never">
          <template #header>
            <div class="card-title">
              <el-icon><UploadFilled /></el-icon>
              <span>{{ $t('update.uploadCard') }}</span>
            </div>
          </template>

          <el-upload
            drag
            :auto-upload="false"
            :limit="1"
            :disabled="uploading || isRunning"
            accept=".tar.gz,.tgz,.tar"
            :on-change="onFileChange"
            :on-remove="onFileRemove"
            :file-list="fileList"
          >
            <el-icon class="el-icon--upload"><UploadFilled /></el-icon>
            <div class="el-upload__text">{{ $t('update.dropHint') }}</div>
            <template #tip>
              <div class="el-upload__tip">{{ $t('update.uploadTip') }}</div>
            </template>
          </el-upload>

          <el-progress
            v-if="uploading"
            class="upload-progress"
            :percentage="uploadProgress"
            :status="uploadProgress === 100 ? 'success' : undefined"
          />

          <div class="upload-actions">
            <el-button
              type="primary"
              :disabled="!selectedFile || uploading || isRunning"
              :loading="uploading"
              @click="uploadAndVerify"
            >
              {{ uploading ? $t('update.uploading') : $t('update.uploadAndVerify') }}
            </el-button>
            <el-button v-if="uploadId && !verifiedPackage" :disabled="isRunning" @click="cancelCurrent">
              {{ $t('common.cancel') }}
            </el-button>
          </div>

          <template v-if="precheck">
            <el-alert
              class="precheck-alert"
              :type="precheck.ok ? 'success' : 'error'"
              :closable="false"
              show-icon
              :title="precheck.ok ? $t('update.precheckPassed') : $t('update.precheckFailed')"
            />
            <el-descriptions v-if="precheck.checks?.length" :column="1" size="small" border class="precheck-list">
              <el-descriptions-item v-for="item in precheck.checks" :key="item.name" :label="item.name">
                <el-tag size="small" :type="item.ok ? 'success' : 'danger'">
                  {{ item.ok ? $t('common.success') : $t('common.fail') }}
                </el-tag>
                <span class="check-detail">{{ item.detail }}</span>
              </el-descriptions-item>
            </el-descriptions>
            <ul v-if="precheck.errors?.length" class="error-list">
              <li v-for="err in precheck.errors" :key="err">{{ err }}</li>
            </ul>
          </template>
        </el-card>
      </el-col>
    </el-row>

    <el-card class="history-card" shadow="never">
      <template #header>
        <div class="card-title">
          <el-icon><History /></el-icon>
          <span>{{ $t('update.historyTitle') }}</span>
        </div>
      </template>
      <el-table :data="history" size="small" stripe>
        <el-table-column prop="created_at" :label="$t('update.historyTime')" width="170" />
        <el-table-column :label="$t('update.historyAction')" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="row.action === 'apply' ? 'primary' : 'warning'">
              {{ row.action === 'apply' ? $t('update.apply') : $t('update.rollback') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="version" :label="$t('update.historyVersion')" width="150" />
        <el-table-column prop="previous_version" :label="$t('update.historyPrevious')" width="150" />
        <el-table-column :label="$t('common.status')" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === 'success' ? 'success' : 'danger'">
              {{ row.status === 'success' ? $t('update.resultSuccess') : $t('update.resultFailed') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="operator" :label="$t('update.historyOperator')" width="120" />
        <el-table-column :label="$t('update.historyDetail')">
          <template #default="{ row }">
            <span class="detail-text">{{ detailText(row.detail) }}</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage, ElMessageBox, type UploadFile } from 'element-plus'
import PageHeader from '@/components/PageHeader.vue'
import {
  applyUpdate,
  cancelUpload,
  getUpdateHistory,
  getUpdateStatus,
  rollbackUpdate,
  startUpload,
  uploadChunk,
  verifyUpload,
  type UpdateHistoryRow,
  type UpdateStatus,
} from '@/api/update'

withDefaults(defineProps<{ embedded?: boolean }>(), { embedded: false })

const CHUNK_SIZE = 8 * 1024 * 1024

const status = ref<UpdateStatus | null>(null)
const history = ref<UpdateHistoryRow[]>([])
const selectedFile = ref<File | null>(null)
const fileList = ref<UploadFile[]>([])
const uploadId = ref('')
const uploading = ref(false)
const uploadProgress = ref(0)
const precheck = ref<any>(null)
const forceApply = ref(false)
const applying = ref(false)
const rollingBack = ref(false)
let pollTimer: number | null = null

const isRunning = computed(() => {
  const st = status.value?.state?.status
  return !!st && ['prepared', 'stopping', 'swapping', 'starting', 'health_checking'].includes(st)
})

const stateLabel = computed(() => {
  const st = status.value?.state?.status
  if (!st) return '-'
  const map: Record<string, string> = {
    prepared: 'update.statePrepared',
    stopping: 'update.stateStopping',
    swapping: 'update.stateSwapping',
    starting: 'update.stateStarting',
    health_checking: 'update.stateChecking',
    success: 'update.stateSuccess',
    failed: 'update.stateFailed',
  }
  return (map[st] || st) as string
})

const stateType = computed(() => {
  const st = status.value?.state?.status
  if (st === 'success') return 'success' as const
  if (st === 'failed') return 'danger' as const
  if (isRunning.value) return 'warning' as const
  return 'info' as const
})

const lastResult = computed(() => status.value?.state?.result)

const hasActiveBusiness = computed(() => {
  const b = status.value?.active_business
  return !!b && (b.batch_in_progress > 0 || b.realtime_in_progress > 0)
})

const verifiedPackage = computed(() => precheck.value?.ok && !!uploadId.value)

function detailText(detail: string) {
  try {
    const parsed = JSON.parse(detail)
    return parsed?.message || parsed?.db_backup || detail
  } catch {
    return detail
  }
}

async function refresh() {
  try {
    const [s, h] = await Promise.all([getUpdateStatus(), getUpdateHistory()])
    status.value = s.data
    history.value = h.data
  } catch {
    // The backend may be restarting during an update; keep polling silently.
  }
}

function startPolling() {
  stopPolling()
  pollTimer = window.setInterval(refresh, 4000)
}

function stopPolling() {
  if (pollTimer !== null) {
    window.clearInterval(pollTimer)
    pollTimer = null
  }
}

function onFileChange(file: UploadFile) {
  const raw = file.raw
  if (!raw) return
  selectedFile.value = raw
  uploadId.value = ''
  precheck.value = null
  uploadProgress.value = 0
}

function onFileRemove() {
  selectedFile.value = null
  uploadId.value = ''
  precheck.value = null
}

async function sha256File(file: File): Promise<string> {
  try {
    if (!window.crypto?.subtle) return ''
    const buffer = await file.arrayBuffer()
    const digest = await window.crypto.subtle.digest('SHA-256', buffer)
    return Array.from(new Uint8Array(digest))
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('')
  } catch {
    return ''
  }
}

async function uploadAndVerify() {
  const file = selectedFile.value
  if (!file) return
  uploading.value = true
  uploadProgress.value = 0
  try {
    const chunks = Math.max(1, Math.ceil(file.size / CHUNK_SIZE))
    const sha256 = await sha256File(file)
    const started = await startUpload({
      filename: file.name,
      size: file.size,
      sha256,
      chunks,
    })
    uploadId.value = started.data.upload_id
    for (let i = 0; i < chunks; i += 1) {
      const blob = file.slice(i * CHUNK_SIZE, Math.min((i + 1) * CHUNK_SIZE, file.size))
      await uploadChunk(uploadId.value, i, blob)
      uploadProgress.value = Math.round(((i + 1) / chunks) * 100)
    }
    ElMessage.success('上传完成，开始校验')
    const verified = await verifyUpload(uploadId.value)
    precheck.value = verified.data
    if (verified.data.ok) {
      ElMessage.success('校验通过')
    } else {
      ElMessage.error('校验未通过，请检查错误列表')
    }
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || '上传失败')
  } finally {
    uploading.value = false
  }
}

async function cancelCurrent() {
  if (!uploadId.value) return
  try {
    await cancelUpload(uploadId.value)
    uploadId.value = ''
    precheck.value = null
    fileList.value = []
    selectedFile.value = null
    ElMessage.success('已取消')
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || '取消失败')
  }
}

async function confirmApply() {
  if (!uploadId.value) return
  try {
    await ElMessageBox.confirm('确认应用该更新包？更新期间服务会短暂重启。', '应用更新', {
      confirmButtonText: '应用',
      cancelButtonText: '取消',
      type: 'warning',
    })
  } catch {
    return
  }
  applying.value = true
  try {
    await applyUpdate(uploadId.value, forceApply.value)
    ElMessage.success('已开始应用更新，系统将在重启后自动恢复')
    await refresh()
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || '应用失败')
  } finally {
    applying.value = false
  }
}

async function confirmRollback() {
  try {
    await ElMessageBox.confirm('确认回滚到上一次更新前的版本？', '回滚', {
      confirmButtonText: '回滚',
      cancelButtonText: '取消',
      type: 'warning',
    })
  } catch {
    return
  }
  rollingBack.value = true
  try {
    await rollbackUpdate(forceApply.value)
    ElMessage.success('已开始回滚')
    await refresh()
  } catch (error: any) {
    ElMessage.error(error.response?.data?.detail || error.message || '回滚失败')
  } finally {
    rollingBack.value = false
  }
}

onMounted(() => {
  refresh()
  startPolling()
})

onBeforeUnmount(stopPolling)
</script>

<style scoped>
.update-page {
  display: flex;
  flex-direction: column;
  gap: 18px;
}
.card-title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 650;
  color: var(--mc-text);
}
.result-message {
  margin-left: 8px;
  color: var(--mc-text-secondary);
  font-size: 12px;
}
.business-alert,
.precheck-alert {
  margin-top: 16px;
}
.force-checkbox {
  margin-bottom: 4px;
}
.force-tip {
  margin: 0 0 14px;
  color: var(--mc-text-secondary);
  font-size: 12px;
}
.action-buttons {
  display: flex;
  gap: 10px;
}
.upload-progress {
  margin-top: 16px;
}
.upload-actions {
  margin-top: 14px;
  display: flex;
  gap: 10px;
}
.precheck-list {
  margin-top: 14px;
}
.check-detail {
  margin-left: 8px;
  color: var(--mc-text-secondary);
  font-size: 12px;
}
.error-list {
  margin: 12px 0 0;
  padding-left: 18px;
  color: var(--el-color-danger);
  font-size: 12px;
}
.detail-text {
  color: var(--mc-text-secondary);
  font-size: 12px;
}
</style>
