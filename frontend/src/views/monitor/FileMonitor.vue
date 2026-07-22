<template>
  <div class="file-monitor">
    <div class="page-header">
      <h2>{{ $t('monitor.title') }}</h2>
    </div>

    <el-card shadow="never" style="margin-bottom: 20px">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>{{ $t('monitor.configs') }}</span>
          <el-button size="small" type="primary" @click="showEditDialog(null)">
            <el-icon><Plus /></el-icon>{{ $t('monitor.startMonitor') }}
          </el-button>
        </div>
      </template>
      <el-table :data="store.configs" stripe style="width: 100%">
        <el-table-column prop="directory_path" :label="$t('monitor.directoryPath')" min-width="250" />
        <el-table-column :label="$t('monitor.targetDirectories')" min-width="250">
          <template #default="{ row }">
            <div v-if="row.target_directories?.length">
              <el-tag v-for="d in row.target_directories" :key="d" size="small" style="margin: 2px">{{ d }}</el-tag>
            </div>
            <span v-else style="color: #999">-</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('common.status')" width="100">
          <template #default="{ row }">
            <StatusTag :status="row.enabled ? 'enabled' : 'disabled'" />
          </template>
        </el-table-column>
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
        <el-table-column :label="$t('common.action')" width="200" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="showEditDialog(row)">
              {{ $t('common.edit') }}
            </el-button>
            <el-button
              text
              :type="row.enabled ? 'warning' : 'success'"
              size="small"
              @click="store.toggleConfig(row.id, row.enabled ? 0 : 1)"
            >
              {{ row.enabled ? $t('monitor.stopMonitor') : $t('monitor.startMonitor') }}
            </el-button>
            <el-button text type="danger" size="small" @click="handleDelete(row.id)">
              {{ $t('common.delete') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card shadow="never" style="margin-bottom: 20px">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>{{ $t('monitor.files') }} <el-tag type="info" size="small">{{ filteredFiles.length }}</el-tag></span>
          <div>
            <el-select
              v-model="filterDir"
              clearable
              :placeholder="$t('monitor.filterDirectory')"
              size="small"
              style="width: 200px; margin-right: 8px"
            >
              <el-option
                v-for="d in dirOptions"
                :key="d"
                :label="d"
                :value="d"
              />
            </el-select>
            <el-select
              v-model="filterStatus"
              clearable
              :placeholder="$t('common.status')"
              size="small"
              style="width: 120px; margin-right: 8px"
            >
              <el-option :label="$t('monitor.status_pending')" value="pending" />
              <el-option :label="$t('monitor.status_processing')" value="processing" />
              <el-option :label="$t('monitor.status_completed')" value="completed" />
              <el-option :label="$t('monitor.status_failed')" value="failed" />
              <el-option :label="$t('monitor.status_distributed')" value="distributed" />
            </el-select>
            <el-button size="small" @click="handleSyncSource" style="margin-right: 8px">
              <el-icon><Refresh /></el-icon>
              {{ $t('monitor.syncSource') }}
            </el-button>
            <el-switch
              v-model="autoRefresh"
              :active-text="$t('monitor.autoRefresh')"
              style="margin-right: 12px"
              @change="handleAutoRefresh"
            />
            <el-button size="small" @click="refreshAll">
              <el-icon><Download /></el-icon>
              {{ $t('monitor.scanNow') }}
            </el-button>
          </div>
        </div>
      </template>
      <el-table :data="filteredFiles" stripe v-loading="store.loading" style="width: 100%">
        <el-table-column prop="filename" :label="$t('monitor.filename')" min-width="200" />
        <el-table-column prop="directory_path" :label="$t('monitor.directoryPath')" min-width="200" />
        <el-table-column :label="$t('monitor.fileSize')" width="100">
          <template #default="{ row }">
            {{ formatFileSize(row.file_size) }}
          </template>
        </el-table-column>
        <el-table-column prop="row_count" :label="$t('monitor.rowCount')" width="80" />
        <el-table-column :label="$t('common.status')" width="120">
          <template #default="{ row }">
            <StatusTag :status="row.status" />
          </template>
        </el-table-column>
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
        <el-table-column :label="$t('common.action')" width="300" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="markStatus(row.id, 'processing')">
              {{ $t('monitor.status_processing') }}
            </el-button>
            <el-button text type="success" size="small" @click="markStatus(row.id, 'completed')">
              {{ $t('monitor.status_completed') }}
            </el-button>
            <el-button text type="warning" size="small" :loading="publishing[row.id]" @click="handlePublish(row)">
              {{ $t('monitor.publishToMQ') }}
            </el-button>
            <el-button text type="info" size="small" :loading="distributing[row.id]" @click="handleDistribute(row)">
              {{ $t('monitor.distribute') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog
      :model-value="editDialogVisible"
      :title="editForm.id ? $t('common.edit') : $t('common.add') + ' ' + $t('monitor.configs')"
      width="650px"
      @close="editDialogVisible = false"
    >
      <el-form :model="editForm" label-width="140px" size="small">
        <el-form-item :label="$t('monitor.directoryPath')" required>
          <el-input v-model="editForm.directory_path" :placeholder="$t('monitor.directoryPlaceholder')" />
        </el-form-item>
        <el-form-item :label="$t('monitor.targetDirectories')">
          <div style="width: 100%">
            <div v-for="(d, i) in editForm.target_directories" :key="i" style="display: flex; margin-bottom: 6px; gap: 6px">
              <el-input v-model="editForm.target_directories[i]" :placeholder="$t('monitor.targetPlaceholder')" style="flex: 1" />
              <el-button size="small" type="danger" @click="removeTarget(i)">
                <el-icon><Delete /></el-icon>
              </el-button>
            </div>
            <el-button size="small" type="primary" plain @click="addTarget">
              <el-icon><Plus /></el-icon> {{ $t('monitor.addTarget') }}
            </el-button>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="editDialogVisible = false">{{ $t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="editSaving" @click="handleEditSave">{{ $t('common.save') }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useMonitorStore } from '@/stores/monitor'
import { formatFileSize } from '@/utils/format'
import { publishFile, distributeFile, syncSourceFiles } from '@/api/monitor'
import StatusTag from '@/components/StatusTag.vue'

const store = useMonitorStore()
const autoRefresh = ref(false)
const publishing = ref<Record<number, boolean>>({})
const distributing = ref<Record<number, boolean>>({})
const editDialogVisible = ref(false)
const editSaving = ref(false)
const editForm = ref({ id: 0, directory_path: '', target_directories: [''] })
const filterDir = ref('')
const filterStatus = ref('')
let refreshTimer: ReturnType<typeof setInterval> | null = null

const dirOptions = computed(() => {
  const dirs = new Set(store.files.map(f => f.directory_path))
  return [...dirs].sort()
})

const filteredFiles = computed(() => {
  return store.files.filter(f => {
    if (filterDir.value && f.directory_path !== filterDir.value) return false
    if (filterStatus.value && f.status !== filterStatus.value) return false
    return true
  })
})

onMounted(() => {
  store.fetchConfigs()
  store.fetchFiles()
})

onUnmounted(() => {
  stopAutoRefresh()
})

function showEditDialog(row: any | null) {
  if (row) {
    editForm.value = {
      id: row.id,
      directory_path: row.directory_path,
      target_directories: row.target_directories?.length ? [...row.target_directories] : [''],
    }
  } else {
    editForm.value = { id: 0, directory_path: '', target_directories: [''] }
  }
  editDialogVisible.value = true
}

function addTarget() {
  editForm.value.target_directories.push('')
}

function removeTarget(i: number) {
  editForm.value.target_directories.splice(i, 1)
}

async function handleEditSave() {
  if (!editForm.value.directory_path) {
    ElMessage.warning('请输入监控目录路径')
    return
  }
  const targets = editForm.value.target_directories.filter(Boolean)
  editSaving.value = true
  try {
    if (editForm.value.id) {
      await store.updateConfig(editForm.value.id, {
        directory_path: editForm.value.directory_path,
        target_directories: targets,
      })
      ElMessage.success('更新成功')
    } else {
      await store.addConfig(editForm.value.directory_path, targets)
      ElMessage.success('添加成功')
    }
    editDialogVisible.value = false
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '操作失败')
  } finally {
    editSaving.value = false
  }
}

async function handleDelete(id: number) {
  try {
    await ElMessageBox.confirm('确认删除该监控配置？', '提示')
    await store.removeConfig(id)
    ElMessage.success('删除成功')
  } catch {}
}

function handleAutoRefresh(val: boolean) {
  if (val) {
    refreshTimer = setInterval(() => store.fetchFiles(), 5000)
  } else {
    stopAutoRefresh()
  }
}

function stopAutoRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

async function handleSyncSource() {
  try {
    const res = await syncSourceFiles()
    ElMessage.success(`同步完成，${res.data.updated} 个文件已标记为已分发`)
    await store.fetchFiles()
  } catch {
    ElMessage.error('同步失败')
  }
}

async function refreshAll() {
  let totalMoved = 0
  for (const cfg of store.configs) {
    if (cfg.enabled) {
      const res = await store.scan(cfg.id)
      totalMoved += res.moved_count || 0
    }
  }
  ElMessage.success(`扫描完成，已自动分发 ${totalMoved} 个文件`)
}

async function markStatus(fileId: number, status: string) {
  try {
    const { updateFileStatus } = await import('@/api/monitor')
    await updateFileStatus(fileId, status)
    await store.fetchFiles()
  } catch (e) {
    ElMessage.error('更新失败')
  }
}

async function handlePublish(row: any) {
  const campaignName = row.filename.replace(/\.(csv|xlsx|xls)$/i, '')
  publishing.value[row.id] = true
  try {
    const res = await publishFile(row.id, campaignName)
    if (res.data.success) {
      ElMessage.success(`发布完成: ${res.data.published}/${res.data.total_rows} 条到 ${res.data.routing_key}`)
    } else {
      ElMessage.error('发布失败')
    }
    await store.fetchFiles()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '发布失败')
  } finally {
    publishing.value[row.id] = false
  }
}

async function handleDistribute(row: any) {
  distributing.value[row.id] = true
  try {
    const res = await distributeFile(row.id)
    if (res.data.success) {
      ElMessage.success(res.data.message)
    } else {
      ElMessage.error(res.data.message)
    }
    await store.fetchFiles()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '分发失败')
  } finally {
    distributing.value[row.id] = false
  }
}
</script>

<style scoped>
.page-header h2 {
  margin: 0 0 16px 0;
  font-size: 22px;
  font-weight: 600;
}
</style>
