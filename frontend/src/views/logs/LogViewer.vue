<template>
  <div class="log-viewer">
    <div v-if="!embedded" class="page-header">
      <div>
        <h2>{{ $t('logViewer.title') }}</h2>
        <p>
          {{ $t('logViewer.description') }}
          <el-tag v-if="currentLevel" size="small" class="level-tag">{{ currentLevel }}</el-tag>
        </p>
      </div>
      <div class="header-actions">
        <span class="auto-refresh">
          {{ $t('logViewer.autoRefresh') }}
          <el-switch v-model="autoRefresh" size="small" />
        </span>
        <el-button @click="refresh">
          <el-icon><Refresh /></el-icon>
          {{ $t('common.refresh') }}
        </el-button>
        <el-button type="primary" @click="download">
          <el-icon><Download /></el-icon>
          {{ $t('logViewer.download') }}
        </el-button>
      </div>
    </div>

    <el-card shadow="never" class="toolbar-card">
      <el-form :inline="true" size="small">
        <el-form-item :label="$t('logViewer.file')">
          <el-select v-model="filters.file" style="width: 180px" @change="changeFilter">
            <el-option v-for="f in files" :key="f.name" :label="f.name" :value="f.name" />
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('logViewer.level')">
          <el-select v-model="filters.level" clearable style="width: 130px" @change="changeFilter">
            <el-option :label="$t('logViewer.levelAll')" value="" />
            <el-option v-for="lv in levels" :key="lv" :label="lv" :value="lv" />
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('logViewer.keyword')">
          <el-input
            v-model="filters.keyword"
            :placeholder="$t('logViewer.keywordPlaceholder')"
            clearable
            style="width: 220px"
            @keyup.enter="changeFilter"
          />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="changeFilter">
            <el-icon><Search /></el-icon>
            {{ $t('common.search') }}
          </el-button>
        </el-form-item>
        <span v-if="truncated" class="truncate-tip">{{ $t('logViewer.truncatedTip') }}</span>
      </el-form>
    </el-card>

    <el-card shadow="never">
      <el-table v-loading="loading" :data="rows" stripe size="small">
        <el-table-column type="expand">
          <template #default="{ row }">
            <pre class="message-detail">{{ row.message }}</pre>
          </template>
        </el-table-column>
        <el-table-column prop="time" :label="$t('logViewer.time')" width="170" />
        <el-table-column :label="$t('logViewer.level')" width="100">
          <template #default="{ row }">
            <el-tag :type="levelType(row.level)" size="small">{{ row.level }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="logger" :label="$t('logViewer.logger')" min-width="180" show-overflow-tooltip />
        <el-table-column prop="message" :label="$t('logViewer.message')" min-width="280" show-overflow-tooltip />
        <el-table-column prop="line" :label="$t('logViewer.line')" width="70" />
      </el-table>

      <div style="display: flex; justify-content: center; margin-top: 16px">
        <el-pagination
          v-model:current-page="page"
          v-model:page-size="pageSize"
          :page-sizes="[50, 100, 200, 500]"
          :total="total"
          layout="total, sizes, prev, pager, next"
          @size-change="handleSizeChange"
          @current-change="fetchData"
        />
      </div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, watch } from 'vue'
import { getLogFiles, getLogEntries, getLogLevel, getLogDownloadUrl, type LogEntry } from '@/api/logs'

withDefaults(defineProps<{ embedded?: boolean }>(), { embedded: false })

const levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

const files = ref<{ name: string; path: string; size: number; modified: number }[]>([])
const rows = ref<LogEntry[]>([])
const loading = ref(false)
const total = ref(0)
const page = ref(1)
const pageSize = ref(100)
const truncated = ref(false)
const currentLevel = ref('')
const autoRefresh = ref(false)

const filters = ref({ file: '', level: '', keyword: '' })

let timer: number | undefined

onMounted(async () => {
  try {
    const [filesRes, levelRes] = await Promise.all([getLogFiles(), getLogLevel()])
    files.value = filesRes.data.items
    currentLevel.value = levelRes.data.level
    filters.value.file = filesRes.data.items[0]?.name || ''
  } catch {
    // File list and level are optional; entries will surface the error state.
  }
  await fetchData()
})

onBeforeUnmount(() => {
  if (timer) {
    window.clearInterval(timer)
  }
})

watch(autoRefresh, enabled => {
  if (timer) {
    window.clearInterval(timer)
    timer = undefined
  }
  if (enabled) {
    timer = window.setInterval(fetchData, 10000)
  }
})

function levelType(level: string): 'primary' | 'success' | 'warning' | 'danger' | 'info' {
  if (level === 'ERROR' || level === 'CRITICAL') return 'danger'
  if (level === 'WARNING') return 'warning'
  if (level === 'DEBUG') return 'info'
  return 'primary'
}

async function fetchData() {
  if (!filters.value.file) return
  loading.value = true
  try {
    const res = await getLogEntries({
      file: filters.value.file,
      level: filters.value.level || undefined,
      keyword: filters.value.keyword || undefined,
      page: page.value,
      page_size: pageSize.value,
    })
    rows.value = res.data.items
    total.value = res.data.total
    truncated.value = res.data.truncated
  } catch {
    rows.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

function changeFilter() {
  page.value = 1
  fetchData()
}

function handleSizeChange() {
  page.value = 1
  fetchData()
}

async function refresh() {
  await Promise.all([getLogFiles().then(res => {
    files.value = res.data.items
    if (!files.value.some(f => f.name === filters.value.file)) {
      filters.value.file = files.value[0]?.name || ''
    }
  }), getLogLevel().then(res => {
    currentLevel.value = res.data.level
  })])
  await fetchData()
}

function download() {
  if (filters.value.file) {
    window.open(getLogDownloadUrl(filters.value.file), '_blank')
  }
}
</script>

<style scoped>
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
  flex-wrap: wrap;
  gap: 10px;
}
.page-header h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 600;
}
.page-header p {
  margin: 6px 0 0;
  color: var(--mc-text-secondary);
  font-size: 13px;
}
.level-tag {
  margin-left: 6px;
}
.header-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}
.auto-refresh {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--mc-text-secondary);
}
.toolbar-card {
  margin-bottom: 16px;
}
.toolbar-card :deep(.el-form-item) {
  margin-bottom: 0;
}
.truncate-tip {
  font-size: 12px;
  color: var(--el-color-warning);
}
.message-detail {
  margin: 4px 8px;
  padding: 10px;
  border-radius: 8px;
  background: var(--mc-bg-muted, #f5f7f8);
  font-size: 12px;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
