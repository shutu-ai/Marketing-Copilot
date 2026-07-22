<template>
  <div class="whitelist-upload">
    <div class="page-header">
      <h2>{{ $t('whitelist.title') }}</h2>
    </div>

    <el-card shadow="never" style="margin-bottom: 20px">
      <template #header>
        <div style="display:flex;align-items:center;justify-content:space-between">
          <span>{{ $t('whitelist.uploadFile') }}</span>
          <el-button size="small" @click="downloadTemplate">
            <el-icon><Download /></el-icon>
            {{ $t('whitelist.downloadTemplate') }}
          </el-button>
        </div>
      </template>
      <FileUploader @file="handleFile" />
    </el-card>

    <el-card v-if="previewData" shadow="never" style="margin-bottom: 20px">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>{{ $t('whitelist.dataPreview') }} ({{ $t('whitelist.totalRecords') }}: {{ previewData.total }})</span>
          <div>
            <el-tag v-if="previewData.missing_fields?.length" type="warning" style="margin-right:8px">
              缺少字段: {{ previewData.missing_fields.join(', ') }}
            </el-tag>
            <el-tag v-if="previewData.format_errors?.length" type="danger">
              数据格式错误: {{ previewData.format_errors.length }} 行
            </el-tag>
          </div>
        </div>
        <el-alert
          v-if="previewData.format_errors?.length"
          type="error"
          :closable="false"
          style="margin-top:8px"
        >
          <template #title>
            <div v-for="e in previewData.format_errors.slice(0, 5)" :key="e.row">
              行{{ e.row }}: {{ e.error }}
            </div>
            <div v-if="previewData.format_errors.length > 5">... 还有 {{ previewData.format_errors.length - 5 }} 个错误</div>
          </template>
        </el-alert>
      </template>
      <el-table :data="previewData.rows" stripe border size="small" max-height="300">
        <el-table-column v-for="h in previewData.headers" :key="h" :prop="h" :label="h" min-width="120" />
      </el-table>
    </el-card>

    <el-card v-if="previewData" shadow="never" style="margin-bottom: 20px">
      <el-form :inline="true">
        <el-form-item :label="$t('whitelist.importMode')">
          <el-radio-group v-model="importMode">
            <el-radio value="append">{{ $t('whitelist.modeAppend') }}</el-radio>
            <el-radio value="overwrite">{{ $t('whitelist.modeOverwrite') }}</el-radio>
            <el-radio value="skip">{{ $t('whitelist.modeSkip') }}</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="importing" @click="handleImport">
            {{ importing ? $t('whitelist.importing') : $t('whitelist.startImport') }}
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card v-if="importResult" shadow="never">
      <template #header>
        <span>{{ $t('whitelist.importResult') }}</span>
      </template>
      <el-descriptions :column="4" border>
        <el-descriptions-item :label="$t('whitelist.totalRecords')">
          <el-tag>{{ importResult.total }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item :label="$t('whitelist.successRecords')">
          <el-tag type="success">{{ importResult.success }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item :label="$t('whitelist.failRecords')">
          <el-tag type="danger">{{ importResult.failed }}</el-tag>
        </el-descriptions-item>
      </el-descriptions>
      <el-alert
        v-if="importResult.errors.length"
        :title="$t('whitelist.viewErrors')"
        type="error"
        :description="importResult.errors.slice(0, 5).map(e => `行${e.row}: ${e.error}`).join('; ')"
        style="margin-top: 12px"
        :closable="false"
      />
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useWhitelistStore } from '@/stores/whitelist'
import type { PreviewResult, ImportResult } from '@/types/whitelist'
import FileUploader from '@/components/FileUploader.vue'

const store = useWhitelistStore()
const previewData = ref<PreviewResult | null>(null)
const importResult = ref<ImportResult | null>(null)
const importMode = ref('append')
const importing = ref(false)
let uploadedFilePath = ''

function downloadTemplate() {
  const blob = new Blob(
    ['msisdn,region,language\n66812345678,TH,th\n'],
    { type: 'text/csv;charset=utf-8' }
  )
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'whitelist_template.csv'
  a.click()
  URL.revokeObjectURL(url)
}

async function handleFile(file: File) {
  try {
    const result = await store.uploadPreview(file)
    previewData.value = result
    uploadedFilePath = result.file_path || ''
    importResult.value = null
  } catch (err: any) {
    ElMessage.error(err.response?.data?.detail || '文件预览失败')
  }
}

async function handleImport() {
  if (!uploadedFilePath) return
  importing.value = true
  try {
    const result = await store.importWhitelist(uploadedFilePath, importMode.value)
    importResult.value = result
    ElMessage.success(`导入完成: ${result.success} 成功, ${result.failed} 失败`)
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '导入失败')
  } finally {
    importing.value = false
  }
}
</script>

<style scoped>
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}
.page-header h2 {
  margin: 0;
  font-size: 22px;
  font-weight: 600;
}
</style>
