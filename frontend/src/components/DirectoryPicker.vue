<template>
  <el-dialog v-model="visible" :title="$t('campaign.browseDirectory')" width="620px" @open="load(initialPath)">
    <div class="path-bar">
      <el-button :disabled="!parent" @click="load(parent || '')">{{ $t('campaign.parentDirectory') }}</el-button>
      <code>{{ currentPath || $t('campaign.selectRoot') }}</code>
    </div>
    <el-alert type="info" :closable="false" show-icon :title="$t('campaign.directoryPickerTip')" />
    <el-scrollbar height="300px" v-loading="loading" class="directory-list">
      <el-empty v-if="!loading && !entries.length" :description="$t('campaign.noSubdirectories')" :image-size="72" />
      <el-button v-for="entry in entries" :key="entry.path" text class="directory-row" @click="load(entry.path)">
        <el-icon><Folder /></el-icon><span>{{ entry.name }}</span>
      </el-button>
    </el-scrollbar>
    <template #footer>
      <el-button @click="visible = false">{{ $t('common.cancel') }}</el-button>
      <el-button type="primary" :disabled="!currentPath" @click="selectCurrent">{{ $t('campaign.selectCurrentDirectory') }}</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Folder } from '@element-plus/icons-vue'
import * as filesystemApi from '@/api/filesystem'

const props = defineProps<{ modelValue: boolean; initialPath?: string }>()
const emit = defineEmits<{ 'update:modelValue': [value: boolean]; select: [path: string] }>()
const visible = computed({ get: () => props.modelValue, set: value => emit('update:modelValue', value) })
const currentPath = ref('')
const parent = ref<string | null>(null)
const entries = ref<filesystemApi.DirectoryEntry[]>([])
const loading = ref(false)

async function load(path = '') {
  loading.value = true
  try {
    const { data } = await filesystemApi.listDirectories(path)
    currentPath.value = data.path
    parent.value = data.parent
    entries.value = data.entries
  } catch (error: any) {
    if (path) {
      await load('')
      return
    }
    ElMessage.error(error.response?.data?.detail || '无法读取目录')
  } finally {
    loading.value = false
  }
}

function selectCurrent() {
  emit('select', currentPath.value)
  visible.value = false
}
</script>

<style scoped>
.path-bar { display:flex; align-items:center; gap:12px; margin-bottom:12px; min-height:32px; }
.path-bar code { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.directory-list { margin-top:12px; border:1px solid var(--el-border-color); border-radius:4px; }
.directory-row { width:100%; justify-content:flex-start; gap:8px; padding:0 14px; height:38px; }
</style>
