<template>
  <div class="upload-history">
    <div class="page-header">
      <h2>{{ $t('whitelist.history') }}</h2>
      <el-button @click="store.fetchHistory()">
        <el-icon><Refresh /></el-icon>
        {{ $t('common.search') }}
      </el-button>
    </div>

    <el-card shadow="never">
      <el-table :data="store.history" stripe v-loading="store.loading" style="width: 100%">
        <el-table-column prop="filename" :label="$t('whitelist.filename')" min-width="200" />
        <el-table-column :label="$t('whitelist.totalRecords')" width="100">
          <template #default="{ row }">
            {{ row.total_rows }}
          </template>
        </el-table-column>
        <el-table-column :label="$t('whitelist.successRecords')" width="100">
          <template #default="{ row }">
            <el-tag type="success" size="small">{{ row.success_rows }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="$t('whitelist.failRecords')" width="100">
          <template #default="{ row }">
            <el-tag v-if="row.failed_rows > 0" type="danger" size="small">{{ row.failed_rows }}</el-tag>
            <span v-else>0</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('whitelist.importStatus')" width="120">
          <template #default="{ row }">
            <StatusTag :status="row.status" />
          </template>
        </el-table-column>
        <el-table-column prop="import_mode" label="导入模式" width="100" />
        <el-table-column prop="created_at" :label="$t('common.created_at')" width="180" />
        <el-table-column :label="$t('common.action')" width="150" fixed="right">
          <template #default="{ row }">
            <el-button text type="primary" size="small" @click="viewDetail(row.id)">
              {{ $t('whitelist.viewErrors') }}
            </el-button>
            <el-button text type="danger" size="small" @click="handleDelete(row.id)">
              {{ $t('common.delete') }}
            </el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="detailVisible" title="导入错误详情" width="600px">
      <el-table :data="errorRows" stripe max-height="400">
        <el-table-column prop="msisdn" label="msisdn" width="160" />
        <el-table-column prop="error" label="错误原因" min-width="200" />
      </el-table>
      <div v-if="!errorRows.length" style="text-align: center; padding: 20px; color: #909399">
        无错误记录
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useWhitelistStore } from '@/stores/whitelist'
import { getWhitelistUsers } from '@/api/whitelist'
import api from '@/api/index'
import StatusTag from '@/components/StatusTag.vue'

const store = useWhitelistStore()
const detailVisible = ref(false)
const errorRows = ref<any[]>([])

onMounted(() => store.fetchHistory())

async function viewDetail(uploadId: number) {
  try {
    const res = await getWhitelistUsers(uploadId)
    errorRows.value = res.data.map((u: any) => ({
      msisdn: u.msisdn,
      error: u.status === 'invalid' ? '无效号码' : '导入成功',
    })).filter((u: any) => u.error !== '导入成功')
    detailVisible.value = true
  } catch {
    errorRows.value = []
    detailVisible.value = true
  }
}

async function handleDelete(id: number) {
  try {
    const { ElMessageBox, ElMessage } = await import('element-plus')
    await ElMessageBox.confirm('确认删除该条记录？', '提示')
    await api.delete(`/whitelist/history/${id}`)
    ElMessage.success('删除成功')
    store.fetchHistory()
  } catch {}
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
