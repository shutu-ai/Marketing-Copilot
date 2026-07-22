<template>
  <div class="db-config">
    <div class="page-header">
      <h2>{{ $t('dbConfig.title') }}</h2>
      <div>
        <el-tag :type="connected ? 'success' : 'danger'" size="default">
          {{ connected ? $t('dbConfig.connected') : $t('dbConfig.disconnected') }}
        </el-tag>
      </div>
    </div>

    <el-card shadow="never">
      <template #header>
        <span>{{ $t('common.edit') }} {{ $t('dbConfig.title') }}</span>
      </template>
      <el-form :model="form" label-width="140px" size="small">
        <el-row :gutter="20">
          <el-col :span="16">
            <el-form-item :label="$t('dbConfig.dbPath')" required>
              <el-input v-model="form.db_path" placeholder="marketing_copilot.duckdb" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item>
          <el-button type="primary" :loading="saving" @click="handleSave">
            <el-icon><Check /></el-icon>
            {{ $t('common.save') }}
          </el-button>
          <el-button :loading="testing" @click="handleTest">
            <el-icon><Refresh /></el-icon>
            {{ $t('common.test') }}
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { getDbConfig, saveDbConfig, testDbConnection } from '@/api/dbConfig'

const connected = ref(false)
const saving = ref(false)
const testing = ref(false)

const form = ref({
  db_path: '',
})

onMounted(async () => {
  try {
    const res = await getDbConfig()
    form.value = { db_path: res.data.db_path || '' }
    connected.value = true
  } catch {
    connected.value = false
  }
})

async function handleSave() {
  saving.value = true
  try {
    await saveDbConfig(form.value)
    ElMessage.success('保存成功')
    connected.value = true
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '保存失败')
    connected.value = false
  } finally {
    saving.value = false
  }
}

async function handleTest() {
  testing.value = true
  try {
    const res = await testDbConnection(form.value)
    if (res.data.success) {
      ElMessage.success(res.data.message)
    } else {
      ElMessage.warning(res.data.message)
    }
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '测试失败')
  } finally {
    testing.value = false
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
