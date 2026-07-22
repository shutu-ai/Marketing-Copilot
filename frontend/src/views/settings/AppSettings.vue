<template>
  <div class="app-settings">
    <div class="page-header">
      <h2>{{ $t('settings.title') }}</h2>
    </div>

    <el-card shadow="never">
      <el-form :model="form" label-width="200px" size="small">
        <el-form-item :label="$t('settings.batchBaseDir')">
          <el-input v-model="form.batch_base_dir" placeholder="D:/campaign_data" />
          <div class="form-tip">{{ $t('settings.batchBaseDirTip') }}</div>
        </el-form-item>
        <el-form-item :label="$t('settings.realtimeBaseDir')">
          <el-input v-model="form.realtime_base_dir" placeholder="D:/realtime_campaign_data" />
          <div class="form-tip">{{ $t('settings.realtimeBaseDirTip') }}</div>
        </el-form-item>
        <el-form-item :label="$t('settings.systemMode')">
          <el-radio-group v-model="form.system_mode">
            <el-radio value="production">{{ $t('settings.production') }}</el-radio>
            <el-radio value="development">{{ $t('settings.development') }}</el-radio>
          </el-radio-group>
          <div class="form-tip">{{ $t('settings.systemModeTip') }}</div>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="saving" @click="handleSave">
            <el-icon><Check /></el-icon>
            {{ $t('common.save') }}
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, reactive } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { getSettings, updateSetting } from '@/api/settings'

const { t } = useI18n()

const saving = ref(false)
const form = reactive({
  batch_base_dir: '',
  realtime_base_dir: '',
  system_mode: 'production',
})

onMounted(async () => {
  try {
    const res = await getSettings()
    const settings = res.data || []
    for (const s of settings) {
      if (s.key === 'batch_base_dir') {
        form.batch_base_dir = s.value
      } else if (s.key === 'realtime_base_dir') {
        form.realtime_base_dir = s.value
      } else if (s.key === 'system_mode') {
        form.system_mode = s.value
      }
    }
  } catch {
    // ignore
  }
})

async function handleSave() {
  saving.value = true
  try {
    await updateSetting('batch_base_dir', form.batch_base_dir)
    await updateSetting('realtime_base_dir', form.realtime_base_dir)
    await updateSetting('system_mode', form.system_mode)
    ElMessage.success(t('common.saveSuccess'))
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || t('common.saveFailed'))
  } finally {
    saving.value = false
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
.form-tip {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
}
</style>
