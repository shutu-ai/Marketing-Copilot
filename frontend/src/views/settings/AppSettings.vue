<template>
  <div class="app-settings">
    <div class="page-header">
      <div>
        <h2>{{ $t('settings.title') }}</h2>
        <p>{{ $t('settings.description') }}</p>
      </div>
    </div>

    <el-tabs v-model="activeTab" class="settings-tabs" @tab-change="handleTabChange">
      <el-tab-pane :label="$t('settings.basicTab')" name="basic">
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
            <el-form-item :label="$t('settings.logLevel')">
              <div class="log-level-row">
                <el-select v-model="form.log_level" style="width: 160px">
                  <el-option v-for="lv in logLevels" :key="lv" :label="lv" :value="lv" />
                </el-select>
                <el-button link type="primary" @click="router.push('/logs')">
                  {{ $t('settings.viewLogs') }}
                </el-button>
              </div>
              <div class="form-tip">{{ $t('settings.logLevelTip') }}</div>
            </el-form-item>
            <el-form-item :label="$t('settings.detailLogging')">
              <div>
                <el-switch v-model="form.detail_logging" active-value="1" inactive-value="0" />
                <div class="form-tip">{{ $t('settings.detailLoggingTip') }}</div>
              </div>
            </el-form-item>
            <el-form-item>
              <el-button type="primary" :loading="saving" @click="handleSave">
                <el-icon><Check /></el-icon>
                {{ $t('common.save') }}
              </el-button>
            </el-form-item>
          </el-form>
        </el-card>
      </el-tab-pane>

      <el-tab-pane :label="$t('settings.smppTab')" name="smpp" lazy>
        <SmppConfig embedded />
      </el-tab-pane>

      <el-tab-pane :label="$t('settings.databaseTab')" name="database" lazy>
        <el-alert
          :title="$t('settings.databaseWarning')"
          type="warning"
          show-icon
          :closable="false"
          class="database-warning"
        />
        <DbConfig embedded />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, reactive, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { getSettings, updateSetting } from '@/api/settings'
import SmppConfig from '@/views/smpp/SmppConfig.vue'
import DbConfig from '@/views/dbConfig/DbConfig.vue'

const { t } = useI18n()
const route = useRoute()
const router = useRouter()
const validTabs = ['basic', 'smpp', 'database'] as const

type SettingsTab = typeof validTabs[number]

function normalizeTab(value: unknown): SettingsTab {
  return typeof value === 'string' && validTabs.includes(value as SettingsTab)
    ? value as SettingsTab
    : 'basic'
}

const activeTab = ref<SettingsTab>(normalizeTab(route.query.tab))
const saving = ref(false)
const form = reactive({
  batch_base_dir: '',
  realtime_base_dir: '',
  system_mode: 'production',
  log_level: 'INFO',
  detail_logging: '0',
})
const logLevels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

watch(() => route.query.tab, value => {
  activeTab.value = normalizeTab(value)
})

function handleTabChange(tab: string | number) {
  const nextTab = normalizeTab(String(tab))
  if (route.query.tab !== nextTab) {
    router.replace({ path: '/settings', query: { ...route.query, tab: nextTab } })
  }
}

onMounted(async () => {
  if (!route.query.tab) {
    router.replace({ path: '/settings', query: { ...route.query, tab: activeTab.value } })
  }
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
      } else if (s.key === 'log_level') {
        form.log_level = s.value
      } else if (s.key === 'detail_logging') {
        form.detail_logging = s.value
      }
    }
  } catch {
    // Individual configuration tabs retain their own connection/error states.
  }
})

async function handleSave() {
  saving.value = true
  try {
    await updateSetting('batch_base_dir', form.batch_base_dir)
    await updateSetting('realtime_base_dir', form.realtime_base_dir)
    await updateSetting('system_mode', form.system_mode)
    await updateSetting('log_level', form.log_level)
    await updateSetting('detail_logging', form.detail_logging)
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
  margin-bottom: 12px;
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
.settings-tabs :deep(.el-tabs__header) {
  margin-bottom: 16px;
}
.form-tip {
  font-size: 12px;
  color: var(--mc-text-tertiary);
  margin-top: 4px;
}
.database-warning {
  margin-bottom: 16px;
}
.log-level-row {
  display: flex;
  align-items: center;
  gap: 10px;
}
@media (max-width: 767px) {
  .settings-tabs :deep(.el-tabs__item) {
    padding: 0 12px;
  }
  :deep(.el-form-item__label) {
    width: 100% !important;
    justify-content: flex-start;
  }
  :deep(.el-form-item__content) {
    margin-left: 0 !important;
  }
}
</style>
