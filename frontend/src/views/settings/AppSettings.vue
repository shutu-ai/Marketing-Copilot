<template>
  <div class="app-settings">
    <div class="page-header">
      <div>
        <h2>{{ $t('settings.title') }}</h2>
        <p>{{ $t('settings.description') }}</p>
      </div>
    </div>

    <el-tabs v-model="activeTab" class="settings-tabs" @tab-change="handleTabChange">
      <el-tab-pane v-if="canSee.basic" :label="$t('settings.basicTab')" name="basic">
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
            <el-form-item :label="$t('settings.smsTaskRetentionDays')">
              <el-input-number v-model="form.sms_task_retention_days" :min="1" :precision="0" controls-position="right" />
              <div class="form-tip">{{ $t('settings.smsTaskRetentionDaysTip') }}</div>
            </el-form-item>
            <el-form-item :label="$t('settings.realtimeFileRetentionDays')">
              <el-input-number v-model="form.realtime_file_retention_days" :min="1" :precision="0" controls-position="right" />
              <div class="form-tip">{{ $t('settings.realtimeFileRetentionDaysTip') }}</div>
            </el-form-item>
            <el-form-item :label="$t('settings.realtimeOutputRetentionDays')">
              <el-input-number v-model="form.realtime_output_retention_days" :min="1" :precision="0" controls-position="right" />
              <div class="form-tip">{{ $t('settings.realtimeOutputRetentionDaysTip') }}</div>
            </el-form-item>
            <el-form-item label="Realtime match chunk size">
              <el-input-number v-model="form.realtime_match_chunk_size" :min="500" :max="100000" :precision="0" controls-position="right" />
              <div class="form-tip">Rows processed per input chunk.</div>
            </el-form-item>
            <el-form-item label="Whitelist build memory">
              <el-input v-model="form.realtime_whitelist_build_memory_limit" placeholder="4GB" />
            </el-form-item>
            <el-form-item label="Whitelist build threads">
              <el-input-number v-model="form.realtime_whitelist_build_threads" :min="1" :max="16" :precision="0" controls-position="right" />
            </el-form-item>
            <el-form-item label="Whitelist build timeout (seconds)">
              <el-input-number v-model="form.realtime_index_build_timeout" :min="0" :max="86400" :precision="0" controls-position="right" />
            </el-form-item>
            <el-form-item label="Whitelist upload timeout (seconds)">
              <el-input-number v-model="form.realtime_whitelist_upload_timeout" :min="1" :max="86400" :precision="0" controls-position="right" />
              <div class="form-tip">Client request timeout for uploading the whitelist; index build timeout is configured separately.</div>
            </el-form-item>
            <el-form-item label="Whitelist retention (days)">
              <el-input-number v-model="form.realtime_whitelist_retention_days" :min="1" :max="3650" :precision="0" controls-position="right" />
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

      <el-tab-pane v-if="canSee.smpp" :label="$t('settings.smppTab')" name="smpp" lazy>
        <SmppConfig embedded />
      </el-tab-pane>

      <el-tab-pane v-if="canSee.database" :label="$t('settings.databaseTab')" name="database" lazy>
        <el-alert
          :title="$t('settings.databaseWarning')"
          type="warning"
          show-icon
          :closable="false"
          class="database-warning"
        />
        <DbConfig embedded />
      </el-tab-pane>

      <el-tab-pane v-if="canSee.logs" :label="$t('settings.logTab')" name="logs" lazy>
        <LogViewer embedded />
      </el-tab-pane>

      <el-tab-pane v-if="canSee.update" :label="$t('settings.updateTab')" name="update" lazy>
        <SystemUpdate embedded />
      </el-tab-pane>

      <el-tab-pane v-if="canSee.users" :label="$t('settings.usersTab')" name="users" lazy>
        <UserManagement />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, reactive, watch, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { getSettings, updateSetting } from '@/api/settings'
import SmppConfig from '@/views/smpp/SmppConfig.vue'
import DbConfig from '@/views/dbConfig/DbConfig.vue'
import LogViewer from '@/views/logs/LogViewer.vue'
import SystemUpdate from '@/views/update/SystemUpdate.vue'
import UserManagement from '@/views/users/UserManagement.vue'
import { useAuthStore } from '@/stores/auth'

const { t } = useI18n()
const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()
const validTabs = ['basic', 'smpp', 'database', 'logs', 'update', 'users'] as const

type SettingsTab = typeof validTabs[number]

function visibleTabs(): SettingsTab[] {
  const role = authStore.role
  const map: Record<string, SettingsTab[]> = {
    admin: [...validTabs],
    marketing: ['logs'],
    analyst: ['logs'],
  }
  return map[role] || ['logs']
}

function normalizeTab(value: unknown): SettingsTab {
  const tabs = visibleTabs()
  return typeof value === 'string' && tabs.includes(value as SettingsTab)
    ? value as SettingsTab
    : tabs[0]
}

const canSee = computed(() => {
  const role = authStore.role
  return {
    basic: role === 'admin',
    smpp: role === 'admin',
    database: role === 'admin',
    logs: true,
    update: role === 'admin',
    users: role === 'admin',
  }
})

const activeTab = ref<SettingsTab>(normalizeTab(route.query.tab))
const saving = ref(false)
const form = reactive({
  batch_base_dir: '',
  realtime_base_dir: '',
  log_level: 'INFO',
  detail_logging: '0',
  sms_task_retention_days: 7,
  realtime_file_retention_days: 1,
  realtime_output_retention_days: 30,
  realtime_match_chunk_size: 5000,
  realtime_whitelist_build_memory_limit: '4GB',
  realtime_whitelist_build_threads: 1,
  realtime_index_build_timeout: 7200,
  realtime_whitelist_upload_timeout: 3600,
  realtime_whitelist_retention_days: 7,
})
const logLevels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

function parseSettingNumber(value: unknown, fallback: number): number {
  const text = String(value ?? '').trim()
  if (!text) return fallback
  const parsed = Number(text)
  return Number.isFinite(parsed) ? parsed : fallback
}

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
      } else if (s.key === 'log_level') {
        form.log_level = s.value
      } else if (s.key === 'detail_logging') {
        form.detail_logging = s.value
      } else if (s.key === 'sms_task_retention_days') {
        form.sms_task_retention_days = parseSettingNumber(s.value, 7)
      } else if (s.key === 'realtime_file_retention_days') {
        form.realtime_file_retention_days = parseSettingNumber(s.value, 1)
      } else if (s.key === 'realtime_output_retention_days') {
        form.realtime_output_retention_days = parseSettingNumber(s.value, 30)
      } else if (s.key === 'realtime_match_chunk_size') {
        form.realtime_match_chunk_size = parseSettingNumber(s.value, 5000)
      } else if (s.key === 'realtime_whitelist_build_memory_limit') {
        form.realtime_whitelist_build_memory_limit = s.value || '4GB'
      } else if (s.key === 'realtime_whitelist_build_threads') {
        form.realtime_whitelist_build_threads = parseSettingNumber(s.value, 1)
      } else if (s.key === 'realtime_index_build_timeout') {
        form.realtime_index_build_timeout = parseSettingNumber(s.value, 7200)
      } else if (s.key === 'realtime_whitelist_upload_timeout') {
        form.realtime_whitelist_upload_timeout = parseSettingNumber(s.value, 3600)
      } else if (s.key === 'realtime_whitelist_retention_days') {
        form.realtime_whitelist_retention_days = parseSettingNumber(s.value, 7)
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
    await updateSetting('log_level', form.log_level)
    await updateSetting('detail_logging', form.detail_logging)
    await updateSetting('sms_task_retention_days', String(form.sms_task_retention_days))
    await updateSetting('realtime_file_retention_days', String(form.realtime_file_retention_days))
    await updateSetting('realtime_output_retention_days', String(form.realtime_output_retention_days))
    await updateSetting('realtime_match_chunk_size', String(form.realtime_match_chunk_size))
    await updateSetting('realtime_whitelist_build_memory_limit', form.realtime_whitelist_build_memory_limit)
    await updateSetting('realtime_whitelist_build_threads', String(form.realtime_whitelist_build_threads))
    await updateSetting('realtime_index_build_timeout', String(form.realtime_index_build_timeout))
    await updateSetting('realtime_whitelist_upload_timeout', String(form.realtime_whitelist_upload_timeout))
    await updateSetting('realtime_whitelist_retention_days', String(form.realtime_whitelist_retention_days))
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
