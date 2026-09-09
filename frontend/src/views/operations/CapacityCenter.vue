<template>
  <div>
    <div v-if="!embedded" class="page-header">
      <div><h2>{{ $t('menu.capacity') }}</h2><p>{{ $t('legacy.x1d2afdea') }}</p></div>
      <div class="header-actions"><PollingStatus :state="pollState" :updated-at="lastUpdated"/><el-button v-if="canEvaluate" @click="evaluate">{{ $t('legacy.x5c6cbf3e') }}</el-button><el-button type="primary" :loading="loading" @click="load">{{ $t('common.refresh') }}</el-button></div>
    </div>
    <div v-else class="embedded-header">
      <div><h3>{{ $t('menu.capacity') }}</h3><p>{{ $t('legacy.x1d2afdea') }}</p></div>
      <div class="header-actions"><PollingStatus :state="pollState" :updated-at="lastUpdated"/><el-button v-if="canEvaluate" @click="evaluate">{{ $t('legacy.x5c6cbf3e') }}</el-button><el-button type="primary" :loading="loading" @click="load">{{ $t('common.refresh') }}</el-button></div>
    </div>
    <el-alert v-if="error" type="warning" :closable="false" show-icon :title="$t('legacy.x27f373e5')" class="state-alert" />
    <el-row :gutter="16" class="summary">
      <el-col :span="8"><el-card><el-statistic :title="$t('legacy.x6a5cb947')" :value="status.workers?.running || 0"><template #suffix>/ {{ status.workers?.configured || 0 }}</template></el-statistic></el-card></el-col>
      <el-col :span="8"><el-card><el-statistic :title="$t('legacy.xf66a3536')" :value="status.smpp?.enabled_configs || 0" /></el-card></el-col>
      <el-col :span="8"><el-card><el-statistic :title="$t('legacy.xfea90f61')" :value="status.smpp?.active_config_id || 0" /></el-card></el-col>
    </el-row>
    <el-card shadow="never" :header="$t('legacy.xfcd3cef4')">
      <el-table :data="status.smpp?.pools || []" stripe>
        <el-table-column prop="priority" :label="$t('legacy.xee8ecb9e')" width="90" />
        <el-table-column prop="name" label="SMSC" />
        <el-table-column prop="desired_pool_size" :label="$t('legacy.x03803fe7')" width="100" />
        <el-table-column :label="$t('legacy.x66e87429')" width="100"><template #default="{ row }">{{ row.connections.filter((c:any) => c.connected).length }}</template></el-table-column>
        <el-table-column :label="$t('common.status')" width="100"><template #default="{ row }"><el-tag :type="statusOf(row).type">{{ statusOf(row).label }}</el-tag></template></el-table-column>
        <el-table-column prop="cooldown_seconds" :label="$t('legacy.x178fa288')" width="110" />
        <el-table-column :label="$t('legacy.xa2b83df0')"><template #default="{ row }">{{ errorText(row) }}</template></el-table-column>
      </el-table>
    </el-card>
    <el-card shadow="never" :header="$t('legacy.x3db9a78c')" class="stats">
      <el-alert v-if="statsError" type="warning" :closable="false" show-icon :title="$t('legacy.xc4d3ad34')" :description="statsError" class="state-alert" />
      <el-row :gutter="16" class="summary">
        <el-col :span="8"><el-card shadow="never"><el-statistic :title="$t('legacy.x58f7d86e')" :value="stats.summary.queue_size || 0" /></el-card></el-col>
        <el-col :span="8"><el-card shadow="never"><el-statistic :title="$t('legacy.xbd3716f8')" :value="stats.summary.submitted_1m || 0" /></el-card></el-col>
        <el-col :span="8"><el-card shadow="never"><el-statistic :title="$t('legacy.xa1b4c69e')" :value="stats.summary.submitted_60m || 0" /></el-card></el-col>
        <el-col :span="6"><el-card shadow="never"><el-statistic :title="$t('legacy.xdefc7fea')" :value="stats.summary.today_submitted || 0" /></el-card></el-col>
        <el-col :span="6"><el-card shadow="never"><el-statistic :title="$t('legacy.x9e1761f4')" :value="stats.summary.today_delivered || 0" /></el-card></el-col>
        <el-col :span="6"><el-card shadow="never"><el-statistic :title="$t('legacy.x1efcaa09')" :value="stats.summary.today_failed || 0" /></el-card></el-col>
        <el-col v-if="stats.summary.unassigned" :span="6"><el-card shadow="never"><el-statistic :title="$t('legacy.x745c726d')" :value="stats.summary.unassigned" /></el-card></el-col>
      </el-row>
      <el-table :data="stats.items || []" stripe>
        <el-table-column :label="$t('legacy.xb2db9f1c')" min-width="170">
          <template #default="{ row }">
            <div>{{ row.name }}</div>
            <small class="muted">{{ row.host }}:{{ row.port }}</small>
          </template>
        </el-table-column>
        <el-table-column :label="$t('common.status')" width="90">
          <template #default="{ row }"><el-tag :type="statusOf(row).type">{{ statusOf(row).label }}</el-tag></template>
        </el-table-column>
        <el-table-column :label="$t('legacy.x30f7dd4e')" width="90">
          <template #default="{ row }">{{ row.connected_count || 0 }} / {{ row.desired_pool_size || 0 }}</template>
        </el-table-column>
        <el-table-column prop="queue_size" :label="$t('legacy.xd2da477c')" width="90" />
        <el-table-column prop="sending" :label="$t('legacy.x702513ff')" width="80" />
        <el-table-column prop="submitted" :label="$t('legacy.x39130199')" width="80" />
        <el-table-column :label="$t('legacy.xd94af3d4')" min-width="150">
          <template #default="{ row }">{{ row.submitted_1m || 0 }} / {{ row.submitted_5m || 0 }} / {{ row.submitted_60m || 0 }}</template>
        </el-table-column>
        <el-table-column :label="$t('legacy.x26074df2')" min-width="150">
          <template #default="{ row }">{{ row.today_submitted || 0 }} / {{ row.today_delivered || 0 }} / {{ row.today_failed || 0 }}</template>
        </el-table-column>
        <el-table-column :label="$t('legacy.x89e1cf1b')" width="110">
          <template #default="{ row }">{{ pct(row.failure_rate_60m) }}</template>
        </el-table-column>
        <el-table-column :label="$t('legacy.xc7e8969c')" width="110">
          <template #default="{ row }">{{ row.submit_sm_tps_1m || 0 }} / {{ row.configured_tps || 0 }}</template>
        </el-table-column>
        <el-table-column :label="$t('legacy.xa2b83df0')" min-width="150">
          <template #default="{ row }">{{ errorText(row) }}</template>
        </el-table-column>
      </el-table>
    </el-card>
    <el-card shadow="never" :header="$t('legacy.x44b54a85')" class="metrics">
      <el-table :data="metricRows" stripe><el-table-column prop="name" :label="$t('legacy.x7e687515')" /><el-table-column prop="labels" :label="$t('legacy.x14d34236')" /><el-table-column prop="value" :label="$t('legacy.xfe7509e0')" width="140" /></el-table>
    </el-card>
  </div>
</template>
<script setup lang="ts">
import i18n from '@/i18n'
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import PollingStatus from '@/components/PollingStatus.vue'
import { evaluateCapacityAlerts, getCapacityStatus, getSmppStats } from '@/api/capacity'
import { useAuthStore } from '@/stores/auth'
withDefaults(defineProps<{ embedded?: boolean }>(), { embedded: false })
const authStore = useAuthStore()
const canEvaluate = computed(() => ['admin', 'marketing'].includes(authStore.role))
const loading=ref(false),status=ref<any>({}),stats=ref<any>({ summary: {}, items: [] }),statsError=ref(''),error=ref(''),lastUpdated=ref<Date|null>(null),pollState=ref<'live'|'stale'|'offline'>('stale');let timer=0
const metricRows = computed(() => [...(status.value.metrics?.gauges || []), ...(status.value.metrics?.counters || [])].map((m:any) => ({ ...m, labels: Object.entries(m.labels || {}).map(([k,v]) => `${k}=${v}`).join(', ') || '—' })))
function pct(value:number|undefined){return value == null ? '—' : `${(value * 100).toFixed(1)}%`}
function statusOf(row:any): { type: 'success'|'danger'|'info'|'warning'; label: string } {
  if (row.enabled === 0) return { type: 'info', label: i18n.global.t('legacy.x69b0f684') }
  if (row.healthy) return { type: 'success', label: i18n.global.t('legacy.xfbc026c9') }
  if (row.cooldown_seconds > 0) return { type: 'danger', label: i18n.global.t('legacy.xd1e4a7e9') }
  if ((row.connections || []).some((c:any) => c.state === 'error')) return { type: 'danger', label: i18n.global.t('legacy.xd1e4a7e9') }
  return { type: 'info', label: i18n.global.t('dbConfig.disconnected') }
}
function errorText(row:any): string {
  const connections = row.connections || []
  const errors: string[] = []
  for (const c of connections) { if (c.last_error) errors.push(String(c.last_error)) }
  if (row.last_error) errors.push(String(row.last_error))
  if (!errors.length) return '—'
  const current = row.cooldown_seconds > 0 || connections.some((c:any) => c.state === 'error')
  return current ? errors.join('; ') : i18n.global.t('dynamic.historicalErrors',{errors:errors.join('; ')})
}
async function load(){
  loading.value=true
  try{
    status.value=(await getCapacityStatus()).data
    try{
      stats.value=(await getSmppStats()).data
      statsError.value=''
    }catch{
      stats.value={ summary: {}, items: [] }
      statsError.value=i18n.global.t('legacy.xe9c8033e')
    }
    lastUpdated.value=new Date();pollState.value='live';error.value=''
  }catch(e:any){error.value=e.response?.data?.detail||i18n.global.t('common.connectionFailed');pollState.value=lastUpdated.value?'stale':'offline'}
  finally{loading.value=false}
}
async function evaluate(){ const result=(await evaluateCapacityAlerts()).data; ElMessage.success(i18n.global.t('dynamic.alertEvaluationComplete',{count:result.alerts.length})); await load() }
onMounted(()=>{load();timer=window.setInterval(()=>{if(!document.hidden)load()},15000)});onBeforeUnmount(()=>window.clearInterval(timer))
</script>
<style scoped>.page-header,.embedded-header{display:flex;justify-content:space-between;gap:20px;margin-bottom:18px}.page-header h2,.embedded-header h3{margin:0}.embedded-header h3{color:var(--mc-text);font-size:16px}.page-header p,.embedded-header p{margin:5px 0 0;color:var(--mc-text-secondary)}.embedded-header p{font-size:12px}.header-actions{display:flex;flex-wrap:wrap;align-items:center;justify-content:flex-end;gap:8px}.state-alert,.summary{margin-bottom:16px}.metrics,.stats{margin-top:16px}.stats .summary .el-card{margin-bottom:16px}.muted{color:var(--mc-text-secondary)}@media(max-width:767px){.page-header,.embedded-header{flex-direction:column;gap:12px}.header-actions{justify-content:flex-start}.summary :deep(.el-col){width:100%;max-width:100%;flex:0 0 100%;margin-bottom:10px}}.stats .summary :deep(.el-col){width:33.333333%;max-width:33.333333%;flex:0 0 33.333333%}@media(max-width:767px){.stats .summary :deep(.el-col){width:100%;max-width:100%;flex:0 0 100%}}</style>
