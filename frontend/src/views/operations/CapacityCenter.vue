<template>
  <div>
    <div v-if="!embedded" class="page-header">
      <div><h2>容量与高可用</h2><p>Worker、SMSC 主备连接池、队列和自动告警状态</p></div>
      <div class="header-actions"><PollingStatus :state="pollState" :updated-at="lastUpdated"/><el-button @click="evaluate">立即评估告警</el-button><el-button type="primary" :loading="loading" @click="load">刷新</el-button></div>
    </div>
    <div v-else class="embedded-header">
      <div><h3>容量与高可用</h3><p>Worker、SMSC 主备连接池、队列和自动告警状态</p></div>
      <div class="header-actions"><PollingStatus :state="pollState" :updated-at="lastUpdated"/><el-button @click="evaluate">立即评估告警</el-button><el-button type="primary" :loading="loading" @click="load">刷新</el-button></div>
    </div>
    <el-alert v-if="error" type="warning" :closable="false" show-icon title="容量状态刷新失败，当前展示上次成功获取的数据" class="state-alert" />
    <el-row :gutter="16" class="summary">
      <el-col :span="8"><el-card><el-statistic title="运行 Worker" :value="status.workers?.running || 0"><template #suffix>/ {{ status.workers?.configured || 0 }}</template></el-statistic></el-card></el-col>
      <el-col :span="8"><el-card><el-statistic title="启用 SMSC" :value="status.smpp?.enabled_configs || 0" /></el-card></el-col>
      <el-col :span="8"><el-card><el-statistic title="活动 SMSC" :value="status.smpp?.active_config_id || 0" /></el-card></el-col>
    </el-row>
    <el-card shadow="never" header="SMSC 连接池">
      <el-table :data="status.smpp?.pools || []" stripe>
        <el-table-column prop="priority" label="优先级" width="90" />
        <el-table-column prop="name" label="SMSC" />
        <el-table-column prop="desired_pool_size" label="期望连接" width="100" />
        <el-table-column label="健康连接" width="100"><template #default="{ row }">{{ row.connections.filter((c:any) => c.connected).length }}</template></el-table-column>
        <el-table-column label="状态" width="100"><template #default="{ row }"><el-tag :type="row.healthy ? 'success' : 'danger'">{{ row.healthy ? '健康' : '不可用' }}</el-tag></template></el-table-column>
        <el-table-column prop="cooldown_seconds" label="冷却秒数" width="110" />
        <el-table-column label="最近错误"><template #default="{ row }">{{ row.connections.map((c:any) => c.last_error).filter(Boolean).join('; ') || '—' }}</template></el-table-column>
      </el-table>
    </el-card>
    <el-card shadow="never" header="运行指标" class="metrics">
      <el-table :data="metricRows" stripe><el-table-column prop="name" label="指标" /><el-table-column prop="labels" label="标签" /><el-table-column prop="value" label="值" width="140" /></el-table>
    </el-card>
  </div>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import PollingStatus from '@/components/PollingStatus.vue'
import { evaluateCapacityAlerts, getCapacityStatus } from '@/api/capacity'
withDefaults(defineProps<{ embedded?: boolean }>(), { embedded: false })
const loading=ref(false),status=ref<any>({}),error=ref(''),lastUpdated=ref<Date|null>(null),pollState=ref<'live'|'stale'|'offline'>('stale');let timer=0
const metricRows = computed(() => [...(status.value.metrics?.gauges || []), ...(status.value.metrics?.counters || [])].map((m:any) => ({ ...m, labels: Object.entries(m.labels || {}).map(([k,v]) => `${k}=${v}`).join(', ') || '—' })))
async function load(){loading.value=true;try{status.value=(await getCapacityStatus()).data;lastUpdated.value=new Date();pollState.value='live';error.value=''}catch(e:any){error.value=e.response?.data?.detail||'无法连接到服务';pollState.value=lastUpdated.value?'stale':'offline'}finally{loading.value=false}}
async function evaluate(){ const result=(await evaluateCapacityAlerts()).data; ElMessage.success(`告警评估完成：${result.alerts.length} 个活动告警`); await load() }
onMounted(()=>{load();timer=window.setInterval(()=>{if(!document.hidden)load()},15000)});onBeforeUnmount(()=>window.clearInterval(timer))
</script>
<style scoped>.page-header,.embedded-header{display:flex;justify-content:space-between;gap:20px;margin-bottom:18px}.page-header h2,.embedded-header h3{margin:0}.embedded-header h3{color:var(--mc-text);font-size:16px}.page-header p,.embedded-header p{margin:5px 0 0;color:var(--mc-text-secondary)}.embedded-header p{font-size:12px}.header-actions{display:flex;flex-wrap:wrap;align-items:center;justify-content:flex-end;gap:8px}.state-alert,.summary{margin-bottom:16px}.metrics{margin-top:16px}@media(max-width:767px){.page-header,.embedded-header{flex-direction:column;gap:12px}.header-actions{justify-content:flex-start}.summary :deep(.el-col){width:100%;max-width:100%;flex:0 0 100%;margin-bottom:10px}}</style>
