<template>
  <div class="dashboard">
    <PageHeader :title="$t('dashboard.title')" :description="$t('dashboard.description')">
      <template #actions><el-button :loading="loading" @click="load"><el-icon><Refresh /></el-icon>{{ $t('common.refresh') }}</el-button></template>
    </PageHeader>
    <DataState :loading="loading" :has-data="loaded" :error="error" @retry="load">
      <el-alert v-if="error" type="warning" :closable="false" show-icon :title="$t('dashboard.stale')" class="stale-alert" />
      <section class="kpi-grid" :aria-label="$t('dashboard.kpi')">
        <article v-for="item in kpis" :key="item.label" class="kpi-card"><div class="kpi-card__label">{{ item.label }}</div><strong>{{ item.value.toLocaleString() }}</strong><span>{{ item.hint }}</span></article>
      </section>
      <section class="dashboard-grid">
        <article class="panel trend-panel">
          <div class="panel__header"><div><h3>{{ $t('dashboard.trendTitle') }}</h3><p>{{ $t('dashboard.trendDescription') }}</p></div><strong>{{ trendTotal.toLocaleString() }}</strong></div>
          <div class="bar-chart" role="img" :aria-label="$t('dashboard.chartLabel')">
            <div v-for="point in stats.daily_trend" :key="point.stats_date" class="bar-chart__item">
              <span class="bar-chart__value">{{ point.sent_count }}</span><div class="bar-chart__track"><div class="bar-chart__bar" :style="{height: `${barHeight(point.sent_count)}%`}" /></div><span>{{ point.stats_date.slice(5) }}</span>
            </div>
          </div>
        </article>
        <article class="panel">
          <div class="panel__header"><div><h3>{{ $t('dashboard.exceptions') }}</h3><p>{{ stats.today_open_exception_count }} {{ $t('legacy.x1c391952') }}</p></div><el-button link type="primary" @click="$router.push({path:'/operations',query:{tab:'exceptions'}})">{{ $t('dashboard.viewAll') }}</el-button></div>
          <div v-if="stats.today_recent_exceptions.length" class="exception-list">
            <button v-for="item in stats.today_recent_exceptions" :key="item.id" class="exception-item" @click="$router.push({path:'/operations',query:{tab:'exceptions'}})"><el-tag :type="item.severity === 'error' ? 'danger' : 'warning'" size="small">{{ item.severity }}</el-tag><span><strong>{{ item.code }}</strong><small>{{ item.message }}</small></span><el-icon><ArrowRight /></el-icon></button>
          </div>
          <div v-else class="healthy-empty"><el-icon><CircleCheckFilled /></el-icon><strong>{{ $t('legacy.xf61f4cf6') }}{{ $t('dashboard.exceptions') }}</strong><span>{{ $t('dashboard.healthy') }}</span></div>
        </article>
      </section>
      <section class="panel quick-panel"><div class="panel__header"><div><h3>{{ $t('dashboard.quickActions') }}</h3><p>{{ $t('dashboard.quickDescription') }}</p></div></div><div class="quick-grid"><el-button type="primary" @click="$router.push('/campaign/batch')"><el-icon><Plus /></el-icon>{{ $t('dashboard.createBatch') }}</el-button><el-button @click="$router.push('/campaign/realtime')">{{ $t('dashboard.manageRealtime') }}</el-button><el-button @click="$router.push({path:'/operations',query:{tab:'sms-tasks',status:'failed'}})">{{ $t('dashboard.failedTasks') }}</el-button><el-button @click="$router.push({path:'/operations',query:{tab:'capacity'}})">{{ $t('dashboard.capacityHealth') }}</el-button></div></section>
    </DataState>
  </div>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { getDashboardStats } from '@/api/dashboard'
import PageHeader from '@/components/PageHeader.vue'
import DataState from '@/components/DataState.vue'
const stats=reactive({total_batch_campaigns:0,in_progress_campaigns:0,in_progress_batch_campaigns:0,in_progress_realtime_campaigns:0,ended_campaigns:0,today_sent_count:0,total_sms_campaigns:0,realtime_campaigns:0,today_realtime_files:0,today_realtime_event_users:0,today_realtime_matched_users:0,daily_trend:[] as Array<{stats_date:string;sent_count:number;conversion_count:number}>,today_open_exception_count:0,today_recent_exceptions:[] as any[]})
const loading=ref(false),loaded=ref(false),error=ref('')
const { t } = useI18n()
const kpis=computed(()=>[{label:t('dashboard.todaySent'),value:stats.today_sent_count,hint:t('dashboard.deliveredSms')},{label:t('dashboard.inProgress'),value:stats.in_progress_campaigns,hint:t('dashboard.batchRealtime',{batch:stats.in_progress_batch_campaigns,realtime:stats.in_progress_realtime_campaigns})},{label:t('dashboard.realtimeMatched'),value:stats.today_realtime_matched_users,hint:t('dashboard.fromFiles',{count:stats.today_realtime_files})},{label:t('dashboard.openExceptions'),value:stats.today_open_exception_count,hint:stats.today_open_exception_count?t('dashboard.handleSoon'):t('dashboard.healthy')}])
const trendTotal=computed(()=>stats.daily_trend.reduce((sum,item)=>sum+item.sent_count,0)),trendMax=computed(()=>Math.max(1,...stats.daily_trend.map(item=>item.sent_count)))
const barHeight=(value:number)=>value?Math.max(8,Math.round(value/trendMax.value*100)):2
async function load(){loading.value=true;error.value='';try{Object.assign(stats,(await getDashboardStats()).data);loaded.value=true}catch(e:any){error.value=e.response?.data?.detail||t('common.connectionFailed')}finally{loading.value=false}}
onMounted(load)
</script>
<style scoped>
.stale-alert{margin-bottom:18px}.kpi-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px;margin-bottom:16px}.kpi-card,.panel{background:var(--mc-surface);border:1px solid var(--mc-border);border-radius:var(--mc-radius-lg);box-shadow:var(--mc-shadow)}.kpi-card{position:relative;overflow:hidden;padding:21px 22px;transition:transform .2s ease,box-shadow .2s ease}.kpi-card::after{position:absolute;right:0;bottom:0;width:54px;height:4px;background:var(--mc-primary);border-radius:4px 0 0;content:""}.kpi-card:hover{transform:translateY(-2px);box-shadow:var(--mc-shadow-hover)}.kpi-card__label{color:var(--mc-text-secondary);font-size:13px;font-weight:600}.kpi-card strong{display:block;margin:10px 0 6px;color:var(--mc-text);font-size:30px;font-weight:750;line-height:1}.kpi-card span,.panel p{color:var(--mc-text-tertiary);font-size:12px}.dashboard-grid{display:grid;grid-template-columns:minmax(0,1.65fr) minmax(320px,1fr);gap:16px}.panel{padding:21px 22px}.panel__header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px}.panel h3{margin:0 0 5px;color:var(--mc-text);font-size:15px;font-weight:700}.panel p{margin:0}.bar-chart{height:230px;display:flex;align-items:flex-end;gap:12px;padding-top:28px}.bar-chart__item{height:100%;flex:1;display:flex;flex-direction:column;align-items:center;gap:7px;color:var(--mc-text-tertiary);font-size:11px}.bar-chart__track{width:100%;height:100%;display:flex;align-items:flex-end;border-bottom:1px solid var(--mc-border);background:repeating-linear-gradient(to bottom,transparent 0,transparent calc(25% - 1px),#edf2f1 25%)}.bar-chart__bar{width:100%;min-height:2px;background:var(--mc-primary);border-radius:7px 7px 2px 2px;box-shadow:0 7px 16px rgb(201 150 23 / 16%)}.bar-chart__value{font-variant-numeric:tabular-nums}.exception-list{margin-top:14px}.exception-item{width:100%;display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:11px;padding:12px 0;background:none;border:0;border-bottom:1px solid var(--mc-border);text-align:left;color:inherit;cursor:pointer}.exception-item:hover{color:var(--mc-primary-hover)}.exception-item span{min-width:0}.exception-item strong,.exception-item small{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.exception-item small{margin-top:3px;color:var(--mc-text-secondary)}.healthy-empty{min-height:196px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;color:var(--mc-text-secondary)}.healthy-empty .el-icon{font-size:30px;color:var(--mc-success)}.quick-panel{margin-top:16px}.quick-grid{display:flex;flex-wrap:wrap;gap:10px;margin-top:16px}.quick-grid .el-button{margin-left:0}@media(max-width:900px){.kpi-grid{grid-template-columns:repeat(2,1fr)}.dashboard-grid{grid-template-columns:1fr}}@media(max-width:520px){.kpi-grid{grid-template-columns:1fr 1fr;gap:10px}.kpi-card{padding:16px}.kpi-card strong{font-size:24px}.bar-chart{height:180px}.quick-grid .el-button{width:100%}}
.bar-chart__item { position: relative; }
.bar-chart__value { width: 100%; text-align: center; }
.bar-chart__track { justify-content: center; }
.bar-chart__bar { width: min(42%, 24px); }
</style>
