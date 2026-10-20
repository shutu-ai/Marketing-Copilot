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
          <div class="panel__header"><div><h3>{{ $t('dashboard.trendTitle') }}</h3><p>{{ $t(stats.pipeline === 'vnext' ? 'dashboard.vnextTrendDescription' : 'dashboard.trendDescription') }}</p></div></div>
          <div class="bar-chart" role="img" :aria-label="$t('dashboard.chartLabel')">
            <div v-for="point in stats.daily_trend" :key="point.stats_date" class="bar-chart__item" tabindex="0" :aria-label="point.stats_date">
              <div class="trend-bars"><div class="trend-series trend-series--files"><span class="trend-series__value">{{ point.received_file_count }}</span><div class="trend-series__track"><div class="trend-series__fill trend-series__fill--files" :style="{height: `${filesHeight(point.received_file_count)}%`}" /></div></div><div class="trend-series trend-series--matched"><span class="trend-series__value">{{ point.matched_count }}</span><div class="trend-series__track"><div class="trend-series__fill trend-series__fill--matched" :style="{height: `${matchedHeight(point.matched_count)}%`}" /></div></div><div class="trend-series trend-series--sent"><span class="trend-series__value">{{ point.sent_count }}</span><div class="trend-series__track"><div class="trend-series__fill trend-series__fill--sent" :style="{height: `${barHeight(point.sent_count)}%`}" /></div></div></div><div class="trend-tooltip" role="tooltip"><strong>{{ point.stats_date }}</strong><span><i class="trend-legend__swatch trend-legend__swatch--files" />{{ $t('dashboard.trendFiles') }}: <strong>{{ formatTrendNumber(point.received_file_count) }}</strong></span><span><i class="trend-legend__swatch trend-legend__swatch--matched" />{{ $t('dashboard.trendMatched') }}: <strong>{{ formatTrendNumber(point.matched_count) }}</strong></span><span><i class="trend-legend__swatch trend-legend__swatch--sent" />{{ $t('dashboard.trendSent') }}: <strong>{{ formatTrendNumber(point.sent_count) }}</strong></span></div><span>{{ point.stats_date.slice(5) }}</span>
            </div>
          </div>
          <div class="trend-legend"><span><i class="trend-legend__swatch trend-legend__swatch--files" />{{ $t('dashboard.trendFiles') }}</span><span><i class="trend-legend__swatch trend-legend__swatch--matched" />{{ $t('dashboard.trendMatched') }}</span><span><i class="trend-legend__swatch trend-legend__swatch--sent" />{{ $t('dashboard.trendSent') }}</span></div>
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
const stats=reactive({pipeline:'legacy',total_batch_campaigns:0,in_progress_campaigns:0,in_progress_batch_campaigns:0,in_progress_realtime_campaigns:0,ended_campaigns:0,today_completed_campaigns:0,today_sent_count:0,total_sms_campaigns:0,realtime_campaigns:0,today_realtime_files:0,today_realtime_event_users:0,today_realtime_matched_users:0,daily_trend:[] as Array<{stats_date:string;sent_count:number;conversion_count:number;matched_count:number;received_file_count:number}>,today_open_exception_count:0,today_recent_exceptions:[] as any[]})
const loading=ref(false),loaded=ref(false),error=ref('')
const { t } = useI18n()
const kpis=computed(()=>[{label:t('dashboard.inProgress'),value:stats.in_progress_campaigns,hint:t('dashboard.batchRealtime',{batch:stats.in_progress_batch_campaigns,realtime:stats.in_progress_realtime_campaigns})},{label:t('dashboard.completed'),value:stats.today_completed_campaigns,hint:t('dashboard.completedToday')},{label:t('dashboard.realtimeMatched'),value:stats.today_realtime_matched_users,hint:t('dashboard.fromFiles',{count:stats.today_realtime_files})},{label:t('dashboard.todaySent'),value:stats.today_sent_count,hint:stats.pipeline==='vnext'?t('dashboard.sentViaSocket'):t('dashboard.deliveredSms')},{label:t('dashboard.openExceptions'),value:stats.today_open_exception_count,hint:stats.today_open_exception_count?t('dashboard.handleSoon'):t('dashboard.healthy')}])
const trendMax=computed(()=>Math.max(1,...stats.daily_trend.map(item=>item.sent_count)))
const trendMatchedMax=computed(()=>Math.max(1,...stats.daily_trend.map(item=>item.matched_count||0)))
const trendFilesMax=computed(()=>Math.max(1,...stats.daily_trend.map(item=>item.received_file_count||0)))
const barHeight=(value:number)=>value?Math.max(8,Math.round(value/trendMax.value*100)):2
const matchedHeight=(value:number)=>value?Math.max(8,Math.round(value/trendMatchedMax.value*100)):2
const filesHeight=(value:number)=>value?Math.max(8,Math.round(value/trendFilesMax.value*100)):2
const formatTrendNumber=(value:number|undefined)=>Number(value||0).toLocaleString()
async function load(){loading.value=true;error.value='';try{Object.assign(stats,(await getDashboardStats()).data);loaded.value=true}catch(e:any){error.value=e.response?.data?.detail||t('common.connectionFailed')}finally{loading.value=false}}
onMounted(load)
</script>
<style scoped>
.stale-alert{margin-bottom:18px}.kpi-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:16px;margin-bottom:16px}.kpi-card,.panel{background:var(--mc-surface);border:1px solid var(--mc-border);border-radius:var(--mc-radius-lg);box-shadow:var(--mc-shadow)}.kpi-card{position:relative;overflow:hidden;padding:21px 22px;transition:transform .2s ease,box-shadow .2s ease}.kpi-card::after{position:absolute;right:0;bottom:0;width:54px;height:4px;background:var(--mc-primary);border-radius:4px 0 0;content:""}.kpi-card:hover{transform:translateY(-2px);box-shadow:var(--mc-shadow-hover)}.kpi-card__label{color:var(--mc-text-secondary);font-size:13px;font-weight:600}.kpi-card strong{display:block;margin:10px 0 6px;color:var(--mc-text);font-size:30px;font-weight:750;line-height:1}.kpi-card span,.panel p{color:var(--mc-text-tertiary);font-size:12px}.dashboard-grid{display:grid;grid-template-columns:minmax(0,1.65fr) minmax(320px,1fr);gap:16px}.panel{padding:21px 22px}.panel__header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px}.panel h3{margin:0 0 5px;color:var(--mc-text);font-size:15px;font-weight:700}.panel p{margin:0}.bar-chart{height:230px;display:flex;align-items:flex-end;gap:12px;padding-top:28px}.bar-chart__item{height:100%;flex:1;display:flex;flex-direction:column;align-items:center;gap:7px;color:var(--mc-text-tertiary);font-size:11px}.bar-chart__track{width:100%;height:100%;display:flex;align-items:flex-end;border-bottom:1px solid var(--mc-border);background:repeating-linear-gradient(to bottom,transparent 0,transparent calc(25% - 1px),#edf2f1 25%)}.bar-chart__bar{width:100%;min-height:2px;background:var(--mc-primary);border-radius:7px 7px 2px 2px;box-shadow:0 7px 16px rgb(201 150 23 / 16%)}.bar-chart__value{font-variant-numeric:tabular-nums}.exception-list{margin-top:14px}.exception-item{width:100%;display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:11px;padding:12px 0;background:none;border:0;border-bottom:1px solid var(--mc-border);text-align:left;color:inherit;cursor:pointer}.exception-item:hover{color:var(--mc-primary-hover)}.exception-item span{min-width:0}.exception-item strong,.exception-item small{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.exception-item small{margin-top:3px;color:var(--mc-text-secondary)}.healthy-empty{min-height:196px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:8px;color:var(--mc-text-secondary)}.healthy-empty .el-icon{font-size:30px;color:var(--mc-success)}.quick-panel{margin-top:16px}.quick-grid{display:flex;flex-wrap:wrap;gap:10px;margin-top:16px}.quick-grid .el-button{margin-left:0}@media(max-width:1100px){.kpi-grid{grid-template-columns:repeat(3,1fr)}}@media(max-width:900px){.kpi-grid{grid-template-columns:repeat(2,1fr)}.dashboard-grid{grid-template-columns:1fr}}@media(max-width:520px){.kpi-grid{grid-template-columns:1fr 1fr;gap:10px}.kpi-card{padding:16px}.kpi-card strong{font-size:24px}.bar-chart{height:180px}.quick-grid .el-button{width:100%}}
.bar-chart__item { position: relative; }
.bar-chart__value { width: 100%; text-align: center; }
.bar-chart__track { justify-content: center; }
.bar-chart__bar { width: min(42%, 24px); }
.trend-bars { display:flex; align-items:flex-end; justify-content:center; gap:3px; width:100%; height:100%; }
.trend-series { display:flex; flex:0 1 18px; width:18px; min-width:0; height:100%; flex-direction:column; align-items:center; gap:4px; }
.trend-series__value { width:100%; overflow:hidden; color:var(--mc-text-secondary); font-size:9px; line-height:1; text-align:center; text-overflow:ellipsis; white-space:nowrap; }
.trend-series__track { display:flex; width:100%; height:100%; align-items:flex-end; justify-content:center; border-bottom:1px solid var(--mc-border); background:repeating-linear-gradient(to bottom,transparent 0,transparent calc(25% - 1px),#edf2f1 25%); }
.trend-series__fill { width:min(100%,18px); min-height:2px; border-radius:5px 5px 0 0; transition:height .25s ease; }
.trend-series__fill--sent { background:var(--mc-primary); box-shadow:0 7px 16px rgb(201 150 23 / 16%); }
.trend-series__fill--matched { background:#3f7ea8; }
.trend-series__fill--files { background:var(--mc-success); }
.trend-legend { display:flex; flex-wrap:wrap; align-items:center; gap:10px 16px; margin-top:12px; color:var(--mc-text-secondary); font-size:11px; }
.trend-legend span { display:inline-flex; align-items:center; gap:5px; white-space:nowrap; }
.trend-legend__swatch { display:inline-block; width:9px; height:9px; border-radius:3px; }
.trend-legend__swatch--sent { background:var(--mc-primary); }
.trend-legend__swatch--matched { background:#3f7ea8; }
.trend-legend__swatch--files { background:var(--mc-success); }
.bar-chart__item { cursor:help; outline:none; }
.bar-chart__item:focus-visible { outline:2px solid var(--mc-primary); outline-offset:3px; border-radius:6px; }
.trend-tooltip { position:absolute; z-index:20; bottom:calc(100% + 8px); left:50%; display:flex; width:190px; flex-direction:column; gap:7px; padding:10px 12px; border:1px solid var(--mc-border); border-radius:8px; background:var(--mc-surface); box-shadow:var(--mc-shadow-hover); color:var(--mc-text-secondary); font-size:11px; line-height:1.2; opacity:0; pointer-events:none; transform:translate(-50%,4px); transition:opacity .16s ease,transform .16s ease,visibility .16s ease; visibility:hidden; }
.trend-tooltip strong { color:var(--mc-text); font-size:12px; }
.trend-tooltip span { display:flex; align-items:center; gap:6px; white-space:nowrap; }
.trend-tooltip span strong { margin-left:auto; font-variant-numeric:tabular-nums; }
.bar-chart__item:hover .trend-tooltip,.bar-chart__item:focus-visible .trend-tooltip { opacity:1; transform:translate(-50%,0); visibility:visible; }
@media(max-width:520px){.trend-bars{gap:2px}.trend-series{flex-basis:14px;width:14px}.trend-tooltip{width:172px}}
</style>
