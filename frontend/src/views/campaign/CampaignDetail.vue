<template>
  <div class="campaign-detail">
    <div class="page-header">
      <el-button text @click="$router.back()">
        <el-icon><ArrowLeft /></el-icon> {{ $t('common.back') }}
      </el-button>
      <h2>{{ campaign?.name || $t('campaign.batchDetailTitle') }}</h2>
    </div>

    <el-collapse v-model="basicInfoActive" style="margin-bottom:12px">
      <el-collapse-item :title="$t('campaign.basicInfo')" name="basic">
        <el-descriptions :column="2" border size="small" v-if="campaign">
          <el-descriptions-item :label="$t('campaign.name')" :span="2">{{ campaign.name }}</el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.startTime')">{{ campaign.start_time || '-' }}</el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.endTime')">{{ campaign.end_time || '-' }}</el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.sendStartTime')">{{ campaign.send_start_time || '-' }}</el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.sendEndTime')">{{ campaign.send_end_time || '-' }}</el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.status')">
            <StatusTag :status="campaign.status">{{ $t('campaign.status_' + campaign.status) }}</StatusTag>
          </el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.customerList')">
            {{ campaign.customer_list_filename || '-' }}
            <el-tag v-if="campaign.row_count" size="small" type="info" style="margin-left: 4px">{{ campaign.row_count }} rows</el-tag>
          </el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.totalProspects')">{{ campaign.row_count || 0 }}</el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.batchSize')">{{ campaign.batch_size || 500 }}</el-descriptions-item>
          <el-descriptions-item :label="$t('smsContent.template')" :span="2">
            <template v-if="campaign.sms_templates?.length">
              <div v-for="tpl in campaign.sms_templates" :key="tpl.id" style="margin-bottom:6px;padding-bottom:6px;border-bottom:1px solid #eee">
                <el-tag size="small" style="margin-right:6px">{{ tpl.language }}</el-tag>
                <span style="white-space:pre-wrap">{{ tpl.content }}</span>
              </div>
            </template>
            <span v-else-if="campaign.sms_template_id" style="color:#999">{{ $t('legacy.xe2172505') }} #{{ campaign.sms_template_id }}</span>
            <span v-else style="color:#ccc">{{ $t('legacy.x71dc8feb') }}</span>
          </el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.sendDays')">
            <template v-if="campaign.send_days?.length">
              <el-tag v-for="d in campaign.send_days" :key="d" size="small" style="margin: 1px">
                {{ $t('campaign.day_' + d) }}
              </el-tag>
            </template>
            <span v-else>-</span>
          </el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.dirRoot')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir || '-' }}</code></el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.dirWhitelist')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\whitelist\\') : '-' }}</code></el-descriptions-item>
          <el-descriptions-item :label="$t('campaign.dirOutput')" :span="2"><code style="font-size: 12px">{{ campaign?.campaign_dir ? (campaign.campaign_dir + '\\output\\') : '-' }}</code></el-descriptions-item>
        </el-descriptions>
      </el-collapse-item>
    </el-collapse>

    <el-card v-if="campaign" shadow="never">
      <el-tabs v-model="detailTab">
        <el-tab-pane :label="$t('campaign.customerList')" name="prospects">
          <template v-if="campaign?.customer_list_path">
            <div style="display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap;margin-bottom:12px">
              <el-select v-model="filterField" clearable :placeholder="$t('legacy.x9caecd93')" size="small" style="width:100px" @change="filterValue = ''">
                <el-option v-for="h in visibleProspectHeaders" :key="h" :label="h" :value="h" />
              </el-select>
              <el-input v-if="filterField" v-model="filterValue" :placeholder="$t('legacy.xe125986c')" size="small" style="width:130px" clearable @input="handleFilter" />
              <el-input v-model="searchText" :placeholder="$t('legacy.x681c0f39')" size="small" style="width:180px" clearable @input="handleFilter">
                <template #prefix><el-icon><Search /></el-icon></template>
              </el-input>
            </div>
            <el-empty v-if="!prospectLoading && prospectTotal === 0" :description="searchText || filterField ? i18n.global.t('legacy.xa32c4660') : i18n.global.t('common.noData')" />
            <template v-else>
              <el-table ref="prospectTableRef" :data="prospectRows" stripe border size="small" max-height="500" v-loading="prospectLoading">
                <el-table-column v-for="h in visibleProspectHeaders" :key="h" :prop="h" :label="h" min-width="120" />
              </el-table>
              <div style="display:flex;justify-content:space-between;align-items:center;margin-top:12px">
                <span style="font-size:13px;color:#909399">{{ $t('legacy.xfbd2b1fb') }} {{ prospectTotal }}</span>
                <el-pagination
                  v-model:current-page="prospectPage"
                  :page-size="prospectPageSize"
                  :total="prospectTotal"
                  layout="prev, pager, next"
                  small
                  @current-change="loadProspects"
                />
              </div>
            </template>
          </template>
          <el-empty v-else :description="$t('legacy.xbebe25ea')" />
        </el-tab-pane>
        <el-tab-pane :label="$t('legacy.xe7502eb1')" name="sent">
          <el-table ref="sentTableRef" v-if="(campaign.daily_stats || []).length" :data="pagedStats" stripe border size="small">
            <el-table-column prop="stats_date" :label="$t('campaign.date')" width="140" />
            <el-table-column prop="total_prospects" :label="$t('campaign.totalProspects')" width="120" />
            <el-table-column prop="sent_count" :label="$t('campaign.sentUsers')" width="120" />
            <el-table-column prop="smpp_label" label="SMPP" min-width="180" />
          </el-table>
          <el-empty v-else :description="$t('legacy.x4c105d20')" />
          <div v-if="(campaign.daily_stats || []).length" style="display:flex;justify-content:flex-end;margin-top:12px">
            <el-pagination
              v-model:current-page="sentPage"
              :page-size="sentPageSize"
              :total="(campaign.daily_stats || []).length"
              layout="total, prev, pager, next"
              small
            />
          </div>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import i18n from '@/i18n'
import { ref, computed, onMounted, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import StatusTag from '@/components/StatusTag.vue'
import * as api from '@/api/campaign'
import type { ProspectResult } from '@/api/campaign'

const route = useRoute()
const campaign = ref<any>(null)
const basicInfoActive = ref('basic')
const prospectHeaders = ref<string[]>([])
const prospectRows = ref<Record<string, string>[]>([])
const hiddenProspectFields = new Set(['cell_id', 'customer_name', 'offer_code', 'event_filter_id'])
const visibleProspectHeaders = computed(() => prospectHeaders.value.filter(h => !hiddenProspectFields.has(h)))
const prospectTotal = ref(0)
const prospectPage = ref(1)
const prospectPageSize = ref(50)
const prospectLoading = ref(false)
const searchText = ref('')
const filterField = ref('')
const filterValue = ref('')
const sentPage = ref(1)
const sentPageSize = ref(10)
const detailTab = ref('prospects')
const prospectTableRef = ref<any>(null)
const sentTableRef = ref<any>(null)

const pagedStats = computed(() => {
  const all = campaign.value?.daily_stats || []
  const start = (sentPage.value - 1) * sentPageSize.value
  return all.slice(start, start + sentPageSize.value)
})

let filterTimer: ReturnType<typeof setTimeout> | null = null

async function loadDetail() {
  const id = Number(route.params.id)
  try {
    const res = await api.getCampaignDetail(id)
    campaign.value = res.data
    sentPage.value = 1
    await nextTick()
    sentTableRef.value?.doLayout?.()
    if (campaign.value?.customer_list_path) {
      loadProspects()
    }
  } catch (e) {
    console.error(e)
  }
}

async function loadProspects() {
  const id = Number(route.params.id)
  prospectLoading.value = true
  try {
    const params: api.ProspectQuery = {
      page: prospectPage.value,
      page_size: prospectPageSize.value,
    }
    if (searchText.value) params.search = searchText.value
    if (filterField.value && filterValue.value) {
      params.field = filterField.value
      params.value = filterValue.value
    }
    const res = await api.getProspects(id, params)
    const data: ProspectResult = res.data
    prospectHeaders.value = data.headers
    prospectRows.value = data.rows
    prospectTotal.value = data.total
    prospectPage.value = data.page
  } finally {
    prospectLoading.value = false
    await nextTick()
    prospectTableRef.value?.doLayout?.()
  }
}

function handleFilter() {
  if (filterTimer) clearTimeout(filterTimer)
  filterTimer = setTimeout(() => {
    prospectPage.value = 1
    loadProspects()
  }, 300)
}

onMounted(loadDetail)
</script>

<style scoped>
.page-header {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 12px;
}
.page-header h2 { margin: 0; font-size: 18px; font-weight: 600; }
</style>
