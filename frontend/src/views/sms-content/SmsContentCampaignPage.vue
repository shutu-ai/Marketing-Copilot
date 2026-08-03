<template>
  <div class="sms-content-campaign-page">
    <div class="page-header">
      <div style="display:flex;align-items:center;gap:12px">
        <el-button text @click="goBack">
          <el-icon><ArrowLeft /></el-icon>
        </el-button>
        <h2>{{ pageTitle }}</h2>
      </div>
      <el-button type="primary" :loading="saving" @click="handleSave">{{ $t('common.save') }}</el-button>
    </div>

    <el-card style="margin-bottom:16px">
      <el-form :model="form" label-width="130px" size="small">
        <el-form-item :label="$t('smsContent.marketingType')" required>
          <template v-if="isEdit || isFromMarketing">
            <el-input :model-value="form.marketing_type === 'realtime' ? '实时营销' : '批量营销'" disabled />
          </template>
          <template v-else>
            <el-select v-model="form.marketing_type" style="width:100%" @change="onMarketingTypeChange">
              <el-option label="批量营销" value="batch" />
              <el-option label="实时营销" value="realtime" />
            </el-select>
          </template>
        </el-form-item>
        <el-form-item :label="$t('smsContent.marketingCampaign')" required>
          <template v-if="isEdit || isFromMarketing">
            <el-input :model-value="campaignName" disabled />
          </template>
          <template v-else>
            <el-select v-model="form.marketing_campaign_id" style="width:100%" filterable :loading="loadingCampaigns">
              <el-option v-for="mc in store.marketingCampaigns" :key="mc.id" :label="mc.name" :value="mc.id" />
            </el-select>
          </template>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card>
      <template #header>
        <div style="display:flex;align-items:center;justify-content:space-between">
          <span>{{ $t('smsContent.template') }}</span>
          <el-button size="small" :disabled="rows.length >= languageOptions.length" @click="addRow">
            <el-icon><Plus /></el-icon>
            {{ $t('common.add') }}
          </el-button>
        </div>
      </template>

      <div v-for="(row, idx) in rows" :key="idx" style="border:1px solid #e4e7ed;border-radius:6px;padding:16px;margin-bottom:12px">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
          <strong style="font-size:14px">{{ $t('common.language') }} {{ idx + 1 }}</strong>
          <el-button v-if="rows.length > 1" text type="danger" size="small" @click="rows.splice(idx, 1)">
            <el-icon><Delete /></el-icon>
          </el-button>
        </div>
        <el-form label-width="100px" size="small">
          <el-form-item :label="$t('common.language')" required>
            <el-select v-model="row.language" style="width:100%" placeholder="请选择语言">
              <el-option
                v-for="language in availableLanguages(idx)"
                :key="language"
                :label="language"
                :value="language"
              />
            </el-select>
          </el-form-item>
          <el-form-item :label="$t('smsContent.content')" required>
            <el-input v-model="row.content" type="textarea" :rows="4" />
          </el-form-item>
        </el-form>
      </div>

      <el-empty v-if="rows.length === 0" :description="$t('common.noData')" />
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus'
import { useSmsContentStore } from '@/stores/smsContent'
import * as api from '@/api/smsContent'
import * as batchApi from '@/api/campaign'
import * as realtimeApi from '@/api/realtime-campaign'

const route = useRoute()
const router = useRouter()
const store = useSmsContentStore()
const { t } = useI18n()

const isEdit = computed(() => !!route.params.campaignId && route.params.campaignId !== 'new')
const isFromMarketing = computed(() => !!route.query.from)
const pageTitle = computed(() => {
  if (isEdit.value || smsContentCampaignId.value) return t('common.edit')
  return isFromMarketing.value ? t('smsContent.configSms') : t('smsContent.addCampaign')
})
const saving = ref(false)
const loadingCampaigns = ref(false)
const campaignName = ref('')
const smsContentCampaignId = ref<number | null>(null)

const form = reactive({
  marketing_type: 'batch',
  marketing_campaign_id: 0 as number | null,
})

interface Row {
  template_id?: number
  language: string
  content: string
}

const rows = ref<Row[]>([])
const languageOptions = ['Amharic', 'Tigrigna', 'Somali', 'English', 'Oromign']
const originalTemplateIds = ref<number[]>([])

function goBack() {
  if (isFromMarketing.value) {
    const from = route.query.from as string
    router.push(from === 'realtime' ? '/campaign/realtime' : '/campaign/batch')
  } else {
    router.push('/sms-content')
  }
}

function addRow() {
  rows.value.push({ language: '', content: '' })
}

function availableLanguages(rowIndex: number) {
  const selectedByOtherRows = new Set(
    rows.value.filter((_, index) => index !== rowIndex).map(row => row.language).filter(Boolean)
  )
  return languageOptions.filter(language => !selectedByOtherRows.has(language))
}

async function onMarketingTypeChange(type: string) {
  form.marketing_campaign_id = null
  loadingCampaigns.value = true
  await store.fetchMarketingCampaigns(type)
  loadingCampaigns.value = false
}

async function loadSmsContentCampaign(campaignId: number) {
  const res = await api.getTemplates(campaignId)
  const campaignRes = await api.getCampaign(campaignId)
  const campaign = campaignRes.data
  smsContentCampaignId.value = campaignId
  form.marketing_type = campaign.marketing_type || 'batch'
  form.marketing_campaign_id = campaign.marketing_campaign_id
  campaignName.value = campaign.name || ''
  originalTemplateIds.value = []
  rows.value = (res.data || []).map((template: any) => {
    originalTemplateIds.value.push(template.id)
    return {
      template_id: template.id,
      language: template.language,
      content: template.content || '',
    }
  })
}

async function handleSave() {
  if (!form.marketing_campaign_id && !isEdit.value) {
    ElMessage.warning('请选择营销活动')
    return
  }
  const validRows = rows.value.filter(row => row.language && row.content?.trim())
  if (validRows.length === 0) {
    ElMessage.warning('请至少添加一条语言内容')
    return
  }
  if (new Set(validRows.map(row => row.language)).size !== validRows.length) {
    ElMessage.warning('同一种语言只能配置一次')
    return
  }

  saving.value = true
  const editingCampaignId = smsContentCampaignId.value || (isEdit.value ? Number(route.params.campaignId) : null)
  try {
    let firstTemplateId: number | null = null

    if (editingCampaignId) {
      const validIds = new Set<number>()
      for (const row of validRows) {
        if (row.template_id) {
          await api.updateTemplate(row.template_id, { language: row.language, content: row.content })
          if (!firstTemplateId) firstTemplateId = row.template_id
          validIds.add(row.template_id)
        } else {
          const res = await api.createTemplate({ campaign_id: editingCampaignId, language: row.language, content: row.content })
          if (!firstTemplateId) firstTemplateId = res.data.id
          validIds.add(res.data.id)
        }
      }
      for (const tid of originalTemplateIds.value) {
        if (!validIds.has(tid)) {
          await api.deleteTemplate(tid)
        }
      }
    } else {
      const nameVal = isFromMarketing.value
        ? (route.query.campaignName as string) || ''
        : store.marketingCampaigns.find((m: any) => m.id === form.marketing_campaign_id)?.name || ''
      const campaign = await api.createCampaign({
        name: nameVal,
        marketing_type: form.marketing_type,
        marketing_campaign_id: form.marketing_campaign_id || 0,
      })
      smsContentCampaignId.value = campaign.data.id
      for (const row of validRows) {
        const res = await api.createTemplate({ campaign_id: campaign.data.id, language: row.language, content: row.content })
        if (!firstTemplateId) firstTemplateId = res.data.id
      }
    }

    if (isFromMarketing.value && firstTemplateId) {
      const mkId = Number(route.query.campaignId)
      if (route.query.from === 'realtime') {
        await realtimeApi.updateRealtimeCampaign(mkId, { sms_template_id: firstTemplateId } as any)
      } else {
        await batchApi.updateCampaign(mkId, { sms_template_id: firstTemplateId } as any)
      }
    }

    ElMessage.success(editingCampaignId ? '更新成功' : '新增成功')
    goBack()
  } catch (e: any) {
    const msg = e?.response?.data?.detail || e?.message || '保存失败'
    ElMessage.error(msg)
  } finally {
    saving.value = false
  }
}

onMounted(async () => {
  if (isEdit.value) {
    await loadSmsContentCampaign(Number(route.params.campaignId))
  } else if (isFromMarketing.value) {
    const mkId = Number(route.query.campaignId)
    const mkName = (route.query.campaignName as string) || ''
    const from = route.query.from as string
    form.marketing_type = from === 'realtime' ? 'realtime' : 'batch'
    form.marketing_campaign_id = mkId
    campaignName.value = mkName
    const existing = await api.getCampaignByMarketing(form.marketing_type, mkId)
    if (existing.data?.id) {
      await loadSmsContentCampaign(existing.data.id)
      campaignName.value = mkName
    } else if (route.query.smsTemplateId) {
      const templateId = Number(route.query.smsTemplateId)
      const templates = await api.getAllTemplates()
      const linked = templates.data.find((item: any) => item.template_id === templateId)
      if (linked?.campaign_id) {
        await loadSmsContentCampaign(linked.campaign_id)
        campaignName.value = mkName
      }
    }
  } else {
    form.marketing_type = 'batch'
    form.marketing_campaign_id = null
    await onMarketingTypeChange('batch')
  }
})
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
