import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { Campaign, SmsTemplate, TemplatePreview } from '@/types/smsContent'
import * as api from '@/api/smsContent'

export const useSmsContentStore = defineStore('smsContent', () => {
  const campaigns = ref<Campaign[]>([])
  const currentCampaign = ref<Campaign | null>(null)
  const templates = ref<SmsTemplate[]>([])
  const marketingCampaigns = ref<any[]>([])
  const loading = ref(false)
  const preview = ref<TemplatePreview | null>(null)

  async function fetchCampaigns() {
    const res = await api.getCampaigns()
    campaigns.value = res.data
  }

  async function fetchMarketingCampaigns(type: string) {
    const res = await api.getMarketingCampaigns(type)
    marketingCampaigns.value = res.data
  }

  async function selectCampaign(id: number) {
    const res = await api.getCampaign(id)
    currentCampaign.value = res.data
    await fetchTemplates(id)
  }

  async function addCampaign(data: { name: string; description?: string; marketing_type?: string; marketing_campaign_id?: number; language?: string; content?: string }) {
    const res = await api.createCampaign(data)
    await fetchCampaigns()
    return res.data
  }

  async function editCampaign(id: number, data: Partial<Campaign>) {
    const res = await api.updateCampaign(id, data)
    await fetchCampaigns()
    return res.data
  }

  async function removeCampaign(id: number) {
    await api.deleteCampaign(id)
    currentCampaign.value = null
    templates.value = []
    await fetchCampaigns()
  }

  async function fetchTemplates(campaignId: number) {
    const res = await api.getTemplates(campaignId)
    templates.value = res.data
  }

  async function addTemplate(data: { campaign_id: number; language: string; content?: string; source_addr?: string }) {
    const res = await api.createTemplate(data)
    await fetchTemplates(data.campaign_id)
    return res.data
  }

  async function editTemplate(id: number, data: Partial<SmsTemplate>) {
    const res = await api.updateTemplate(id, data)
    if (currentCampaign.value) {
      await fetchTemplates(currentCampaign.value.id)
    }
    return res.data
  }

  async function removeTemplate(id: number) {
    await api.deleteTemplate(id)
    if (currentCampaign.value) {
      await fetchTemplates(currentCampaign.value.id)
    }
  }

  async function doPreview(content: string, variables: Record<string, string> = {}) {
    const res = await api.previewTemplate(content, variables)
    preview.value = res.data
    return res.data
  }

  return {
    campaigns, currentCampaign, templates, marketingCampaigns, loading, preview,
    fetchCampaigns, fetchMarketingCampaigns, selectCampaign, addCampaign, editCampaign, removeCampaign,
    fetchTemplates, addTemplate, editTemplate, removeTemplate, doPreview,
  }
})
