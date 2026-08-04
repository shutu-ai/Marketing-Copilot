import api from './index'
import type { Campaign, SmsTemplate, TemplatePreview } from '@/types/smsContent'

export function getMarketingCampaigns(type: string) {
  return api.get<any[]>('/sms-content/marketing-campaigns', { params: { type } })
}

export function getCampaigns() {
  return api.get<Campaign[]>('/sms-content/campaigns')
}

export function getCampaign(id: number) {
  return api.get<Campaign>(`/sms-content/campaigns/${id}`)
}

export function getCampaignByMarketing(type: string, marketingCampaignId: number) {
  return api.get<Campaign | null>('/sms-content/campaigns/by-marketing', {
    params: { type, marketing_campaign_id: marketingCampaignId }
  })
}

export function createCampaign(data: { name: string; description?: string; marketing_type?: string; marketing_campaign_id?: number; language?: string; content?: string }) {
  return api.post<Campaign>('/sms-content/campaigns', data)
}

export function updateCampaign(id: number, data: Partial<Campaign>) {
  return api.put<Campaign>(`/sms-content/campaigns/${id}`, data)
}

export function deleteCampaign(id: number) {
  return api.delete(`/sms-content/campaigns/${id}`)
}

export function getTemplatesPage(params: { page?: number; page_size?: number } = {}) {
  return api.get<any>('/sms-content/templates', { params })
}

export function getAllTemplates() {
  return api.get<any[]>('/sms-content/templates/all')
}

export function getTemplatesForSelect() {
  return api.get<any[]>('/sms-content/templates-for-select')
}

export function getTemplates(campaignId: number) {
  return api.get<SmsTemplate[]>(`/sms-content/campaigns/${campaignId}/templates`)
}

export function getCampaignVersions(campaignId: number, params: { page?: number; page_size?: number } = {}) {
  return api.get<any>(`/sms-content/campaigns/${campaignId}/versions`, { params })
}

export function createTemplate(data: { campaign_id: number; language: string; content?: string; source_addr?: string }) {
  return api.post<SmsTemplate>('/sms-content/templates', data)
}

export function updateTemplate(id: number, data: Partial<SmsTemplate>) {
  return api.put<SmsTemplate>(`/sms-content/templates/${id}`, data)
}

export function deleteTemplate(id: number) {
  return api.delete(`/sms-content/templates/${id}`)
}

export function previewTemplate(content: string, variables: Record<string, string> = {}) {
  return api.post<TemplatePreview>('/sms-content/preview', null, {
    params: { content, variables: JSON.stringify(variables) }
  })
}

export function validateTemplate(content: string, available_variables: string[] = []) {
  return api.post('/sms-content/templates/validate', { content, available_variables })
}

export function getTemplateVersions(id: number, params: { page?: number; page_size?: number } = {}) {
  return api.get<any>(`/sms-content/templates/${id}/versions`, { params })
}

export function restoreTemplateVersion(id: number, version_no: number) {
  return api.post(`/sms-content/templates/${id}/restore`, { version_no })
}
