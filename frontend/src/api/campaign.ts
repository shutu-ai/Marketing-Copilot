import api from './index'

export interface BatchCampaign {
  id: number
  name: string
  start_time: string
  end_time: string
  status: string
  send_start_time: string
  send_end_time: string
  send_days: string[]
  batch_size: number
  customer_list_filename: string
  customer_list_path: string
  row_count: number
  campaign_dir: string
  use_custom_directory: boolean
  sms_template_id: number | null
  smpp_config_id: number | null
  phone_prefix: string
  created_at: string
}

export interface CampaignListResult {
  total: number
  page: number
  page_size: number
  items: BatchCampaign[]
}

export interface BatchCampaignUploadResult extends BatchCampaign {
  ignored_rows?: number
  ignored_msisdn_rows?: number
  ignored_malformed_rows?: number
}

export function getCampaigns(params: { page?: number; page_size?: number } = {}) {
  return api.get<CampaignListResult>('/batch-campaigns', { params })
}

export function getCampaign(id: number) {
  return api.get<BatchCampaign>(`/batch-campaigns/${id}`)
}

export function createCampaign(data: {
  name: string; start_time?: string; end_time?: string;
  send_start_time?: string; send_end_time?: string; send_days?: string[];
  batch_size?: number; campaign_dir?: string; use_custom_directory?: boolean;
  smpp_config_id?: number; phone_prefix?: string;
}) {
  return api.post<BatchCampaign>('/batch-campaigns', data)
}

export function updateCampaign(id: number, data: Partial<BatchCampaign>) {
  return api.put<BatchCampaign>(`/batch-campaigns/${id}`, data)
}

export function deleteCampaign(id: number) {
  return api.delete(`/batch-campaigns/${id}`)
}

export function uploadCustomerList(campaignId: number, file: File) {
  const form = new FormData()
  form.append('file', file)
  return api.post<BatchCampaignUploadResult>(`/batch-campaigns/${campaignId}/upload`, form)
}

export function copyCampaign(id: number) {
  return api.post<BatchCampaign>(`/batch-campaigns/${id}/copy`)
}

export function getCampaignDetail(id: number) {
  return api.get<any>(`/batch-campaigns/${id}/detail`)
}

export function generateBatches(campaignId: number, batchSize: number) {
  return api.post(`/batch-campaigns/${campaignId}/generate-batches`, null, { params: { batch_size: batchSize } })
}

export function processBatch(batchId: number) {
  return api.post(`/batch-campaigns/batches/${batchId}/process`)
}

export interface ProspectQuery {
  search?: string
  field?: string
  value?: string
  page?: number
  page_size?: number
}

export interface ProspectResult {
  headers: string[]
  rows: Record<string, string>[]
  total: number
  page: number
  page_size: number
}

export function getProspects(campaignId: number, params: ProspectQuery = {}) {
  return api.get<ProspectResult>(`/batch-campaigns/${campaignId}/prospects`, { params })
}
