import api from './index'

export interface RealtimeCampaign {
  id: number
  name: string
  use_whitelist: boolean
  start_time: string
  end_time: string
  send_start_time: string
  send_end_time: string
  send_days: string[]
  status: string
  customer_list_filename: string
  customer_list_path: string
  row_count: number
  campaign_dir: string
  use_custom_directory: boolean
  watch_source: 'local' | 'sftp'
  watch_dir: string
  sftp_host: string
  sftp_port: number
  sftp_username: string
  sftp_password: string
 watch_filename_prefix: string
 phone_prefix: string
 sms_template_id: number | null
  smpp_config_id: number | null
  flash_sms: boolean
  created_at: string
}

export interface RealtimeCampaignListResult {
  total: number
  page: number
  page_size: number
  items: RealtimeCampaign[]
}

export function getRealtimeCampaigns(params: { page?: number; page_size?: number } = {}) {
  return api.get<RealtimeCampaignListResult>('/realtime-campaigns', { params })
}

export function getRealtimeCampaign(id: number) {
  return api.get<RealtimeCampaign>(`/realtime-campaigns/${id}`)
}

export function createRealtimeCampaign(data: {
  name: string; use_whitelist?: boolean; start_time?: string; end_time?: string;
  send_start_time?: string; send_end_time?: string; send_days?: string[];
  campaign_dir?: string; use_custom_directory?: boolean;
  watch_source?: 'local' | 'sftp'; watch_dir?: string;
  sftp_host?: string; sftp_port?: number; sftp_username?: string; sftp_password?: string;
 watch_filename_prefix?: string;
 phone_prefix?: string;
 sms_template_id?: number;
  smpp_config_id?: number; flash_sms?: boolean;
}) {
  return api.post<RealtimeCampaign>('/realtime-campaigns', data)
}

export function updateRealtimeCampaign(id: number, data: Partial<RealtimeCampaign>) {
  return api.put<RealtimeCampaign>(`/realtime-campaigns/${id}`, data)
}

export function deleteRealtimeCampaign(id: number) {
  return api.delete(`/realtime-campaigns/${id}`)
}

export function uploadRealtimeCustomerList(campaignId: number, file: File) {
  const form = new FormData()
  form.append('file', file)
  return api.post<RealtimeCampaign>(`/realtime-campaigns/${campaignId}/upload`, form, {
    timeout: 600000, // 10 minutes for large files
    onUploadProgress: (progressEvent) => {
      // stub — caller overrides via request interceptor or config
    },
  })
}

export function uploadRealtimeCustomerListWithProgress(
  campaignId: number,
  file: File,
  onUploadProgress: (percent: number) => void,
) {
  const form = new FormData()
  form.append('file', file)
  return api.post<RealtimeCampaign>(`/realtime-campaigns/${campaignId}/upload`, form, {
    timeout: 600000,
    onUploadProgress: (progressEvent) => {
      if (progressEvent.total) {
        const percent = Math.round((progressEvent.loaded * 100) / progressEvent.total)
        onUploadProgress(percent)
      }
    },
  })
}

export function copyRealtimeCampaign(id: number) {
  return api.post<RealtimeCampaign>(`/realtime-campaigns/${id}/copy`)
}

export function getRealtimeCampaignDetail(id: number) {
  return api.get<any>(`/realtime-campaigns/${id}/detail`)
}

export function testRealtimeSftpConnection(data: {
  campaign_id?: number;
  watch_dir: string; sftp_host: string; sftp_port: number;
  sftp_username: string; sftp_password?: string
}) {
  return api.post<{ success: boolean; directory: string; entry_count: number }>(
    '/realtime-campaigns/test-sftp', data, { timeout: 30000 },
  )
}
