import api from './index'

export interface RealtimeStats {
  file_count: number
  total_msisdn_sum: number
  total_matched: number
  task_created_users: number
  accepted_users: number
  delivered_users: number
  today_file_count: number
  today_msisdn_sum: number
  today_matched: number
  today_task_created_users: number
  today_accepted_users: number
  today_delivered_users: number
  today_reached_users: number
}

export function getRealtimeStats(campaignId: number) {
  return api.get<RealtimeStats>(`/realtime-campaigns/${campaignId}/stats`)
}

export function getRealtimeFiles(campaignId: number, params: { page?: number; page_size?: number } = {}) {
  return api.get<any>(`/realtime-campaigns/${campaignId}/files`, { params })
}

export function getRealtimeWatchFiles(campaignId: number, params: {
  page?: number
  page_size?: number
  extension?: string
  status?: string
  reason?: string
} = {}) {
  return api.get<any>(`/realtime-campaigns/${campaignId}/watch-files`, { params })
}

export function getRealtimeSentRecords(campaignId: number, params: { page?: number; page_size?: number } = {}) {
  return api.get<any>(`/realtime-campaigns/${campaignId}/sent-records`, { params })
}

export function downloadRealtimeSentRecord(campaignId: number, filename: string) {
  return api.get<Blob>(`/realtime-campaigns/${campaignId}/sent-records/${encodeURIComponent(filename)}`, {
    responseType: 'blob',
  })
}

export function receiveRealtimeFile(campaignId: number, file: File) {
  const form = new FormData()
  form.append('file', file)
  return api.post<{
    success: boolean; filename?: string; total_msisdn?: number; matched_count?: number; duplicate_file?: boolean
  }>(`/realtime-campaigns/${campaignId}/receive-file`, form)
}
