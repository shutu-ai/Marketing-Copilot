import api from './index'

export interface RealtimeStats {
  file_count: number
  total_msisdn_sum: number
  total_matched: number
}

export function getRealtimeStats(campaignId: number) {
  return api.get<RealtimeStats>(`/realtime-campaigns/${campaignId}/stats`)
}

export function getRealtimeFiles(campaignId: number) {
  return api.get<any[]>(`/realtime-campaigns/${campaignId}/files`)
}

export function receiveRealtimeFile(campaignId: number, file: File) {
  const form = new FormData()
  form.append('file', file)
  return api.post<{
    success: boolean; filename: string; total_msisdn: number; matched_count: number
  }>(`/realtime-campaigns/${campaignId}/receive-file`, form)
}
