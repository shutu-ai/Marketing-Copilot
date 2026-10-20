import api from './index'

export function getSmsTasks(params: { page?: number; page_size?: number; cursor?: number; status?: string; campaign_type?: string; smpp_config_id?: number; msisdn?: string }) {
  return api.get<any>('/sms-tasks', { params })
}

export function getSmsTaskStats() {
  return api.get<Record<string, number>>('/sms-tasks/stats')
}

export function retrySmsTask(id: number) {
  return api.post(`/sms-tasks/${id}/retry`)
}

export function confirmDelivered(id: number) {
  return api.post(`/sms-tasks/${id}/deliver`)
}

export function getExclusions(params: { page?: number; page_size?: number } = {}) {
  return api.get<any>('/sms-tasks/exclusions/list', { params })
}
export function addExclusion(msisdn: string, reason: string) {
  return api.post('/sms-tasks/exclusions', { msisdn, reason })
}
export function deleteExclusion(id: number) { return api.delete(`/sms-tasks/exclusions/${id}`) }
