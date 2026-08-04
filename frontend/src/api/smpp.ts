import api from './index'
import type { SmppConfig, SmppConfigForm } from '@/types/smpp'

export function getSmppConfigs() {
  return api.get<SmppConfig[]>('/smpp/configs')
}

export function getSmppConfig(id: number) {
  return api.get<SmppConfig>(`/smpp/configs/${id}`)
}

export function createSmppConfig(data: SmppConfigForm) {
  return api.post<SmppConfig>('/smpp/configs', data)
}

export function updateSmppConfig(id: number, data: Partial<SmppConfigForm>) {
  return api.put<SmppConfig>(`/smpp/configs/${id}`, data)
}

export function deleteSmppConfig(id: number) {
  return api.delete(`/smpp/configs/${id}`)
}

export function testSmppConnection(data: { config_id?: number; host: string; port: number; system_id: string; password: string; bind_type: string }) {
  return api.post<{ success: boolean; message: string }>('/smpp/test', data)
}
