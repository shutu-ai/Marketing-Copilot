import api from './index'

export interface AppSetting {
  id: number
  key: string
  value: string
}

export function getSettings() {
  return api.get<AppSetting[]>('/settings')
}

export function getSetting(key: string) {
  return api.get<{ key: string; value: string }>(`/settings/${encodeURIComponent(key)}`)
}

export function updateSetting(key: string, value: string) {
  return api.put<{ key: string; value: string }>(`/settings/${encodeURIComponent(key)}`, { value })
}
