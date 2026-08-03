import api from './index'
import type { MonitorConfig, MonitorFile } from '@/types/monitor'

export interface RealtimeMonitorState {
  campaign_id?: number
  watch_dir: string
  watch_dirs: any[]
  campaigns: any[]
  exists: boolean
  active_campaigns: number
  source_files: any[]
  processed_files: any[]
}

export function getRealtimeMonitor(campaignId?: number) {
  return api.get<RealtimeMonitorState>('/monitor/realtime', {
    params: campaignId ? { campaign_id: campaignId } : undefined,
  })
}

export function scanRealtimeMonitor(campaignId?: number) {
  return api.post<{ sync: any; processing: any; monitor: RealtimeMonitorState }>('/monitor/realtime/scan', null, {
    params: campaignId ? { campaign_id: campaignId } : undefined,
  })
}

export function getMonitorConfigs() {
  return api.get<MonitorConfig[]>('/monitor/configs')
}

export function addMonitorConfig(directoryPath: string, targetDirectories?: string[]) {
  return api.post<MonitorConfig>('/monitor/configs', { directory_path: directoryPath, target_directories: targetDirectories || [] })
}

export function updateMonitorConfig(configId: number, data: Partial<MonitorConfig>) {
  return api.put<MonitorConfig>(`/monitor/configs/${configId}`, data)
}

export function toggleMonitorConfig(configId: number, enabled: number) {
  return api.put<MonitorConfig>(`/monitor/configs/${configId}/toggle`, null, { params: { enabled } })
}

export function deleteMonitorConfig(configId: number) {
  return api.delete(`/monitor/configs/${configId}`)
}

export function scanDirectory(configId: number) {
  return api.post(`/monitor/configs/${configId}/scan`)
}

export function getMonitorFiles(status?: string, directoryPath?: string) {
  return api.get<MonitorFile[]>('/monitor/files', { params: { status, directory_path: directoryPath } })
}

export function syncSourceFiles() {
  return api.post<{ updated: number }>('/monitor/sync-source')
}

export function updateFileStatus(fileId: number, status: string) {
  return api.put(`/monitor/files/${fileId}/status`, null, { params: { status } })
}

export function distributeFile(fileId: number) {
  return api.post<{ success: boolean; message: string }>(`/monitor/files/${fileId}/distribute`)
}

export function publishFile(fileId: number, campaignName?: string) {
  return api.post<{ success: boolean; published: number; total_rows: number; routing_key: string; campaign: string }>(
    '/monitor/files/publish', { file_id: fileId, campaign_name: campaignName || '' }
  )
}
