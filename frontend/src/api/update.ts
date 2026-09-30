import api from './index'

export interface UploadStartPayload {
  upload_id?: string
  filename: string
  size: number
  sha256: string
  chunks: number
}

export interface UpdateStatus {
  version: string
  release: Record<string, any>
  mode: string
  state: Record<string, any> | null
  has_backups: boolean
  active_business: {
    batch_in_progress: number
    realtime_in_progress: number
    sending_tasks: number
    pending_tasks: number
  }
}

export interface UpdateHistoryRow {
  id: number
  run_id: string
  action: string
  version: string
  previous_version: string
  status: string
  operator: string
  detail: string
  created_at: string
}

export interface RecentUpload {
  upload_id: string
  filename: string
  size: number
  complete: boolean
  verified: boolean
  version: string
  precheck: { ok: boolean; version: string; checks: any[]; errors: string[]; verified_at: string }
  created_at: string
}

export function getUpdateStatus() {
  return api.get<UpdateStatus>('/update/status')
}

export function getRecentUploads(limit = 5) {
  return api.get<RecentUpload[]>('/update/uploads', { params: { limit } })
}

export function startUpload(payload: UploadStartPayload) {
  return api.post<{ upload_id: string; filename: string }>('/update/upload/start', payload)
}

export function uploadChunk(
  uploadId: string,
  chunk: number,
  blob: Blob,
  onProgress?: (loaded: number, total: number) => void,
) {
  return api.post('/update/upload/chunk', blob, {
    params: { upload_id: uploadId, chunk },
    headers: { 'Content-Type': 'application/octet-stream' },
    timeout: 600000,
    onUploadProgress: (event) => {
      onProgress?.(event.loaded, event.total || blob.size)
    },
  })
}

export function verifyUpload(uploadId: string) {
  return api.post<{ ok: boolean; version: string; checks: any[]; errors: string[] }>('/update/verify', null, {
    params: { upload_id: uploadId },
    timeout: 600000,
  })
}

export function applyUpdate(uploadId: string, force: boolean) {
  return api.post('/update/apply', null, { params: { upload_id: uploadId, force } })
}

export function rollbackUpdate(force: boolean) {
  return api.post('/update/rollback', null, { params: { force } })
}

export function cancelUpload(uploadId: string) {
  return api.post('/update/cancel', null, { params: { upload_id: uploadId } })
}

export function getUpdateHistory(limit = 50) {
  return api.get<UpdateHistoryRow[]>('/update/history', { params: { limit } })
}
