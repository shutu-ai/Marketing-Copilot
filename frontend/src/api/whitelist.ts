import api from './index'
import type { UploadHistory, PreviewResult, ImportResult, WhitelistUser } from '@/types/whitelist'

export function getUploadHistory() {
  return api.get<UploadHistory[]>('/whitelist/history')
}

export function previewFile(file: File) {
  const form = new FormData()
  form.append('file', file)
  return api.post<PreviewResult>('/whitelist/preview', form)
}

export function importFile(filePath: string, importMode: string = 'append') {
  const form = new FormData()
  form.append('file_path', filePath)
  form.append('import_mode', importMode)
  return api.post<ImportResult>('/whitelist/import', form)
}

export function getWhitelistUsers(uploadId?: number) {
  return api.get<WhitelistUser[]>('/whitelist/users', { params: { upload_id: uploadId } })
}
