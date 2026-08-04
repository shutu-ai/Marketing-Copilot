import api from './index'

export interface LogFile {
  name: string
  path: string
  size: number
  modified: number
}

export interface LogEntry {
  time: string
  level: string
  logger: string
  message: string
  line: number
}

export interface LogEntriesResult {
  total: number
  page: number
  page_size: number
  items: LogEntry[]
  truncated: boolean
}

export function getLogFiles() {
  return api.get<{ dir: string; items: LogFile[] }>('/logs/files')
}

export function getLogEntries(params: {
  file?: string
  level?: string
  keyword?: string
  page?: number
  page_size?: number
}) {
  return api.get<LogEntriesResult>('/logs/entries', { params })
}

export function getLogLevel() {
  return api.get<{ level: string; detail_logging: string }>('/logs/level')
}

export function getLogDownloadUrl(file: string) {
  const base = (api.defaults.baseURL || '/api').replace(/\/$/, '')
  return `${base}/logs/download?file=${encodeURIComponent(file)}`
}
