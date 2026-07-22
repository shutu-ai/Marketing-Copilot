export function formatDateTime(dt: string): string {
  if (!dt) return '-'
  return dt.replace('T', ' ').substring(0, 19)
}

export function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

export function getStatusType(status: string): string {
  const map: Record<string, string> = {
    pending: 'warning',
    processing: 'primary',
    completed: 'success',
    failed: 'danger',
    partial: 'warning',
    active: 'success',
    inactive: 'info',
  }
  return map[status] || 'info'
}

export function getStatusLabel(t: (key: string) => string, prefix: string, status: string): string {
  const key = `${prefix}_${status}`
  const label = t(key)
  return label === key ? status : label
}
