const dateTimeFormatters = new Map<string, Intl.DateTimeFormat>()

export function formatDateTime(dt: string): string {
  if (!dt) return '-'
  return dt.replace('T', ' ').substring(0, 19)
}

export function formatDateTimeInTimeZone(dt: string, timeZone?: string): string {
  if (!dt) return '-'
  if (!timeZone || !/[zZ]|[+-]\d{2}:?\d{2}$/.test(dt.trim())) {
    return formatDateTime(dt)
  }

  const parsed = new Date(dt)
  if (Number.isNaN(parsed.getTime())) return formatDateTime(dt)

  try {
    let formatter = dateTimeFormatters.get(timeZone)
    if (!formatter) {
      formatter = new Intl.DateTimeFormat('sv-SE', {
        timeZone,
        calendar: 'gregory',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23',
      })
      dateTimeFormatters.set(timeZone, formatter)
    }
    return formatter.format(parsed)
  } catch {
    return formatDateTime(dt)
  }
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
