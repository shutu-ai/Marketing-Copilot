export function isValidMsisdn(msisdn: string): boolean {
  return /^\d{7,15}$/.test(msisdn.trim())
}

export function isValidPort(port: number): boolean {
  return Number.isInteger(port) && port > 0 && port <= 65535
}

export function isValidHost(host: string): boolean {
  const ipv4 = /^(\d{1,3}\.){3}\d{1,3}$/
  const domain = /^([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$/
  return ipv4.test(host) || domain.test(host)
}

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}
