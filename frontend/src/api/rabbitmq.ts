import api from './index'

export function getRabbitMQConfig() {
  return api.get<any>('/rabbitmq/config')
}

export function saveRabbitMQConfig(data: any) {
  return api.post<any>('/rabbitmq/config', data)
}

export function updateRabbitMQConfig(data: any) {
  return api.put<any>('/rabbitmq/config', data)
}

export function getRabbitMQStatus() {
  return api.get<{ connected: boolean; message: string }>('/rabbitmq/status')
}

export function testPublish(routingKey: string, message: any) {
  return api.post<{ success: boolean; message: string }>('/rabbitmq/test', { routing_key: routingKey, message })
}

export function getSmsLog(params: { page?: number; page_size?: number; status?: string; msisdn?: string }) {
  return api.get<{ total: number; page: number; page_size: number; items: any[] }>('/rabbitmq/sms-log', { params })
}

export function publishFile(fileId: number, campaignName?: string) {
  return api.post<{ success: boolean; published: number; total_rows: number; routing_key: string; campaign: string }>(
    '/monitor/files/publish', { file_id: fileId, campaign_name: campaignName || '' }
  )
}
