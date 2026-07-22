import api from './index'

export function getDbConfig() {
  return api.get<any>('/db-config')
}

export function saveDbConfig(data: any) {
  return api.post<any>('/db-config', data)
}

export function updateDbConfig(data: any) {
  return api.put<any>('/db-config', data)
}

export function testDbConnection(data: any) {
  return api.post<{ success: boolean; message: string }>('/db-config/test', data)
}
