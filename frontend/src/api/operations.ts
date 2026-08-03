import api from './index'

export const getOperationsProgress = (page = 1, page_size = 50) =>
  api.get<any>('/operations/progress', { params: { page, page_size } })
export const runCampaignAction = (campaign_type: string, campaign_id: number, action: string) =>
  api.post('/operations/campaigns/action', { campaign_type, campaign_id, action })
export const bulkFailedTasks = (action: 'retry' | 'archive', task_ids: number[]) =>
  api.post('/operations/tasks/bulk', { action, task_ids })
export const getFailureArchive = (page = 1, page_size = 50) =>
  api.get<any>('/operations/tasks/archive', { params: { page, page_size } })
export const getFailedTasks = (page = 1, page_size = 50) =>
  api.get<any>('/operations/tasks/failures', { params: { page, page_size } })
export const getTaskTimeline = (id: number) => api.get<any>(`/operations/tasks/${id}/timeline`)
export const getExceptions = (params: Record<string, unknown> = {}) => api.get<any>('/operations/exceptions', { params })
export const resolveException = (id: number, resolved = true) => api.put(`/operations/exceptions/${id}/resolve`, { resolved })
export const getConfigurationHealth = (record_events = false) => api.get<any[]>('/operations/configuration-health', { params: { record_events } })
export const getAuditLogs = (page = 1, page_size = 50, action = '') =>
  api.get<any>('/operations/audits', { params: { page, page_size, action } })
export const getDlrCodes = () => api.get<Record<string, any>>('/sms-tasks/dlr-codes')
export const exportReport = (report: string) => api.get(`/operations/export/${report}`, { responseType: 'blob' })
