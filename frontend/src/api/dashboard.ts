import api from './index'

export interface DashboardStats {
  total_batch_campaigns: number
  in_progress_campaigns: number
  in_progress_batch_campaigns: number
  in_progress_realtime_campaigns: number
  ended_campaigns: number
  today_sent_count: number
  total_sms_campaigns: number
  realtime_campaigns: number
  realtime_files: number
  realtime_event_users: number
  realtime_matched_users: number
  daily_trend: Array<{ stats_date: string; sent_count: number; conversion_count: number }>
  open_exception_count: number
  recent_exceptions: Array<{ id: number; category: string; severity: string; code: string; message: string; created_at: string }>
}

export function getDashboardStats() {
  return api.get<DashboardStats>('/dashboard/stats')
}
