import api from './index'

export interface DashboardStats {
  total_batch_campaigns: number
  in_progress_campaigns: number
  ended_campaigns: number
  today_sent_count: number
  total_sms_campaigns: number
  realtime_campaigns: number
  realtime_files: number
  realtime_event_users: number
  realtime_matched_users: number
}

export function getDashboardStats() {
  return api.get<DashboardStats>('/dashboard/stats')
}
