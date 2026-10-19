import api from './index'

export interface DashboardStats {
  pipeline: 'legacy' | 'vnext'
  total_batch_campaigns: number
  in_progress_campaigns: number
  in_progress_batch_campaigns: number
  in_progress_realtime_campaigns: number
  ended_campaigns: number
  today_sent_count: number
  today_socket_written_count?: number
  today_smsc_accepted_count?: number
  today_unknown_count?: number
  today_write_failed_count?: number
  sent_metric?: string
  sent_unit?: string
  delivery_receipts_available?: boolean
  total_sms_campaigns: number
  realtime_campaigns: number
  today_realtime_files: number
  today_realtime_event_users: number
  today_realtime_matched_users: number
  daily_trend: Array<{ stats_date: string; sent_count: number; conversion_count: number }>
  today_open_exception_count: number
  today_recent_exceptions: Array<{ id: number; category: string; severity: string; code: string; message: string; created_at: string }>
}

export function getDashboardStats() {
  return api.get<DashboardStats>('/dashboard/stats')
}
