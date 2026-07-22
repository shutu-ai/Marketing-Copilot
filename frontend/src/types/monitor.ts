export interface MonitorConfig {
  id: number
  directory_path: string
  target_directories: string[]
  enabled: number
  created_at: string
  updated_at?: string
}

export interface MonitorFile {
  id: number
  filename: string
  directory_path: string
  file_size: number
  row_count: number
  status: 'pending' | 'processing' | 'completed' | 'failed'
  created_at: string
  updated_at: string
}
