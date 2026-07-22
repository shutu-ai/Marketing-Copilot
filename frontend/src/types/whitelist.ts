export interface UploadHistory {
  id: number
  filename: string
  total_rows: number
  success_rows: number
  failed_rows: number
  status: string
  import_mode: string
  error_detail: string
  created_at: string
}

export interface PreviewResult {
  headers: string[]
  rows: Record<string, string>[]
  total: number
  missing_fields?: string[]
  format_errors?: { row: number; error: string }[]
  file_path?: string
}

export interface ImportResult {
  upload_id: number
  total: number
  success: number
  failed: number
  errors: { row: number; error: string }[]
}

export interface WhitelistUser {
  id: number
  msisdn: string
  region: string
  language: string
  status: string
  upload_id: number
  created_at: string
}
