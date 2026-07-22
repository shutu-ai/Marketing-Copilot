import api from './index'

export interface ForeignKey {
  ref_table: string
  ref_column: string
}

export interface ColumnInfo {
  column_name: string
  data_type: string
  is_nullable: string
  column_default: string | null
  column_comment: string | null
  is_pk: boolean
  foreign_key: ForeignKey | null
}

export interface TableInfo {
  table_name: string
  table_comment: string | null
  columns: ColumnInfo[]
}

export interface QueryResult {
  type: 'select' | 'execute' | 'error'
  columns?: string[]
  rows?: any[][]
  row_count?: number
  affected_rows?: number
  message?: string
  error?: string
}

export function getTables() {
  return api.get<TableInfo[]>('/db-query/tables')
}

export function executeQuery(data: { sql: string; params?: any[] }) {
  return api.post<QueryResult>('/db-query/execute', data)
}
