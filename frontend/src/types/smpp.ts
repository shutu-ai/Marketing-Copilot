export interface SmppConfig {
  id: number
  name: string
  host: string
  port: number
  system_id: string
  password: string
  source_addr: string
  bind_type: 'transmitter' | 'receiver' | 'transceiver'
  address_ton: number
  address_npi: number
  data_coding: number
  pool_size: number
  window_size: number
  tps: number
  tps_scope: 'per_bind'
  priority: number
  reconnect_max_seconds: number
  delivery_receipt_mode: 'required' | 'optional' | 'disabled'
  enquire_interval: number
  timeout: number
  enabled: number
  created_at: string
  updated_at: string
  restart_required?: boolean
  effective_state?: 'applied' | 'pending_restart' | 'next_start'
  hot_applied?: boolean
}

export interface SmppConfigForm {
  name: string
  host: string
  port: number
  system_id: string
  password: string
  source_addr: string
  bind_type: string
  address_ton: number
  address_npi: number
  data_coding: number
  pool_size: number
  window_size: number
  tps: number
  priority: number
  reconnect_max_seconds: number
  delivery_receipt_mode: 'required' | 'optional' | 'disabled'
  enquire_interval: number
  timeout: number
}
