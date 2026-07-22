export interface Campaign {
  id: number
  name: string
  description: string
  enabled: number
  marketing_type: string
  marketing_campaign_id: number
  created_at: string
  updated_at: string
}

export interface SmsTemplate {
  id: number
  campaign_id: number
  language: string
  content: string
  source_addr: string
  enabled: number
  created_at: string
  updated_at: string
}

export interface TemplatePreview {
  content: string
  variables: Record<string, string>
  rendered: string
  character_count: number
  sms_segments: number
}
