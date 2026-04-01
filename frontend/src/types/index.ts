export interface FirmSummary {
  crd_number: number
  legal_name: string
  business_name: string | null
  main_city: string | null
  main_state: string | null
  aum_total: number | null
  registration_status: string | null
  last_filing_date: string | null
  platforms: string[]
}

export interface FirmDetail extends FirmSummary {
  sec_number: string | null
  firm_type: string | null
  aum_discretionary: number | null
  aum_non_discretionary: number | null
  aum_2023: number | null
  aum_2024: number | null
  num_accounts: number | null
  num_employees: number | null
  main_street1: string | null
  main_street2: string | null
  main_zip: string | null
  main_country: string | null
  phone: string | null
  website: string | null
  org_type: string | null
  fiscal_year_end: string | null
  created_at: string | null
  updated_at: string | null
  latest_brochure: BrochureMeta | null
}

export interface PaginatedFirms {
  total: number
  page: number
  page_size: number
  results: FirmSummary[]
}

export interface BrochureMeta {
  brochure_version_id: number
  brochure_name: string | null
  date_submitted: string | null
  source_month: string | null
  file_size_bytes: number | null
}

export interface AumHistoryPoint {
  filing_date: string
  aum_total: number | null
  aum_discretionary: number | null
  aum_non_discretionary: number | null
  num_accounts: number | null
  source: string
}

export interface AumAnnualSummary {
  year: number
  peak_aum: number | null
  trough_aum: number | null
  latest_aum_for_year: number | null
  filing_count: number
}

export interface AumHistoryResponse {
  crd_number: number
  annual: AumAnnualSummary[]
  filings: AumHistoryPoint[]
}

export interface ChangeRecord {
  id: number
  field_path: string
  old_value: string | null
  new_value: string | null
  detected_at: string
  snapshot_from: number | null
  snapshot_to: number | null
}

export interface FirmHistoryResponse {
  crd_number: number
  changes: ChangeRecord[]
}

export interface PlatformOut {
  id: number
  name: string
  description: string | null
  created_at: string | null
}

export interface FirmPlatformTag {
  id: number
  platform_id: number
  platform_name: string
  tagged_at: string | null
  tagged_by: string | null
  notes: string | null
}

export interface AlertRuleOut {
  id: number
  label: string
  rule_type: string
  platform_ids: number[] | null
  crd_numbers: number[] | null
  threshold_pct: number | null
  field_path: string | null
  delivery: string
  delivery_target: string | null
  active: boolean
  created_at: string | null
}

export interface AlertEventOut {
  id: number
  rule_id: number
  crd_number: number
  firm_name: string | null
  rule_type: string
  field_path: string | null
  old_value: string | null
  new_value: string | null
  platform_name: string | null
  fired_at: string
  delivered_at: string | null
  delivery_status: string | null
}

export interface AlertTestResponse {
  rule_id: number
  delivery: string
  delivery_target: string | null
  success: boolean
  message: string
}

export interface ExportJobOut {
  id: string
  format: string
  status: string
  row_count: number | null
  file_path: string | null
  error_message: string | null
  created_at: string | null
  completed_at: string | null
  expires_at: string | null
}

export interface ExportTemplateOut {
  id: number
  name: string
  description: string | null
  format: string
  filter_criteria: Record<string, unknown> | null
  field_selection: Record<string, unknown> | null
  created_at: string | null
  updated_at: string | null
}

export interface SyncStatusEntry {
  id: number
  job_type: string
  status: string
  firms_processed: number
  firms_updated: number
  changes_detected: number
  error_message: string | null
  started_at: string | null
  completed_at: string | null
  created_at: string | null
}

export interface CronScheduleOut {
  id: number
  name: string
  task: string
  description: string | null
  minute: string
  hour: string
  day_of_month: string
  month_of_year: string
  day_of_week: string
  enabled: boolean
  updated_at: string
}

export interface MatchResult {
  id: string | number | null
  input_name: string
  input_city: string | null
  input_state: string | null
  input_zip: string | null
  best_score: number
  best_status: 'confirmed' | 'probable' | 'possible' | 'no_match'
  candidates: MatchCandidate[]
}

export interface MatchCandidate {
  crd_number: number
  legal_name: string
  business_name: string | null
  main_city: string | null
  main_state: string | null
  main_zip: string | null
  registration_status: string | null
  score: number
  status: string
}

export interface BulkMatchSyncResponse {
  results: MatchResult[]
  stats: {
    total: number
    confirmed: number
    probable: number
    possible: number
    no_match: number
  }
}

export interface BulkMatchAsyncResponse {
  job_id: number
  status: string
  message: string
}

export interface MatchJobStatus {
  job_id: number
  status: string
  created_at: string | null
  started_at: string | null
  completed_at: string | null
  error_message: string | null
  results: BulkMatchSyncResponse | null
}
