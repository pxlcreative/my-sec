export function formatAum(n: number | null | undefined): string {
  if (n == null) return '—'
  if (n === 0) return '$0'
  const abs = Math.abs(n)
  if (abs >= 1_000_000_000) {
    return `$${(n / 1_000_000_000).toFixed(1)}B`
  }
  if (abs >= 1_000_000) {
    return `$${(n / 1_000_000).toFixed(1)}M`
  }
  if (abs >= 1_000) {
    return `$${(n / 1_000).toFixed(1)}K`
  }
  return `$${n.toLocaleString()}`
}

export function formatDate(s: string | null | undefined): string {
  if (!s) return '—'
  try {
    const d = new Date(s)
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
  } catch {
    return s
  }
}

export function formatDuration(start: string | null, end: string | null): string {
  if (!start || !end) return '—'
  try {
    const ms = new Date(end).getTime() - new Date(start).getTime()
    const seconds = Math.floor(ms / 1000)
    if (seconds < 60) return `${seconds}s`
    const minutes = Math.floor(seconds / 60)
    const remainingSeconds = seconds % 60
    return `${minutes}m ${remainingSeconds}s`
  } catch {
    return '—'
  }
}

export const US_STATES = [
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
  'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
  'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
  'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
  'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
  'DC', 'PR', 'VI', 'GU',
]

export const DEFAULT_EXPORT_FIELDS = [
  'crd_number',
  'legal_name',
  'business_name',
  'registration_status',
  'firm_type',
  'aum_total',
  'aum_discretionary',
  'aum_non_discretionary',
  'num_accounts',
  'num_employees',
  'main_street1',
  'main_street2',
  'main_city',
  'main_state',
  'main_zip',
  'main_country',
  'phone',
  'website',
  'org_type',
  'fiscal_year_end',
  'last_filing_date',
  'sec_number',
  'aum_2023',
  'aum_2024',
  'created_at',
  'updated_at',
]
