import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { AlertTriangle, ArrowLeft, CheckCircle, Download, ExternalLink, FileQuestion, FileSearch, FileText, Loader2, Plus, RefreshCw, X, XCircle } from 'lucide-react'
import { Button } from '../components/Button'
import {
  addFirmPlatform,
  getBrochureParsed,
  getFirm,
  getFirmAumHistory,
  getFirmBrochures,
  getFirmBusinessProfile,
  getFirmDisclosures,
  getFirmHistory,
  getFirmPlatforms,
  getFirmQuestionnaire,
  getFirmQuestionnaires,
  getPlatforms,
  getReductoSettings,
  parseBrochure,
  refreshFirm,
  regenerateFirmQuestionnaire,
  removeFirmPlatform,
  updateFirmQuestionnaireAnswers,
} from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { StatusBadge } from '../components/StatusBadge'
import { useToast } from '../components/Toast'
import { formatAum, formatDate } from '../utils'
import type { BusinessProfile, DisclosuresSummary, FirmDetail as FirmDetailType, QuestionnaireQuestionOut } from '../types'

type TabKey = 'overview' | 'disclosures' | 'business-profile' | 'aum' | 'brochures' | 'platforms' | 'history' | 'questionnaires'

const TABS: { key: TabKey; label: string }[] = [
  { key: 'overview', label: 'Overview' },
  { key: 'disclosures', label: 'Disclosures' },
  { key: 'business-profile', label: 'Business Profile' },
  { key: 'aum', label: 'AUM History' },
  { key: 'brochures', label: 'Brochures' },
  { key: 'platforms', label: 'Platform Tags' },
  { key: 'history', label: 'Change History' },
  { key: 'questionnaires', label: 'Questionnaires' },
]

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex gap-2 py-2 border-b border-gray-100 last:border-0">
      <dt className="w-44 flex-shrink-0 text-sm font-medium text-gray-500">{label}</dt>
      <dd className="text-sm text-gray-900 min-w-0 break-words">{value ?? '—'}</dd>
    </div>
  )
}

function FreshnessChip({ lastFilingDate }: { lastFilingDate: string | null }) {
  if (!lastFilingDate) {
    return (
      <span className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded bg-amber-100 text-amber-700">
        <AlertTriangle className="w-3 h-3" />
        No filing date
      </span>
    )
  }
  const stale = new Date(lastFilingDate) < new Date(new Date().setFullYear(new Date().getFullYear() - 2))
  if (stale) {
    return (
      <span className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded bg-amber-100 text-amber-700">
        <AlertTriangle className="w-3 h-3" />
        Last filed {formatDate(lastFilingDate)}
      </span>
    )
  }
  return (
    <span className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded bg-gray-100 text-gray-500">
      Filed {formatDate(lastFilingDate)}
    </span>
  )
}

export default function FirmDetail() {
  const { crd } = useParams<{ crd: string }>()
  const crdNum = Number(crd)
  const [activeTab, setActiveTab] = useState<TabKey>('overview')
  const [selectedPlatformId, setSelectedPlatformId] = useState<string>('')
  const { addToast } = useToast()
  const queryClient = useQueryClient()

  const { data: firm, isLoading, error } = useQuery({
    queryKey: ['firm', crdNum],
    queryFn: () => getFirm(crdNum),
  })

  const { data: disclosures } = useQuery({
    queryKey: ['firm-disclosures', crdNum],
    queryFn: () => getFirmDisclosures(crdNum),
    enabled: !!firm,
  })

  const { data: aumHistory } = useQuery({
    queryKey: ['aum-history', crdNum],
    queryFn: () => getFirmAumHistory(crdNum),
    enabled: activeTab === 'aum',
  })

  const { data: brochures } = useQuery({
    queryKey: ['brochures', crdNum],
    queryFn: () => getFirmBrochures(crdNum),
    enabled: activeTab === 'brochures',
  })

  const { data: businessProfile } = useQuery({
    queryKey: ['firm-business-profile', crdNum],
    queryFn: () => getFirmBusinessProfile(crdNum),
    enabled: activeTab === 'business-profile',
  })

  const { data: firmPlatforms } = useQuery({
    queryKey: ['firm-platforms', crdNum],
    queryFn: () => getFirmPlatforms(crdNum),
    enabled: activeTab === 'platforms',
  })

  const { data: allPlatforms } = useQuery({
    queryKey: ['platforms'],
    queryFn: getPlatforms,
    enabled: activeTab === 'platforms',
  })

  const { data: history } = useQuery({
    queryKey: ['firm-history', crdNum],
    queryFn: () => getFirmHistory(crdNum),
    enabled: activeTab === 'history',
  })

  const { data: firmQuestionnaires } = useQuery({
    queryKey: ['firm-questionnaires', crdNum],
    queryFn: () => getFirmQuestionnaires(crdNum),
    enabled: activeTab === 'questionnaires',
  })

  const refreshMutation = useMutation({
    mutationFn: () => refreshFirm(crdNum),
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['firm', crdNum] })
      queryClient.invalidateQueries({ queryKey: ['firm-business-profile', crdNum] })
      queryClient.invalidateQueries({ queryKey: ['aum-history', crdNum] })
      queryClient.invalidateQueries({ queryKey: ['firm-history', crdNum] })
      const msg = result.changed
        ? `Refreshed — ${result.num_changes} field${result.num_changes === 1 ? '' : 's'} updated: ${result.fields_changed.join(', ')}`
        : 'Refreshed — no changes detected'
      addToast(msg, result.changed ? 'success' : 'info')
    },
    onError: (err: any) => {
      const detail = err?.response?.data?.detail ?? 'IAPD refresh failed'
      addToast(detail, 'error')
    },
  })

  const addPlatformMutation = useMutation({
    mutationFn: (platformId: number) => addFirmPlatform(crdNum, platformId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['firm-platforms', crdNum] })
      addToast('Platform tag added', 'success')
      setSelectedPlatformId('')
    },
    onError: () => addToast('Failed to add platform tag', 'error'),
  })

  const removePlatformMutation = useMutation({
    mutationFn: (tagId: number) => removeFirmPlatform(crdNum, tagId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['firm-platforms', crdNum] })
      addToast('Platform tag removed', 'success')
    },
    onError: () => addToast('Failed to remove platform tag', 'error'),
  })

  function handleDownloadDDQ() {
    window.open(`/api/firms/${crdNum}/due-diligence-excel`, '_blank')
  }

  function handleDownloadBrochure() {
    window.open(`/api/firms/${crdNum}/brochure`, '_blank')
  }

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-6 w-48" />
        <Skeleton className="h-96 w-full" />
      </div>
    )
  }

  if (error || !firm) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-red-700">
        Failed to load firm. The CRD number may not exist.
      </div>
    )
  }

  const statusColor =
    firm.registration_status === 'Registered'
      ? 'bg-green-100 text-green-800'
      : firm.registration_status === 'Withdrawn'
      ? 'bg-red-100 text-red-800'
      : 'bg-gray-100 text-gray-700'

  const totalDisclosures = disclosures ? disclosures.total_count : 0

  return (
    <div>
      {/* Back link */}
      <Link to="/" className="inline-flex items-center gap-1 text-sm text-brand-600 hover:text-brand-700 mb-4">
        <ArrowLeft className="w-4 h-4" />
        Back to Search
      </Link>

      {/* Header */}
      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{firm.legal_name}</h1>
          <div className="flex items-center gap-3 mt-2 flex-wrap">
            <a
              href={`https://adviserinfo.sec.gov/firm/summary/${firm.crd_number}`}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-sm font-mono bg-gray-100 text-gray-600 hover:bg-gray-200 px-2 py-0.5 rounded"
            >
              CRD {firm.crd_number}
              <ExternalLink className="w-3 h-3" />
            </a>
            {firm.registration_status && (
              <span className={`text-xs font-medium px-2 py-0.5 rounded ${statusColor}`}>
                {firm.registration_status}
              </span>
            )}
            <FreshnessChip lastFilingDate={firm.last_filing_date} />
            {totalDisclosures > 0 && (
              <button
                onClick={() => setActiveTab('disclosures')}
                className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded bg-red-50 text-red-700 hover:bg-red-100 transition-colors"
              >
                <AlertTriangle className="w-3 h-3" />
                {totalDisclosures} disclosure{totalDisclosures !== 1 ? 's' : ''}
              </button>
            )}
            {firm.main_city && firm.main_state && (
              <span className="text-sm text-gray-500">
                {firm.main_city}, {firm.main_state}
              </span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button onClick={handleDownloadDDQ} size="sm" icon={<Download className="w-4 h-4" />}>
            DDQ Excel
          </Button>
          <Button onClick={handleDownloadBrochure} variant="outline" size="sm" icon={<FileText className="w-4 h-4" />}>
            Latest Brochure
          </Button>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200 mb-6">
        <nav className="flex gap-0 -mb-px overflow-x-auto">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
                activeTab === tab.key
                  ? 'border-brand-600 text-brand-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab.label}
              {tab.key === 'disclosures' && totalDisclosures > 0 && (
                <span className="ml-1.5 inline-flex items-center justify-center min-w-[1.1rem] px-1 h-4 text-xs rounded-full bg-gray-200 text-gray-600">
                  {totalDisclosures}
                </span>
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab content */}
      {activeTab === 'overview' && <OverviewTab firm={firm} disclosures={disclosures ?? null} onDisclosuresClick={() => setActiveTab('disclosures')} />}
      {activeTab === 'disclosures' && (
        <DisclosuresTab crd={crdNum} disclosures={disclosures ?? null} />
      )}
      {activeTab === 'business-profile' && (
        <div>
          {!businessProfile ? (
            <Skeleton className="h-80 w-full" />
          ) : (
            <BusinessProfileTab
              profile={businessProfile}
              lastRefreshed={firm?.last_iapd_refresh_at ?? null}
              onRefresh={() => refreshMutation.mutate()}
              isRefreshing={refreshMutation.isPending}
            />
          )}
        </div>
      )}
      {activeTab === 'aum' && (
        <div>
          {!aumHistory ? (
            <Skeleton className="h-80 w-full" />
          ) : (
            <AumHistoryTab aumHistory={aumHistory} />
          )}
        </div>
      )}
      {activeTab === 'brochures' && (
        <div>
          {!brochures ? (
            <Skeleton className="h-40 w-full" />
          ) : (
            <BrochuresTab brochures={brochures} crd={crdNum} />
          )}
        </div>
      )}
      {activeTab === 'platforms' && (
        <div>
          {firmPlatforms && allPlatforms ? (
            <PlatformTagsTab
              crd={crdNum}
              firmPlatforms={firmPlatforms}
              allPlatforms={allPlatforms}
              selectedPlatformId={selectedPlatformId}
              onSelectPlatform={setSelectedPlatformId}
              onAdd={() => {
                if (selectedPlatformId) {
                  addPlatformMutation.mutate(Number(selectedPlatformId))
                }
              }}
              onRemove={(tagId) => removePlatformMutation.mutate(tagId)}
              isAdding={addPlatformMutation.isPending}
            />
          ) : (
            <Skeleton className="h-40 w-full" />
          )}
        </div>
      )}
      {activeTab === 'history' && (
        <div>
          {!history ? (
            <Skeleton className="h-40 w-full" />
          ) : (
            <ChangeHistoryTab history={history} />
          )}
        </div>
      )}
      {activeTab === 'questionnaires' && (
        <div>
          {!firmQuestionnaires ? (
            <Skeleton className="h-40 w-full" />
          ) : (
            <QuestionnairesTab crd={crdNum} questionnaires={firmQuestionnaires} />
          )}
        </div>
      )}
    </div>
  )
}

function OverviewTab({
  firm,
  disclosures,
  onDisclosuresClick,
}: {
  firm: FirmDetailType
  disclosures: DisclosuresSummary | null
  onDisclosuresClick: () => void
}) {
  const hasDisclosures = disclosures && disclosures.total_count > 0

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Identity</h3>
        <dl>
          <DetailRow label="Legal Name" value={firm.legal_name} />
          <DetailRow label="Business Name" value={firm.business_name} />
          <DetailRow label="CRD Number" value={firm.crd_number} />
          <DetailRow label="SEC Number" value={firm.sec_number} />
          <DetailRow label="Registration Status" value={
            firm.registration_status ? <StatusBadge status={firm.registration_status} /> : null
          } />
          <DetailRow label="Org Type" value={firm.org_type} />
          <DetailRow label="Last Filing Date" value={formatDate(firm.last_filing_date)} />
          {hasDisclosures && (
            <DetailRow label="Disclosures" value={
              <button onClick={onDisclosuresClick} className="inline-flex items-center gap-1.5 text-sm text-red-700 hover:text-red-800">
                <AlertTriangle className="w-3.5 h-3.5" />
                <span>
                  {disclosures.criminal_count > 0 && `${disclosures.criminal_count} criminal`}
                  {disclosures.criminal_count > 0 && disclosures.regulatory_count > 0 && ' · '}
                  {disclosures.regulatory_count > 0 && `${disclosures.regulatory_count} regulatory`}
                  {(disclosures.criminal_count > 0 || disclosures.regulatory_count > 0) && disclosures.civil_count > 0 && ' · '}
                  {disclosures.civil_count > 0 && `${disclosures.civil_count} civil`}
                  {(disclosures.criminal_count > 0 || disclosures.regulatory_count > 0 || disclosures.civil_count > 0) && disclosures.customer_count > 0 && ' · '}
                  {disclosures.customer_count > 0 && `${disclosures.customer_count} customer`}
                </span>
              </button>
            } />
          )}
        </dl>
      </div>
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Assets Under Management</h3>
        <dl>
          <DetailRow label="Total AUM" value={formatAum(firm.aum_total)} />
          <DetailRow label="Discretionary AUM" value={formatAum(firm.aum_discretionary)} />
          <DetailRow label="Non-Discretionary AUM" value={formatAum(firm.aum_non_discretionary)} />
          <DetailRow label="AUM 2023" value={formatAum(firm.aum_2023)} />
          <DetailRow label="AUM 2024" value={formatAum(firm.aum_2024)} />
          <DetailRow label="Num. Accounts" value={firm.num_accounts?.toLocaleString()} />
          <DetailRow label="Num. Employees" value={firm.num_employees?.toLocaleString()} />
          <DetailRow label="Fiscal Year End" value={firm.fiscal_year_end} />
        </dl>
      </div>
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Contact & Address</h3>
        <dl>
          <DetailRow label="Street 1" value={firm.main_street1} />
          <DetailRow label="Street 2" value={firm.main_street2} />
          <DetailRow label="City" value={firm.main_city} />
          <DetailRow label="State" value={firm.main_state} />
          <DetailRow label="ZIP" value={firm.main_zip} />
          <DetailRow label="Country" value={firm.main_country} />
          <DetailRow label="Phone" value={firm.phone} />
          <DetailRow label="Website" value={
            firm.website ? (
              <a href={firm.website} target="_blank" rel="noopener noreferrer" className="text-brand-600 hover:underline">
                {firm.website}
              </a>
            ) : null
          } />
        </dl>
      </div>
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Sync Info</h3>
        <dl>
          <DetailRow label="Created At" value={formatDate(firm.created_at)} />
          <DetailRow label="Updated At" value={formatDate(firm.updated_at)} />
          <DetailRow label="Last IAPD Refresh" value={firm.last_iapd_refresh_at ? formatDate(firm.last_iapd_refresh_at) : 'Never'} />
          {firm.platforms && firm.platforms.length > 0 && (
            <DetailRow label="Platforms" value={
              <div className="flex flex-wrap gap-1">
                {firm.platforms.map((p) => (
                  <span key={p} className="px-2 py-0.5 rounded-full text-xs bg-brand-100 text-brand-800">{p}</span>
                ))}
              </div>
            } />
          )}
        </dl>
      </div>
    </div>
  )
}

function DisclosuresTab({ crd, disclosures }: { crd: number; disclosures: DisclosuresSummary | null }) {
  if (!disclosures) {
    return <Skeleton className="h-40 w-full" />
  }

  const noRecord = disclosures.total_count === 0 && !disclosures.updated_at
  if (noRecord) {
    return (
      <div className="bg-white rounded-lg border border-gray-200 p-8 text-center">
        <p className="text-gray-500 font-medium">Disclosure data not available</p>
        <p className="text-sm text-gray-400 mt-1">Run bulk CSV load or check IAPD directly.</p>
        <a
          href={`https://adviserinfo.sec.gov/firm/summary/${crd}`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 mt-3 text-sm text-brand-600 hover:underline"
        >
          View on IAPD <ExternalLink className="w-3.5 h-3.5" />
        </a>
      </div>
    )
  }

  const cards = [
    { label: 'Criminal', count: disclosures.criminal_count },
    { label: 'Regulatory', count: disclosures.regulatory_count },
    { label: 'Civil', count: disclosures.civil_count },
    { label: 'Customer Disputes', count: disclosures.customer_count },
  ]

  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {cards.map((c) => (
          <div key={c.label} className="rounded-lg border border-gray-200 bg-gray-50 p-5 text-center">
            <div className="text-3xl font-bold text-gray-900 tabular-nums leading-tight">{c.count.toLocaleString()}</div>
            <div className="text-sm font-medium text-gray-500 mt-1">{c.label}</div>
          </div>
        ))}
      </div>
      <div className="bg-white rounded-lg border border-gray-200 p-5 flex items-center justify-between">
        <p className="text-sm text-gray-500">
          Counts from SEC DRP filings (loaded with bulk CSV).
          {disclosures.updated_at && ` Last updated ${formatDate(disclosures.updated_at)}.`}
        </p>
        <a
          href={`https://adviserinfo.sec.gov/firm/summary/${crd}`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-sm text-brand-600 hover:underline whitespace-nowrap ml-4"
        >
          View full disclosures on IAPD <ExternalLink className="w-3.5 h-3.5" />
        </a>
      </div>
    </div>
  )
}

function Chip({ label }: { label: string }) {
  return (
    <span className="inline-block px-2.5 py-1 rounded-full text-xs font-medium bg-brand-50 text-brand-800 border border-brand-100">
      {label}
    </span>
  )
}

function BusinessProfileTab({
  profile,
  lastRefreshed,
  onRefresh,
  isRefreshing,
}: {
  profile: BusinessProfile
  lastRefreshed: string | null
  onRefresh: () => void
  isRefreshing: boolean
}) {
  const isEmpty =
    profile.client_types.length === 0 &&
    profile.compensation_types.length === 0 &&
    profile.investment_strategies.length === 0 &&
    profile.affiliations.length === 0

  const header = (
    <div className="flex items-center justify-between mb-4">
      <p className="text-xs text-gray-400">
        {lastRefreshed ? `Last refreshed from IAPD: ${formatDate(lastRefreshed)}` : 'Never refreshed from IAPD'}
      </p>
      <Button size="sm" variant="outline" onClick={onRefresh} disabled={isRefreshing}>
        <RefreshCw className={`w-3.5 h-3.5 mr-1.5 ${isRefreshing ? 'animate-spin' : ''}`} />
        {isRefreshing ? 'Refreshing…' : 'Refresh from IAPD'}
      </Button>
    </div>
  )

  if (!profile.available) {
    return (
      <div>
        {header}
        <div className="bg-white rounded-lg border border-gray-200 p-8 text-center">
          <p className="text-gray-500 font-medium">Business profile data is not available</p>
          <p className="text-sm text-gray-400 mt-1 max-w-md mx-auto">
            The public IAPD API does not expose Form ADV Part 1A checkbox data (client types,
            compensation arrangements, strategies). This information is only available in the
            PDF version of Form ADV.
          </p>
        </div>
      </div>
    )
  }

  if (isEmpty) {
    return (
      <div>
        {header}
        <div className="bg-white rounded-lg border border-gray-200 p-8 text-center">
          <p className="text-gray-500 font-medium">No business profile data available</p>
          <p className="text-sm text-gray-400 mt-1">
            IAPD did not return business profile data for this firm.
          </p>
        </div>
      </div>
    )
  }

  return (
    <div>
      {header}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Clients Served</h3>
        {profile.client_types.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {profile.client_types.map((t) => <Chip key={t} label={t} />)}
          </div>
        ) : (
          <p className="text-sm text-gray-400">Not available</p>
        )}
      </div>
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Compensation Arrangements</h3>
        {profile.compensation_types.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {profile.compensation_types.map((t) => <Chip key={t} label={t} />)}
          </div>
        ) : (
          <p className="text-sm text-gray-400">Not available</p>
        )}
      </div>
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Investment Strategies</h3>
        {profile.investment_strategies.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {profile.investment_strategies.map((t) => <Chip key={t} label={t} />)}
          </div>
        ) : (
          <p className="text-sm text-gray-400">Not available</p>
        )}
      </div>
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Business Affiliations</h3>
        {profile.affiliations.length > 0 ? (
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-3 py-1.5 text-left text-xs font-semibold text-gray-600 uppercase">Type</th>
                <th className="px-3 py-1.5 text-left text-xs font-semibold text-gray-600 uppercase">Name</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {profile.affiliations.map((a, i) => (
                <tr key={i}>
                  <td className="px-3 py-2 text-gray-700">{a.type || '—'}</td>
                  <td className="px-3 py-2 text-gray-700">{a.name || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="text-sm text-gray-400">No affiliations on file</p>
        )}
      </div>
    </div>
    </div>
  )
}

function AumHistoryTab({ aumHistory }: { aumHistory: Awaited<ReturnType<typeof getFirmAumHistory>> }) {
  const chartData = aumHistory.filings.map((f) => ({
    date: f.filing_date,
    aum_total: f.aum_total,
    aum_discretionary: f.aum_discretionary,
    aum_non_discretionary: f.aum_non_discretionary,
  }))

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg border border-gray-200 p-5">
        <h3 className="text-sm font-semibold text-gray-700 mb-4">AUM Over Time</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData} margin={{ top: 5, right: 20, left: 20, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis
              dataKey="date"
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => v?.slice(0, 7) ?? ''}
            />
            <YAxis
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => formatAum(v)}
            />
            <Tooltip
              formatter={(value: number) => formatAum(value)}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <Legend />
            <Line type="monotone" dataKey="aum_total" stroke="#2563eb" name="Total AUM" dot={false} strokeWidth={2} />
            <Line type="monotone" dataKey="aum_discretionary" stroke="#16a34a" name="Discretionary" dot={false} strokeWidth={1.5} />
            <Line type="monotone" dataKey="aum_non_discretionary" stroke="#ea580c" name="Non-Discretionary" dot={false} strokeWidth={1.5} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700">Annual Summary</h3>
        </div>
        <table className="w-full text-sm">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Year</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Peak AUM</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Trough AUM</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Latest AUM</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Filings</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {aumHistory.annual.map((row) => (
              <tr key={row.year} className="hover:bg-gray-50">
                <td className="px-4 py-2 font-medium">{row.year}</td>
                <td className="px-4 py-2 font-mono">{formatAum(row.peak_aum)}</td>
                <td className="px-4 py-2 font-mono">{formatAum(row.trough_aum)}</td>
                <td className="px-4 py-2 font-mono">{formatAum(row.latest_aum_for_year)}</td>
                <td className="px-4 py-2">{row.filing_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function BrochuresTab({
  brochures,
  crd,
}: {
  brochures: Awaited<ReturnType<typeof getFirmBrochures>>
  crd: number
}) {
  const queryClient = useQueryClient()
  const { addToast } = useToast()
  const [pendingVid, setPendingVid] = useState<number | null>(null)
  const [viewingVid, setViewingVid] = useState<number | null>(null)

  const { data: reducto } = useQuery({
    queryKey: ['reducto-settings'],
    queryFn: getReductoSettings,
  })
  const reductoEnabled = !!(reducto?.enabled && reducto?.api_key)
  const hasParsedRow = brochures.some((b) => b.parse_status)
  const showParseColumn = reductoEnabled || hasParsedRow

  const parseMutation = useMutation({
    mutationFn: (versionId: number) => parseBrochure(crd, versionId),
    onMutate: (versionId) => setPendingVid(versionId),
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['brochures', crd] })
      const pages = result.page_count ? `${result.page_count} pages, ` : ''
      addToast(`Parsed (${pages}${result.chunk_count ?? 0} chunks)`, 'success')
    },
    onError: (err: any) => {
      const detail = err?.response?.data?.detail ?? 'Parse failed'
      addToast(detail, 'error')
    },
    onSettled: () => setPendingVid(null),
  })

  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-5 py-3 border-b border-gray-200">
        <h3 className="text-sm font-semibold text-gray-700">ADV Part 2 Brochures</h3>
      </div>
      {brochures.length === 0 ? (
        <p className="px-5 py-6 text-sm text-gray-400">No brochures found for this firm.</p>
      ) : (
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Date Submitted</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Brochure Name</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Source Month</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">File Size</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Download</th>
              {showParseColumn && (
                <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Parse</th>
              )}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {brochures.map((b) => {
              const isParsing = parseMutation.isPending && pendingVid === b.brochure_version_id
              return (
                <tr key={b.brochure_version_id} className="hover:bg-gray-50">
                  <td className="px-4 py-2">{formatDate(b.date_submitted)}</td>
                  <td className="px-4 py-2">
                    <a
                      href={`/api/firms/${crd}/brochures/${b.brochure_version_id}/download`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-brand-600 hover:text-brand-700 hover:underline"
                    >
                      {b.brochure_name ?? `Brochure ${b.brochure_version_id}`}
                    </a>
                  </td>
                  <td className="px-4 py-2 font-mono text-xs">{b.source_month ?? '—'}</td>
                  <td className="px-4 py-2 text-sm">
                    {b.file_size_bytes ? `${(b.file_size_bytes / 1024).toFixed(0)} KB` : '—'}
                  </td>
                  <td className="px-4 py-2">
                    <a
                      href={`/api/firms/${crd}/brochures/${b.brochure_version_id}/download`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 text-brand-600 hover:text-brand-700 text-xs font-medium"
                    >
                      <Download className="w-3 h-3" />
                      Download
                    </a>
                  </td>
                  {showParseColumn && (
                    <td className="px-4 py-2">
                      <div className="flex items-center gap-2">
                        {reductoEnabled && (
                          <button
                            onClick={() => parseMutation.mutate(b.brochure_version_id)}
                            disabled={isParsing}
                            className="inline-flex items-center gap-1 px-2 py-1 text-xs font-medium rounded border border-gray-300 text-gray-700 hover:bg-gray-100 disabled:opacity-50"
                            title="Parse PDF via Reducto"
                          >
                            {isParsing ? (
                              <Loader2 className="w-3 h-3 animate-spin" />
                            ) : (
                              <FileSearch className="w-3 h-3" />
                            )}
                            {b.parse_status === 'success' ? 'Re-parse' : 'Parse'}
                          </button>
                        )}
                        {b.parse_status === 'success' && (
                          <button
                            onClick={() => setViewingVid(b.brochure_version_id)}
                            className="inline-flex items-center gap-1 text-xs text-green-600 hover:text-green-700"
                            title={`Parsed ${b.parsed_at ? new Date(b.parsed_at).toLocaleString() : ''}`}
                          >
                            <CheckCircle className="w-3 h-3" />
                            View
                          </button>
                        )}
                        {b.parse_status === 'failed' && (
                          <span className="inline-flex items-center gap-1 text-xs text-red-600" title="Last parse failed">
                            <XCircle className="w-3 h-3" />
                            Failed
                          </span>
                        )}
                      </div>
                    </td>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table>
      )}

      {viewingVid !== null && (
        <ParsedBrochureModal crd={crd} versionId={viewingVid} onClose={() => setViewingVid(null)} />
      )}
    </div>
  )
}

function ParsedBrochureModal({
  crd,
  versionId,
  onClose,
}: {
  crd: number
  versionId: number
  onClose: () => void
}) {
  const { data, isLoading, error } = useQuery({
    queryKey: ['brochure-parsed', crd, versionId],
    queryFn: () => getBrochureParsed(crd, versionId),
  })

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={onClose}>
      <div
        className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[85vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200">
          <div>
            <h3 className="text-sm font-semibold text-gray-800">Parsed Brochure</h3>
            <p className="text-xs text-gray-500">
              Version {versionId}
              {data?.parsed_at && ` · parsed ${new Date(data.parsed_at).toLocaleString()}`}
            </p>
          </div>
          <button onClick={onClose} className="p-1 text-gray-400 hover:text-gray-700">
            <X className="w-5 h-5" />
          </button>
        </div>
        <div className="overflow-auto p-5">
          {isLoading && <Skeleton className="h-64" />}
          {error && (
            <p className="text-sm text-red-600">Failed to load parsed content.</p>
          )}
          {data && data.parse_status !== 'success' && (
            <p className="text-sm text-gray-500">
              No parsed content available.{data.parse_error ? ` Error: ${data.parse_error}` : ''}
            </p>
          )}
          {data?.parsed_markdown && (
            <pre className="text-xs whitespace-pre-wrap font-mono text-gray-800 leading-relaxed">
              {data.parsed_markdown}
            </pre>
          )}
        </div>
      </div>
    </div>
  )
}

function PlatformTagsTab({
  firmPlatforms,
  allPlatforms,
  selectedPlatformId,
  onSelectPlatform,
  onAdd,
  onRemove,
  isAdding,
}: {
  crd: number
  firmPlatforms: Awaited<ReturnType<typeof getFirmPlatforms>>
  allPlatforms: Awaited<ReturnType<typeof getPlatforms>>
  selectedPlatformId: string
  onSelectPlatform: (id: string) => void
  onAdd: () => void
  onRemove: (tagId: number) => void
  isAdding: boolean
}) {
  const taggedPlatformIds = firmPlatforms.map((t) => t.platform_id)
  const availablePlatforms = allPlatforms.filter((p) => !taggedPlatformIds.includes(p.id))

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5 space-y-5">
      <div>
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Current Platform Tags</h3>
        {firmPlatforms.length === 0 ? (
          <p className="text-sm text-gray-400">No platform tags assigned.</p>
        ) : (
          <div className="flex flex-wrap gap-2">
            {firmPlatforms.map((tag) => (
              <div
                key={tag.id}
                className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-brand-100 text-brand-800 text-sm font-medium"
              >
                {tag.platform_name}
                <button
                  onClick={() => onRemove(tag.platform_id)}
                  className="hover:text-red-600 transition-colors"
                >
                  <X className="w-3 h-3" />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="border-t border-gray-100 pt-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Add Platform Tag</h3>
        {availablePlatforms.length === 0 ? (
          <p className="text-sm text-gray-400">All platforms already assigned.</p>
        ) : (
          <div className="flex items-center gap-3">
            <select
              value={selectedPlatformId}
              onChange={(e) => onSelectPlatform(e.target.value)}
              className="text-sm border border-gray-300 rounded-md px-3 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
            >
              <option value="">Select a platform...</option>
              {availablePlatforms.map((p) => (
                <option key={p.id} value={String(p.id)}>{p.name}</option>
              ))}
            </select>
            <Button onClick={onAdd} disabled={!selectedPlatformId || isAdding} size="sm" icon={<Plus className="w-4 h-4" />}>
              Add
            </Button>
          </div>
        )}
      </div>

      {firmPlatforms.length > 0 && (
        <div className="border-t border-gray-100 pt-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Tag Details</h3>
          <table className="w-full text-sm">
            <thead className="bg-gray-50 rounded">
              <tr>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Platform</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Tagged At</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Tagged By</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Notes</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {firmPlatforms.map((tag) => (
                <tr key={tag.id}>
                  <td className="px-3 py-2 font-medium">{tag.platform_name}</td>
                  <td className="px-3 py-2 text-gray-500">{formatDate(tag.tagged_at)}</td>
                  <td className="px-3 py-2 text-gray-500">{tag.tagged_by ?? '—'}</td>
                  <td className="px-3 py-2 text-gray-500">{tag.notes ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function ChangeHistoryTab({ history }: { history: Awaited<ReturnType<typeof getFirmHistory>> }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="px-5 py-3 border-b border-gray-200">
        <h3 className="text-sm font-semibold text-gray-700">Change History</h3>
      </div>
      {history.changes.length === 0 ? (
        <p className="px-5 py-6 text-sm text-gray-400">No changes recorded for this firm.</p>
      ) : (
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Field</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Change</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Detected At</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {history.changes.map((change) => (
              <tr key={change.id} className="hover:bg-gray-50">
                <td className="px-4 py-2 font-mono text-xs text-gray-700">{change.field_path}</td>
                <td className="px-4 py-2">
                  <div className="flex items-center gap-2 text-sm">
                    <span className="text-red-600 bg-red-50 px-1.5 py-0.5 rounded text-xs font-mono">
                      {change.old_value ?? 'null'}
                    </span>
                    <span className="text-gray-400">→</span>
                    <span className="text-green-700 bg-green-50 px-1.5 py-0.5 rounded text-xs font-mono">
                      {change.new_value ?? 'null'}
                    </span>
                  </div>
                </td>
                <td className="px-4 py-2 text-gray-500 text-xs">{formatDate(change.detected_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Questionnaire preview panel
// ---------------------------------------------------------------------------

function QuestionnairePreview({
  crd,
  templateId,
  templateName,
  onClose,
}: {
  crd: number
  templateId: number
  templateName: string
  onClose: () => void
}) {
  const { addToast } = useToast()
  const queryClient = useQueryClient()
  const [pendingAnswers, setPendingAnswers] = useState<Record<string, string>>({})
  const [pendingNotes, setPendingNotes] = useState<Record<string, string>>({})

  const { data: response, isLoading } = useQuery({
    queryKey: ['firm-questionnaire', crd, templateId],
    queryFn: () => getFirmQuestionnaire(crd, templateId),
  })

  const saveMutation = useMutation({
    mutationFn: (data: { answers?: Record<string, string>; analyst_notes?: Record<string, string> }) =>
      updateFirmQuestionnaireAnswers(crd, templateId, data),
    onSuccess: (updated) => {
      queryClient.setQueryData(['firm-questionnaire', crd, templateId], updated)
      setPendingAnswers({})
      setPendingNotes({})
      addToast('Answers saved', 'success')
    },
    onError: () => addToast('Failed to save answers', 'error'),
  })

  const regenerateMutation = useMutation({
    mutationFn: () => regenerateFirmQuestionnaire(crd, templateId),
    onSuccess: (updated) => {
      queryClient.setQueryData(['firm-questionnaire', crd, templateId], updated)
      addToast('Answers regenerated from current firm data', 'success')
    },
    onError: () => addToast('Failed to regenerate answers', 'error'),
  })

  const handleAnswerBlur = (questionId: string, value: string) => {
    const current = response?.answers?.[questionId] ?? ''
    if (value !== current) {
      setPendingAnswers(prev => ({ ...prev, [questionId]: value }))
    }
  }

  const handleNoteBlur = (questionId: string, value: string) => {
    const current = response?.analyst_notes?.[questionId] ?? ''
    if (value !== current) {
      setPendingNotes(prev => ({ ...prev, [questionId]: value }))
    }
  }

  const hasPending = Object.keys(pendingAnswers).length > 0 || Object.keys(pendingNotes).length > 0

  const questions: QuestionnaireQuestionOut[] = response?.template?.questions ?? []

  const sections: Record<string, QuestionnaireQuestionOut[]> = {}
  for (const q of [...questions].sort((a, b) => (a.section < b.section ? -1 : 0) || a.order_index - b.order_index)) {
    ;(sections[q.section] ??= []).push(q)
  }

  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200 bg-gray-50">
        <div>
          <h3 className="text-sm font-semibold text-gray-800">{templateName}</h3>
          {response && (
            <p className="text-xs text-gray-400 mt-0.5">
              {questions.length} questions · Last updated {formatDate(response.generated_at)}
              {' · '}
              <span className={`font-medium ${response.status === 'final' ? 'text-green-600' : 'text-amber-500'}`}>
                {response.status}
              </span>
            </p>
          )}
        </div>
        <div className="flex items-center gap-2">
          {hasPending && (
            <Button
              size="sm"
              onClick={() => saveMutation.mutate({ answers: pendingAnswers, analyst_notes: pendingNotes })}
              disabled={saveMutation.isPending}
            >
              {saveMutation.isPending ? <Loader2 size={12} className="animate-spin mr-1" /> : null}
              Save changes
            </Button>
          )}
          <Button
            size="sm"
            variant="outline"
            onClick={() => regenerateMutation.mutate()}
            disabled={regenerateMutation.isPending}
            title="Re-resolve all auto-populated answers from current firm data"
          >
            <RefreshCw size={12} className={`mr-1 ${regenerateMutation.isPending ? 'animate-spin' : ''}`} />
            Regenerate
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => window.open(`/api/firms/${crd}/questionnaires/${templateId}/excel`, '_blank')}
          >
            <Download size={12} className="mr-1" /> Excel
          </Button>
          <button onClick={onClose} className="p-1 text-gray-400 hover:text-gray-600">
            <X size={16} />
          </button>
        </div>
      </div>

      {isLoading ? (
        <div className="p-6"><Skeleton /></div>
      ) : !response ? (
        <p className="px-5 py-6 text-sm text-gray-400">Failed to load questionnaire.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 w-8">#</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 w-1/2">Question</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 w-1/4">Answer</th>
                <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 w-1/4">Analyst Notes</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {Object.entries(sections).map(([section, qs]) => (
                <>
                  <tr key={`section-${section}`} className="bg-blue-50">
                    <td colSpan={4} className="px-3 py-1.5 text-xs font-semibold text-blue-700">{section}</td>
                  </tr>
                  {qs.map((q, idx) => {
                    const qId = String(q.id)
                    const storedAnswer = response.answers?.[qId] ?? ''
                    const storedNote = response.analyst_notes?.[qId] ?? ''
                    const isAutoFilled = Boolean(q.answer_field_path && storedAnswer && storedAnswer !== 'N/A')
                    const localAnswer = pendingAnswers[qId] ?? storedAnswer
                    const localNote = pendingNotes[qId] ?? storedNote

                    return (
                      <tr key={q.id} className="hover:bg-gray-50">
                        <td className="px-3 py-2 text-xs text-gray-400">{idx + 1}</td>
                        <td className="px-3 py-2 text-sm text-gray-800">
                          {q.question_text}
                          {q.answer_hint && !isAutoFilled && (
                            <span className="ml-1 text-xs text-gray-400 italic">({q.answer_hint})</span>
                          )}
                        </td>
                        <td className="px-3 py-2">
                          <textarea
                            defaultValue={localAnswer}
                            onBlur={e => handleAnswerBlur(qId, e.target.value)}
                            rows={1}
                            className={`w-full text-sm rounded px-2 py-1 border focus:outline-none focus:ring-1 focus:ring-brand-500 resize-none ${
                              isAutoFilled
                                ? 'bg-blue-50 border-blue-100 text-blue-900'
                                : 'bg-yellow-50 border-yellow-100'
                            }`}
                          />
                        </td>
                        <td className="px-3 py-2">
                          {q.notes_enabled ? (
                            <textarea
                              defaultValue={localNote}
                              onBlur={e => handleNoteBlur(qId, e.target.value)}
                              rows={1}
                              placeholder="Add note..."
                              className="w-full text-sm bg-gray-50 border border-gray-200 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-brand-500 resize-none"
                            />
                          ) : (
                            <span className="text-gray-300 text-xs">—</span>
                          )}
                        </td>
                      </tr>
                    )
                  })}
                </>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function QuestionnairesTab({
  crd,
  questionnaires,
}: {
  crd: number
  questionnaires: Awaited<ReturnType<typeof getFirmQuestionnaires>>
}) {
  const [activeTemplateId, setActiveTemplateId] = useState<number | null>(null)

  if (questionnaires.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-center bg-white rounded-lg border border-gray-200">
        <FileQuestion size={40} className="text-gray-200 mb-3" />
        <p className="text-gray-500 font-medium">No questionnaire templates configured.</p>
        <p className="text-sm text-gray-400 mt-1">
          Go to{' '}
          <Link to="/questionnaires" className="text-brand-600 hover:underline">
            Questionnaires
          </Link>{' '}
          to create templates, or run <code className="bg-gray-100 px-1 rounded">make seed</code>.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {questionnaires.map(item => (
          <button
            key={item.template_id}
            onClick={() => setActiveTemplateId(
              activeTemplateId === item.template_id ? null : item.template_id
            )}
            className={`text-left p-4 rounded-lg border transition-all ${
              activeTemplateId === item.template_id
                ? 'border-brand-500 bg-brand-50 shadow-sm'
                : 'border-gray-200 bg-white hover:border-gray-300 hover:shadow-sm'
            }`}
          >
            <div className="flex items-start justify-between">
              <div className="font-medium text-sm text-gray-800">{item.template_name}</div>
              {item.has_response ? (
                <span className={`ml-2 text-xs px-1.5 py-0.5 rounded shrink-0 ${
                  item.response_status === 'final'
                    ? 'bg-green-100 text-green-700'
                    : 'bg-amber-100 text-amber-700'
                }`}>
                  {item.response_status}
                </span>
              ) : (
                <span className="ml-2 text-xs px-1.5 py-0.5 rounded bg-gray-100 text-gray-400 shrink-0">new</span>
              )}
            </div>
            {item.description && (
              <p className="text-xs text-gray-400 mt-1 line-clamp-2">{item.description}</p>
            )}
            <div className="text-xs text-gray-400 mt-2">
              {item.question_count} question{item.question_count !== 1 ? 's' : ''}
              {item.response_generated_at && ` · ${formatDate(item.response_generated_at)}`}
            </div>
          </button>
        ))}
      </div>

      {activeTemplateId && (
        <QuestionnairePreview
          crd={crd}
          templateId={activeTemplateId}
          templateName={questionnaires.find(q => q.template_id === activeTemplateId)?.template_name ?? ''}
          onClose={() => setActiveTemplateId(null)}
        />
      )}
    </div>
  )
}
