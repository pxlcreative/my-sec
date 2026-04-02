import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from 'recharts'
import { ArrowLeft, Download, ExternalLink, FileText, Plus, X } from 'lucide-react'
import {
  getFirm,
  getFirmHistory,
  getFirmAumHistory,
  getFirmBrochures,
  getFirmPlatforms,
  getPlatforms,
  addFirmPlatform,
  removeFirmPlatform,
} from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { StatusBadge } from '../components/StatusBadge'
import { useToast } from '../components/Toast'
import { formatAum, formatDate } from '../utils'
import type { FirmDetail as FirmDetailType } from '../types'

type TabKey = 'overview' | 'adv' | 'aum' | 'brochures' | 'platforms' | 'history'

const TABS: { key: TabKey; label: string }[] = [
  { key: 'overview', label: 'Overview' },
  { key: 'adv', label: 'ADV Data' },
  { key: 'aum', label: 'AUM History' },
  { key: 'brochures', label: 'Brochures' },
  { key: 'platforms', label: 'Platform Tags' },
  { key: 'history', label: 'Change History' },
]

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex gap-2 py-2 border-b border-gray-100 last:border-0">
      <dt className="w-44 flex-shrink-0 text-sm font-medium text-gray-500">{label}</dt>
      <dd className="text-sm text-gray-900 min-w-0 break-words">{value ?? '—'}</dd>
    </div>
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
          <div className="flex items-center gap-3 mt-2">
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
            {firm.main_city && firm.main_state && (
              <span className="text-sm text-gray-500">
                {firm.main_city}, {firm.main_state}
              </span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleDownloadDDQ}
            className="flex items-center gap-2 px-3 py-2 text-sm bg-brand-600 text-white rounded-lg hover:bg-brand-700"
          >
            <Download className="w-4 h-4" />
            DDQ Excel
          </button>
          <button
            onClick={handleDownloadBrochure}
            className="flex items-center gap-2 px-3 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-50 text-gray-700"
          >
            <FileText className="w-4 h-4" />
            Latest Brochure
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-200 mb-6">
        <nav className="flex gap-0 -mb-px">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.key
                  ? 'border-brand-600 text-brand-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab content */}
      {activeTab === 'overview' && <OverviewTab firm={firm} />}
      {activeTab === 'adv' && <AdvDataTab firm={firm} />}
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
    </div>
  )
}

function OverviewTab({ firm }: { firm: FirmDetailType }) {
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
          <DetailRow label="Firm Type" value={firm.firm_type} />
          <DetailRow label="Org Type" value={firm.org_type} />
          <DetailRow label="Last Filing Date" value={formatDate(firm.last_filing_date)} />
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
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Record Info</h3>
        <dl>
          <DetailRow label="Created At" value={formatDate(firm.created_at)} />
          <DetailRow label="Updated At" value={formatDate(firm.updated_at)} />
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

function AdvDataTab({ firm }: { firm: FirmDetailType }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">ADV Part 1 Data</h3>
      <dl className="grid grid-cols-1 md:grid-cols-2 gap-x-6">
        <DetailRow label="CRD Number" value={firm.crd_number} />
        <DetailRow label="SEC Number" value={firm.sec_number} />
        <DetailRow label="Legal Name" value={firm.legal_name} />
        <DetailRow label="Business Name" value={firm.business_name} />
        <DetailRow label="Registration Status" value={
          firm.registration_status ? <StatusBadge status={firm.registration_status} /> : null
        } />
        <DetailRow label="Firm Type" value={firm.firm_type} />
        <DetailRow label="Org Type" value={firm.org_type} />
        <DetailRow label="Total AUM" value={formatAum(firm.aum_total)} />
        <DetailRow label="Discretionary AUM" value={formatAum(firm.aum_discretionary)} />
        <DetailRow label="Non-Discretionary AUM" value={formatAum(firm.aum_non_discretionary)} />
        <DetailRow label="Num. Accounts" value={firm.num_accounts?.toLocaleString()} />
        <DetailRow label="Num. Employees" value={firm.num_employees?.toLocaleString()} />
        <DetailRow label="Fiscal Year End" value={firm.fiscal_year_end} />
        <DetailRow label="Last Filing Date" value={formatDate(firm.last_filing_date)} />
        <DetailRow label="Main Address" value={[firm.main_street1, firm.main_city, firm.main_state, firm.main_zip].filter(Boolean).join(', ')} />
        <DetailRow label="Phone" value={firm.phone} />
        <DetailRow label="Website" value={firm.website} />
      </dl>
    </div>
  )
}

function AumHistoryTab({ aumHistory }: { aumHistory: ReturnType<typeof getFirmAumHistory> extends Promise<infer T> ? T : never }) {
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
            <Line
              type="monotone"
              dataKey="aum_total"
              stroke="#2563eb"
              name="Total AUM"
              dot={false}
              strokeWidth={2}
            />
            <Line
              type="monotone"
              dataKey="aum_discretionary"
              stroke="#16a34a"
              name="Discretionary"
              dot={false}
              strokeWidth={1.5}
            />
            <Line
              type="monotone"
              dataKey="aum_non_discretionary"
              stroke="#ea580c"
              name="Non-Discretionary"
              dot={false}
              strokeWidth={1.5}
            />
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
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {brochures.map((b) => (
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
                  {b.file_size_bytes
                    ? `${(b.file_size_bytes / 1024).toFixed(0)} KB`
                    : '—'}
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
              </tr>
            ))}
          </tbody>
        </table>
      )}
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
            <button
              onClick={onAdd}
              disabled={!selectedPlatformId || isAdding}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-brand-600 text-white text-sm rounded-md hover:bg-brand-700 disabled:opacity-50"
            >
              <Plus className="w-4 h-4" />
              Add
            </button>
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
