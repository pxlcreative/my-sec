import { useState, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus, Trash2, Play, Loader2, Bell, BellOff, X, ChevronDown,
} from 'lucide-react'
import {
  AreaChart, Area, BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'
import {
  getAlertRules,
  createAlertRule,
  deleteAlertRule,
  updateAlertRule,
  testAlertRule,
  getAlertEvents,
  getPlatforms,
} from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { useToast } from '../components/Toast'
import { formatDate } from '../utils'
import type { AlertRuleOut, AlertEventOut } from '../types'

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RULE_TYPES = [
  { value: 'deregistration', label: 'Deregistration' },
  { value: 'aum_decline', label: 'AUM Decline %' },
  { value: 'field_change', label: 'Field Change' },
]

const DELIVERY_TYPES = [
  { value: 'email', label: 'Email' },
  { value: 'webhook', label: 'Webhook' },
  { value: 'log', label: 'Log Only' },
]

const DELIVERY_STATUSES = ['sent', 'failed', 'pending', 'test', 'logged']

const TYPE_COLORS: Record<string, string> = {
  deregistration: '#ef4444',
  aum_decline: '#f59e0b',
  field_change: '#3b82f6',
}

// ---------------------------------------------------------------------------
// Shared badges
// ---------------------------------------------------------------------------

function RuleBadge({ active }: { active: boolean }) {
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${
      active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-500'
    }`}>
      {active ? <Bell className="w-3 h-3" /> : <BellOff className="w-3 h-3" />}
      {active ? 'Active' : 'Paused'}
    </span>
  )
}

function DeliveryBadge({ delivery }: { delivery: string }) {
  const colors: Record<string, string> = {
    email: 'bg-blue-100 text-blue-800',
    webhook: 'bg-purple-100 text-purple-800',
    log: 'bg-gray-100 text-gray-600',
  }
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[delivery] ?? 'bg-gray-100 text-gray-600'}`}>
      {delivery}
    </span>
  )
}

function EventStatusBadge({ status }: { status: string | null }) {
  if (!status) return <span className="text-gray-400">—</span>
  const colors: Record<string, string> = {
    sent: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',
    test: 'bg-yellow-100 text-yellow-800',
    logged: 'bg-gray-100 text-gray-600',
    pending: 'bg-blue-100 text-blue-700',
  }
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[status] ?? 'bg-gray-100 text-gray-600'}`}>
      {status}
    </span>
  )
}

function StatCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">{label}</p>
      <p className="text-2xl font-bold text-gray-900">{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-1">{sub}</p>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function Alerts() {
  const [tab, setTab] = useState<'rules' | 'events'>('rules')

  const { data: rules } = useQuery({
    queryKey: ['alert-rules-all'],
    queryFn: () => getAlertRules({ active_only: false } as Record<string, unknown>),
  })

  const activeCount = rules?.filter((r) => r.active).length ?? 0
  const totalCount = rules?.length ?? 0

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Alerts</h1>
      </div>

      <div className="flex gap-1 border-b border-gray-200 mb-6">
        {(['rules', 'events'] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
              tab === t
                ? 'border-brand-600 text-brand-600'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            {t === 'rules'
              ? `Alert Rules${totalCount > 0 ? ` (${totalCount})` : ''}`
              : 'Event Feed'}
          </button>
        ))}
      </div>

      {tab === 'rules' ? <RulesTab rules={rules} activeCount={activeCount} /> : <EventsTab rules={rules ?? []} />}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Rules tab
// ---------------------------------------------------------------------------

function RulesTab({ rules, activeCount }: { rules: AlertRuleOut[] | undefined; activeCount: number }) {
  const queryClient = useQueryClient()
  const { addToast } = useToast()

  const [activeOnly, setActiveOnly] = useState(false)
  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState({
    label: '',
    rule_type: 'deregistration',
    delivery: 'email',
    delivery_target: '',
    threshold_pct: '',
    field_path: '',
    platform_ids: [] as number[],
  })
  const [testingId, setTestingId] = useState<number | null>(null)

  const { data: platforms } = useQuery({ queryKey: ['platforms'], queryFn: getPlatforms })

  const isLoading = rules === undefined

  const displayedRules = activeOnly ? (rules ?? []).filter((r) => r.active) : (rules ?? [])

  const createMutation = useMutation({
    mutationFn: () => {
      const payload: Record<string, unknown> = {
        label: form.label.trim(),
        rule_type: form.rule_type,
        delivery: form.delivery,
      }
      if (form.delivery_target.trim()) payload.delivery_target = form.delivery_target.trim()
      if (form.rule_type === 'aum_decline' && form.threshold_pct) payload.threshold_pct = Number(form.threshold_pct)
      if (form.rule_type === 'field_change' && form.field_path.trim()) payload.field_path = form.field_path.trim()
      if (form.platform_ids.length > 0) payload.platform_ids = form.platform_ids
      return createAlertRule(payload)
    },
    onSuccess: (rule) => {
      queryClient.invalidateQueries({ queryKey: ['alert-rules-all'] })
      addToast(`Rule "${rule.label}" created`, 'success')
      setShowForm(false)
      setForm({ label: '', rule_type: 'deregistration', delivery: 'email', delivery_target: '', threshold_pct: '', field_path: '', platform_ids: [] })
    },
    onError: () => addToast('Failed to create rule', 'error'),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteAlertRule,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alert-rules-all'] })
      addToast('Rule deleted', 'success')
    },
    onError: () => addToast('Failed to delete rule', 'error'),
  })

  const toggleMutation = useMutation({
    mutationFn: ({ id, active }: { id: number; active: boolean }) =>
      updateAlertRule(id, { active }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['alert-rules-all'] }),
    onError: () => addToast('Failed to update rule', 'error'),
  })

  async function handleTest(rule: AlertRuleOut) {
    setTestingId(rule.id)
    try {
      const result = await testAlertRule(rule.id)
      addToast(result.success ? `Test sent: ${result.message}` : `Test failed: ${result.message}`, result.success ? 'success' : 'error')
    } catch {
      addToast('Test request failed', 'error')
    } finally {
      setTestingId(null)
    }
  }

  function togglePlatform(id: number) {
    setForm((prev) => ({
      ...prev,
      platform_ids: prev.platform_ids.includes(id)
        ? prev.platform_ids.filter((p) => p !== id)
        : [...prev.platform_ids, id],
    }))
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <label className="flex items-center gap-2 text-sm text-gray-600 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={activeOnly}
            onChange={(e) => setActiveOnly(e.target.checked)}
            className="rounded text-brand-600"
          />
          Active only ({activeCount})
        </label>
        <button
          onClick={() => setShowForm((v) => !v)}
          className="flex items-center gap-2 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700"
        >
          <Plus className="w-4 h-4" />
          New Rule
        </button>
      </div>

      {showForm && (
        <div className="bg-white border border-gray-200 rounded-lg p-5 mb-6 max-w-xl">
          <h2 className="text-sm font-semibold text-gray-700 mb-4">Create Alert Rule</h2>
          <div className="space-y-3">
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Label <span className="text-red-500">*</span></label>
              <input
                type="text"
                value={form.label}
                onChange={(e) => setForm((f) => ({ ...f, label: e.target.value }))}
                placeholder="e.g. Deregistration Alert"
                className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
              />
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Rule Type</label>
                <select
                  value={form.rule_type}
                  onChange={(e) => setForm((f) => ({ ...f, rule_type: e.target.value }))}
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                >
                  {RULE_TYPES.map((r) => <option key={r.value} value={r.value}>{r.label}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Delivery</label>
                <select
                  value={form.delivery}
                  onChange={(e) => setForm((f) => ({ ...f, delivery: e.target.value }))}
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                >
                  {DELIVERY_TYPES.map((d) => <option key={d.value} value={d.value}>{d.label}</option>)}
                </select>
              </div>
            </div>

            {form.rule_type === 'aum_decline' && (
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Threshold % (decline)</label>
                <input type="number" min="1" max="100" value={form.threshold_pct}
                  onChange={(e) => setForm((f) => ({ ...f, threshold_pct: e.target.value }))}
                  placeholder="e.g. 20"
                  className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                />
              </div>
            )}

            {form.rule_type === 'field_change' && (
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Field Path</label>
                <input type="text" value={form.field_path}
                  onChange={(e) => setForm((f) => ({ ...f, field_path: e.target.value }))}
                  placeholder="e.g. registration_status"
                  className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                />
              </div>
            )}

            {(form.delivery === 'email' || form.delivery === 'webhook') && (
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">
                  {form.delivery === 'email' ? 'Email Address' : 'Webhook URL'}
                </label>
                <input
                  type={form.delivery === 'email' ? 'email' : 'url'}
                  value={form.delivery_target}
                  onChange={(e) => setForm((f) => ({ ...f, delivery_target: e.target.value }))}
                  placeholder={form.delivery === 'email' ? 'you@example.com' : 'https://hooks.example.com/...'}
                  className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                />
              </div>
            )}

            {platforms && platforms.length > 0 && (
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Scope to Platforms (leave blank for all)</label>
                <div className="flex flex-wrap gap-2">
                  {platforms.map((p) => (
                    <label key={p.id} className="flex items-center gap-1.5 cursor-pointer">
                      <input type="checkbox" checked={form.platform_ids.includes(p.id)} onChange={() => togglePlatform(p.id)} className="rounded text-brand-600" />
                      <span className="text-sm text-gray-700">{p.name}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}

            <div className="flex gap-2 pt-1">
              <button
                onClick={() => createMutation.mutate()}
                disabled={!form.label.trim() || createMutation.isPending}
                className="flex items-center gap-2 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
              >
                {createMutation.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
                Create
              </button>
              <button onClick={() => setShowForm(false)} className="px-4 py-2 text-sm text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50">
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {isLoading ? (
        <div className="space-y-3">{[1, 2, 3].map((i) => <Skeleton key={i} className="h-16" />)}</div>
      ) : displayedRules.length === 0 ? (
        <div className="bg-white border border-gray-200 rounded-lg p-10 text-center text-gray-400">
          <Bell className="w-10 h-10 mx-auto mb-3 opacity-50" />
          <p className="font-medium">No alert rules yet</p>
          <p className="text-sm mt-1">Create a rule to start monitoring firms</p>
        </div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Label</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Type</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Delivery</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Target</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Status</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Created</th>
                <th className="px-4 py-3 w-32"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {displayedRules.map((rule) => (
                <tr key={rule.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-medium text-gray-900">
                    {rule.label}
                    {rule.threshold_pct != null && <span className="ml-2 text-xs text-gray-400">≥{rule.threshold_pct}%</span>}
                  </td>
                  <td className="px-4 py-3 text-gray-600">{rule.rule_type.replace(/_/g, ' ')}</td>
                  <td className="px-4 py-3"><DeliveryBadge delivery={rule.delivery} /></td>
                  <td className="px-4 py-3 text-gray-500 text-xs font-mono max-w-[180px] truncate">{rule.delivery_target ?? '—'}</td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => toggleMutation.mutate({ id: rule.id, active: !rule.active })}
                      disabled={toggleMutation.isPending}
                      className="disabled:opacity-50"
                    >
                      <RuleBadge active={rule.active} />
                    </button>
                  </td>
                  <td className="px-4 py-3 text-gray-400 text-xs">{formatDate(rule.created_at)}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2 justify-end">
                      <button onClick={() => handleTest(rule)} disabled={testingId === rule.id} title="Send test alert"
                        className="p-1.5 rounded text-gray-400 hover:text-brand-600 hover:bg-brand-50 disabled:opacity-50">
                        {testingId === rule.id ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                      </button>
                      <button onClick={() => deleteMutation.mutate(rule.id)} disabled={deleteMutation.isPending} title="Delete rule"
                        className="p-1.5 rounded text-gray-400 hover:text-red-600 hover:bg-red-50 disabled:opacity-50">
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Events tab
// ---------------------------------------------------------------------------

type DatePreset = '7d' | '30d' | '90d' | 'custom' | 'all'

function presetToSince(preset: DatePreset): string {
  if (preset === 'all' || preset === 'custom') return ''
  const days = preset === '7d' ? 7 : preset === '30d' ? 30 : 90
  const d = new Date()
  d.setDate(d.getDate() - days)
  return d.toISOString().slice(0, 10)
}

function EventsTab({ rules }: { rules: AlertRuleOut[] }) {
  const navigate = useNavigate()

  // --- API-side filters ---
  const [ruleId, setRuleId] = useState('')
  const [deliveryStatus, setDeliveryStatus] = useState('')
  const [platformFilter, setPlatformFilter] = useState('')
  const [datePreset, setDatePreset] = useState<DatePreset>('30d')
  const [customSince, setCustomSince] = useState('')
  const [customUntil, setCustomUntil] = useState('')
  const [offset, setOffset] = useState(0)
  const [allEvents, setAllEvents] = useState<AlertEventOut[]>([])

  // --- Client-side filters ---
  const [typeFilter, setTypeFilter] = useState('')
  const [firmSearch, setFirmSearch] = useState('')

  const { data: platforms } = useQuery({ queryKey: ['platforms'], queryFn: getPlatforms })

  const since = datePreset === 'custom' ? customSince : presetToSince(datePreset)
  const until = datePreset === 'custom' ? customUntil : ''

  const apiParams: Record<string, unknown> = { limit: 500, offset }
  if (ruleId) apiParams.rule_id = ruleId
  if (deliveryStatus) apiParams.delivery_status = deliveryStatus
  if (platformFilter) apiParams.platform = platformFilter
  if (since) apiParams.since = since
  if (until) apiParams.until = until

  const { data: page, isLoading } = useQuery({
    queryKey: ['alert-events', apiParams],
    queryFn: () => getAlertEvents(apiParams),
    select: (data) => data as AlertEventOut[],
  })

  // Accumulate pages for load-more
  const events: AlertEventOut[] = useMemo(() => {
    if (!page) return allEvents
    if (offset === 0) return page
    const ids = new Set(allEvents.map((e) => e.id))
    return [...allEvents, ...page.filter((e) => !ids.has(e.id))]
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page])

  // Keep allEvents in sync so load-more works
  useMemo(() => { if (page) setAllEvents(events) }, [events, page])

  // Client-side filtering
  const filtered = useMemo(() => {
    return events.filter((e) => {
      if (typeFilter && e.rule_type !== typeFilter) return false
      if (firmSearch) {
        const q = firmSearch.toLowerCase()
        const name = (e.firm_name ?? '').toLowerCase()
        const crd = String(e.crd_number)
        if (!name.includes(q) && !crd.includes(q)) return false
      }
      return true
    })
  }, [events, typeFilter, firmSearch])

  // Reset offset + accumulated list when primary filters change
  function resetFilters() {
    setOffset(0)
    setAllEvents([])
  }

  // Stats derived from filtered events
  const now = new Date()
  const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
  const thisWeek = filtered.filter((e) => new Date(e.fired_at) >= weekAgo).length
  const sentCount = filtered.filter((e) => e.delivery_status === 'sent').length
  const failedCount = filtered.filter((e) => e.delivery_status === 'failed').length
  const deliveryRate = sentCount + failedCount > 0
    ? Math.round((sentCount / (sentCount + failedCount)) * 100)
    : null

  const ruleCountMap = filtered.reduce<Record<string, { label: string; count: number }>>((acc, e) => {
    const rule = rules.find((r) => r.id === e.rule_id)
    const label = rule?.label ?? `Rule ${e.rule_id}`
    acc[e.rule_id] = { label, count: (acc[e.rule_id]?.count ?? 0) + 1 }
    return acc
  }, {})
  const mostActiveRule = Object.values(ruleCountMap).sort((a, b) => b.count - a.count)[0]

  // Chart data: events per day
  const timelineData = useMemo(() => {
    const counts: Record<string, number> = {}
    filtered.forEach((e) => {
      const day = e.fired_at.slice(0, 10)
      counts[day] = (counts[day] ?? 0) + 1
    })
    return Object.entries(counts)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, count]) => ({
        date: new Date(date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        count,
      }))
  }, [filtered])

  // Chart data: by rule type
  const typeData = useMemo(() => {
    return ['deregistration', 'aum_decline', 'field_change'].map((type) => ({
      name: type.replace(/_/g, ' '),
      type,
      count: filtered.filter((e) => e.rule_type === type).length,
    }))
  }, [filtered])

  // Active filter chips
  const chips: { label: string; clear: () => void }[] = []
  if (ruleId) {
    const rule = rules.find((r) => String(r.id) === ruleId)
    chips.push({ label: `Rule: ${rule?.label ?? ruleId}`, clear: () => { setRuleId(''); resetFilters() } })
  }
  if (typeFilter) chips.push({ label: `Type: ${typeFilter.replace(/_/g, ' ')}`, clear: () => setTypeFilter('') })
  if (deliveryStatus) chips.push({ label: `Status: ${deliveryStatus}`, clear: () => { setDeliveryStatus(''); resetFilters() } })
  if (platformFilter) chips.push({ label: `Platform: ${platformFilter}`, clear: () => { setPlatformFilter(''); resetFilters() } })
  if (firmSearch) chips.push({ label: `Firm: "${firmSearch}"`, clear: () => setFirmSearch('') })

  const hasMore = page?.length === 500

  return (
    <div className="space-y-4">
      {/* Filter bar */}
      <div className="bg-white border border-gray-200 rounded-lg p-4">
        {/* Row 1 — dropdowns + text search */}
        <div className="flex flex-wrap gap-3 items-end">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Rule</label>
            <select value={ruleId} onChange={(e) => { setRuleId(e.target.value); resetFilters() }}
              className="text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none">
              <option value="">All Rules</option>
              {rules.map((r) => <option key={r.id} value={String(r.id)}>{r.label}</option>)}
            </select>
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Alert Type</label>
            <select value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)}
              className="text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none">
              <option value="">All Types</option>
              {RULE_TYPES.map((r) => <option key={r.value} value={r.value}>{r.label}</option>)}
            </select>
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Delivery Status</label>
            <select value={deliveryStatus} onChange={(e) => { setDeliveryStatus(e.target.value); resetFilters() }}
              className="text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none">
              <option value="">All Statuses</option>
              {DELIVERY_STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>

          {platforms && platforms.length > 0 && (
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">Platform</label>
              <select value={platformFilter} onChange={(e) => { setPlatformFilter(e.target.value); resetFilters() }}
                className="text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none">
                <option value="">All Platforms</option>
                {platforms.map((p) => <option key={p.id} value={p.name}>{p.name}</option>)}
              </select>
            </div>
          )}

          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Firm</label>
            <input type="text" value={firmSearch} onChange={(e) => setFirmSearch(e.target.value)}
              placeholder="Search name or CRD…"
              className="w-44 text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
            />
          </div>
        </div>

        {/* Row 2 — date range presets */}
        <div className="flex flex-wrap items-center gap-2 mt-3">
          <span className="text-xs font-medium text-gray-500">Range:</span>
          {(['7d', '30d', '90d', 'all', 'custom'] as DatePreset[]).map((p) => (
            <button key={p} onClick={() => { setDatePreset(p); resetFilters() }}
              className={`px-3 py-1 rounded-full text-xs font-medium border transition-colors ${
                datePreset === p
                  ? 'bg-brand-600 text-white border-brand-600'
                  : 'bg-white text-gray-600 border-gray-300 hover:border-brand-400'
              }`}>
              {p === 'all' ? 'All time' : p === 'custom' ? 'Custom' : p}
            </button>
          ))}
          {datePreset === 'custom' && (
            <div className="flex items-center gap-2 ml-1">
              <input type="date" value={customSince} onChange={(e) => { setCustomSince(e.target.value); resetFilters() }}
                className="text-sm border border-gray-300 rounded-md px-2 py-1 focus:ring-2 focus:ring-brand-600 outline-none"
              />
              <span className="text-gray-400 text-xs">to</span>
              <input type="date" value={customUntil} onChange={(e) => { setCustomUntil(e.target.value); resetFilters() }}
                className="text-sm border border-gray-300 rounded-md px-2 py-1 focus:ring-2 focus:ring-brand-600 outline-none"
              />
            </div>
          )}
        </div>

        {/* Active filter chips */}
        {chips.length > 0 && (
          <div className="flex flex-wrap items-center gap-2 mt-3 pt-3 border-t border-gray-100">
            {chips.map((chip) => (
              <span key={chip.label} className="inline-flex items-center gap-1 px-2.5 py-1 bg-brand-50 text-brand-700 rounded-full text-xs font-medium">
                {chip.label}
                <button onClick={chip.clear} className="hover:text-brand-900"><X className="w-3 h-3" /></button>
              </span>
            ))}
            <button onClick={() => {
              setRuleId(''); setTypeFilter(''); setDeliveryStatus(''); setPlatformFilter('')
              setFirmSearch(''); setDatePreset('30d'); resetFilters()
            }} className="text-xs text-gray-400 hover:text-gray-600 underline">
              Clear all
            </button>
          </div>
        )}
      </div>

      {isLoading && offset === 0 ? (
        <div className="space-y-3">
          <div className="grid grid-cols-4 gap-4">{[1,2,3,4].map((i) => <Skeleton key={i} className="h-20" />)}</div>
          <Skeleton className="h-52" />
        </div>
      ) : (
        <>
          {/* Stat widgets */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard label="Events" value={filtered.length} sub={filtered.length !== events.length ? `of ${events.length} loaded` : undefined} />
            <StatCard
              label="Delivery Rate"
              value={deliveryRate !== null ? `${deliveryRate}%` : '—'}
              sub={deliveryRate !== null ? `${sentCount} sent, ${failedCount} failed` : 'no sent/failed events'}
            />
            <StatCard
              label="Most Active Rule"
              value={mostActiveRule?.count ?? 0}
              sub={mostActiveRule?.label ?? 'No events'}
            />
            <StatCard label="This Week" value={thisWeek} sub="events in last 7 days" />
          </div>

          {/* Charts */}
          {filtered.length > 0 && (
            <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
              {/* Timeline */}
              <div className="lg:col-span-3 bg-white border border-gray-200 rounded-lg p-4">
                <h3 className="text-sm font-semibold text-gray-700 mb-4">Events Over Time</h3>
                {timelineData.length < 2 ? (
                  <div className="h-40 flex items-center justify-center text-gray-400 text-sm">Not enough data points</div>
                ) : (
                  <ResponsiveContainer width="100%" height={160}>
                    <AreaChart data={timelineData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
                      <defs>
                        <linearGradient id="eventsGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#2563eb" stopOpacity={0.2} />
                          <stop offset="95%" stopColor="#2563eb" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                      <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                      <YAxis tick={{ fontSize: 10 }} allowDecimals={false} />
                      <Tooltip contentStyle={{ fontSize: 12 }} />
                      <Area type="monotone" dataKey="count" stroke="#2563eb" strokeWidth={2} fill="url(#eventsGrad)" name="Events" />
                    </AreaChart>
                  </ResponsiveContainer>
                )}
              </div>

              {/* By type */}
              <div className="lg:col-span-2 bg-white border border-gray-200 rounded-lg p-4">
                <h3 className="text-sm font-semibold text-gray-700 mb-4">By Alert Type</h3>
                <ResponsiveContainer width="100%" height={160}>
                  <BarChart data={typeData} layout="vertical" margin={{ top: 0, right: 16, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" horizontal={false} />
                    <XAxis type="number" tick={{ fontSize: 10 }} allowDecimals={false} />
                    <YAxis type="category" dataKey="name" tick={{ fontSize: 10 }} width={90} />
                    <Tooltip contentStyle={{ fontSize: 12 }} />
                    <Bar dataKey="count" name="Events" radius={[0, 3, 3, 0]}>
                      {typeData.map((entry) => (
                        <Cell key={entry.type} fill={TYPE_COLORS[entry.type] ?? '#6b7280'} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Events table */}
          <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-gray-700">
                Events <span className="text-gray-400 font-normal">({filtered.length})</span>
              </h3>
            </div>
            {filtered.length === 0 ? (
              <div className="p-10 text-center text-gray-400">
                <Bell className="w-10 h-10 mx-auto mb-3 opacity-50" />
                <p>No events match your filters</p>
              </div>
            ) : (
              <>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 border-b border-gray-200">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Fired</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Firm</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Type</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Change</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Platform</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Delivery</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {filtered.map((event) => (
                        <tr key={event.id} className="hover:bg-gray-50">
                          <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">{formatDate(event.fired_at)}</td>
                          <td className="px-4 py-3">
                            <button
                              onClick={() => navigate(`/firms/${event.crd_number}`)}
                              className="font-medium text-brand-600 hover:underline text-left"
                            >
                              {event.firm_name ?? `CRD ${event.crd_number}`}
                            </button>
                            <div className="text-xs text-gray-400 font-mono">{event.crd_number}</div>
                          </td>
                          <td className="px-4 py-3">
                            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
                              style={{ backgroundColor: `${TYPE_COLORS[event.rule_type]}20`, color: TYPE_COLORS[event.rule_type] }}>
                              {event.rule_type.replace(/_/g, ' ')}
                            </span>
                          </td>
                          <td className="px-4 py-3">
                            {event.old_value || event.new_value ? (
                              <span className="text-xs">
                                <span className="text-red-500">{event.old_value ?? '—'}</span>
                                <span className="text-gray-400 mx-1">→</span>
                                <span className="text-green-600">{event.new_value ?? '—'}</span>
                              </span>
                            ) : <span className="text-gray-400">—</span>}
                          </td>
                          <td className="px-4 py-3 text-gray-500 text-xs">{event.platform_name ?? '—'}</td>
                          <td className="px-4 py-3"><EventStatusBadge status={event.delivery_status} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {hasMore && (
                  <div className="px-4 py-3 border-t border-gray-100 text-center">
                    <button
                      onClick={() => setOffset((o) => o + 500)}
                      disabled={isLoading}
                      className="flex items-center gap-2 mx-auto text-sm text-brand-600 hover:text-brand-700 font-medium disabled:opacity-50"
                    >
                      {isLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <ChevronDown className="w-4 h-4" />}
                      Load more
                    </button>
                  </div>
                )}
              </>
            )}
          </div>
        </>
      )}
    </div>
  )
}
