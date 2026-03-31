import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Trash2, Play, Loader2, Bell, BellOff } from 'lucide-react'
import {
  getAlertRules,
  createAlertRule,
  deleteAlertRule,
  testAlertRule,
  getAlertEvents,
  getPlatforms,
} from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { useToast } from '../components/Toast'
import { formatDate } from '../utils'
import type { AlertRuleOut } from '../types'

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

function RuleBadge({ active }: { active: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${
        active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-500'
      }`}
    >
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
  }
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[status] ?? 'bg-gray-100 text-gray-600'}`}>
      {status}
    </span>
  )
}

export default function Alerts() {
  const [tab, setTab] = useState<'rules' | 'events'>('rules')

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
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              tab === t
                ? 'border-brand-600 text-brand-600'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            {t === 'rules' ? 'Alert Rules' : 'Event Feed'}
          </button>
        ))}
      </div>

      {tab === 'rules' ? <RulesTab /> : <EventsTab />}
    </div>
  )
}

function RulesTab() {
  const queryClient = useQueryClient()
  const { addToast } = useToast()

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

  const { data: rules, isLoading } = useQuery({
    queryKey: ['alert-rules'],
    queryFn: getAlertRules,
  })

  const { data: platforms } = useQuery({
    queryKey: ['platforms'],
    queryFn: getPlatforms,
  })

  const createMutation = useMutation({
    mutationFn: () => {
      const payload: Record<string, unknown> = {
        label: form.label.trim(),
        rule_type: form.rule_type,
        delivery: form.delivery,
      }
      if (form.delivery_target.trim()) payload.delivery_target = form.delivery_target.trim()
      if (form.rule_type === 'aum_decline' && form.threshold_pct) {
        payload.threshold_pct = Number(form.threshold_pct)
      }
      if (form.rule_type === 'field_change' && form.field_path.trim()) {
        payload.field_path = form.field_path.trim()
      }
      if (form.platform_ids.length > 0) payload.platform_ids = form.platform_ids
      return createAlertRule(payload)
    },
    onSuccess: (rule) => {
      queryClient.invalidateQueries({ queryKey: ['alert-rules'] })
      addToast(`Rule "${rule.label}" created`, 'success')
      setShowForm(false)
      setForm({ label: '', rule_type: 'deregistration', delivery: 'email', delivery_target: '', threshold_pct: '', field_path: '', platform_ids: [] })
    },
    onError: () => addToast('Failed to create rule', 'error'),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteAlertRule,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['alert-rules'] })
      addToast('Rule deleted', 'success')
    },
    onError: () => addToast('Failed to delete rule', 'error'),
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
      <div className="flex justify-end mb-4">
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
              <label className="block text-xs font-medium text-gray-600 mb-1">
                Label <span className="text-red-500">*</span>
              </label>
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
                  {RULE_TYPES.map((r) => (
                    <option key={r.value} value={r.value}>{r.label}</option>
                  ))}
                </select>
              </div>

              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Delivery</label>
                <select
                  value={form.delivery}
                  onChange={(e) => setForm((f) => ({ ...f, delivery: e.target.value }))}
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                >
                  {DELIVERY_TYPES.map((d) => (
                    <option key={d.value} value={d.value}>{d.label}</option>
                  ))}
                </select>
              </div>
            </div>

            {form.rule_type === 'aum_decline' && (
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Threshold % (decline)</label>
                <input
                  type="number"
                  min="1"
                  max="100"
                  value={form.threshold_pct}
                  onChange={(e) => setForm((f) => ({ ...f, threshold_pct: e.target.value }))}
                  placeholder="e.g. 20"
                  className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                />
              </div>
            )}

            {form.rule_type === 'field_change' && (
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Field Path</label>
                <input
                  type="text"
                  value={form.field_path}
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
                <label className="block text-xs font-medium text-gray-600 mb-1">
                  Scope to Platforms (leave blank for all)
                </label>
                <div className="flex flex-wrap gap-2">
                  {platforms.map((p) => (
                    <label key={p.id} className="flex items-center gap-1.5 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={form.platform_ids.includes(p.id)}
                        onChange={() => togglePlatform(p.id)}
                        className="rounded text-brand-600"
                      />
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
              <button
                onClick={() => setShowForm(false)}
                className="px-4 py-2 text-sm text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {isLoading ? (
        <div className="space-y-3">
          {[1, 2, 3].map((i) => <Skeleton key={i} className="h-16" />)}
        </div>
      ) : !rules || rules.length === 0 ? (
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
                <th className="px-4 py-3 w-28"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {rules.map((rule) => (
                <tr key={rule.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-medium text-gray-900">
                    {rule.label}
                    {rule.threshold_pct != null && (
                      <span className="ml-2 text-xs text-gray-400">≥{rule.threshold_pct}%</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-gray-600">{rule.rule_type.replace('_', ' ')}</td>
                  <td className="px-4 py-3">
                    <DeliveryBadge delivery={rule.delivery} />
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs font-mono max-w-[180px] truncate">
                    {rule.delivery_target ?? '—'}
                  </td>
                  <td className="px-4 py-3">
                    <RuleBadge active={rule.active} />
                  </td>
                  <td className="px-4 py-3 text-gray-400 text-xs">{formatDate(rule.created_at)}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2 justify-end">
                      <button
                        onClick={() => handleTest(rule)}
                        disabled={testingId === rule.id}
                        title="Send test alert"
                        className="p-1.5 rounded text-gray-400 hover:text-brand-600 hover:bg-brand-50 disabled:opacity-50"
                      >
                        {testingId === rule.id ? (
                          <Loader2 className="w-4 h-4 animate-spin" />
                        ) : (
                          <Play className="w-4 h-4" />
                        )}
                      </button>
                      <button
                        onClick={() => deleteMutation.mutate(rule.id)}
                        disabled={deleteMutation.isPending}
                        title="Delete rule"
                        className="p-1.5 rounded text-gray-400 hover:text-red-600 hover:bg-red-50 disabled:opacity-50"
                      >
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

function EventsTab() {
  const [ruleId, setRuleId] = useState('')
  const [crdFilter, setCrdFilter] = useState('')
  const [since, setSince] = useState('')
  const [until, setUntil] = useState('')

  const params: Record<string, unknown> = {}
  if (ruleId) params.rule_id = ruleId
  if (crdFilter) params.crd_number = crdFilter
  if (since) params.since = since
  if (until) params.until = until

  const { data: events, isLoading } = useQuery({
    queryKey: ['alert-events', params],
    queryFn: () => getAlertEvents(params),
  })

  const { data: rules } = useQuery({
    queryKey: ['alert-rules'],
    queryFn: getAlertRules,
  })

  return (
    <div>
      {/* Filters */}
      <div className="bg-white border border-gray-200 rounded-lg p-4 mb-4 flex flex-wrap gap-3 items-end">
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Rule</label>
          <select
            value={ruleId}
            onChange={(e) => setRuleId(e.target.value)}
            className="text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
          >
            <option value="">All Rules</option>
            {rules?.map((r) => (
              <option key={r.id} value={String(r.id)}>{r.label}</option>
            ))}
          </select>
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">CRD</label>
          <input
            type="text"
            value={crdFilter}
            onChange={(e) => setCrdFilter(e.target.value)}
            placeholder="e.g. 123456"
            className="w-32 text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Since</label>
          <input
            type="date"
            value={since}
            onChange={(e) => setSince(e.target.value)}
            className="text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Until</label>
          <input
            type="date"
            value={until}
            onChange={(e) => setUntil(e.target.value)}
            className="text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
          />
        </div>

        {(ruleId || crdFilter || since || until) && (
          <button
            onClick={() => { setRuleId(''); setCrdFilter(''); setSince(''); setUntil('') }}
            className="text-sm text-gray-500 hover:text-gray-700 py-1.5"
          >
            Clear
          </button>
        )}
      </div>

      {isLoading ? (
        <div className="space-y-2">
          {[1, 2, 3, 4, 5].map((i) => <Skeleton key={i} className="h-12" />)}
        </div>
      ) : !events || events.length === 0 ? (
        <div className="bg-white border border-gray-200 rounded-lg p-10 text-center text-gray-400">
          <p>No alert events found</p>
        </div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
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
                {events.map((event) => (
                  <tr key={event.id} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-gray-400 text-xs whitespace-nowrap">
                      {formatDate(event.fired_at)}
                    </td>
                    <td className="px-4 py-3">
                      <div className="font-medium text-gray-900">{event.firm_name ?? `CRD ${event.crd_number}`}</div>
                      <div className="text-xs text-gray-400 font-mono">{event.crd_number}</div>
                    </td>
                    <td className="px-4 py-3 text-gray-600">{event.rule_type.replace('_', ' ')}</td>
                    <td className="px-4 py-3">
                      {event.old_value || event.new_value ? (
                        <span className="text-xs">
                          <span className="text-red-500">{event.old_value ?? '—'}</span>
                          <span className="text-gray-400 mx-1">→</span>
                          <span className="text-green-600">{event.new_value ?? '—'}</span>
                        </span>
                      ) : (
                        <span className="text-gray-400">—</span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-gray-500 text-xs">{event.platform_name ?? '—'}</td>
                    <td className="px-4 py-3">
                      <EventStatusBadge status={event.delivery_status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
