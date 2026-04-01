import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { RefreshCw, Loader2, CheckCircle, XCircle, Clock, AlertTriangle, Play, Pencil, Check, X } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { getSyncStatus, triggerSync, getSchedules, patchSchedule, triggerSchedule } from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { useToast } from '../components/Toast'
import { formatDate, formatDuration } from '../utils'
import type { SyncStatusEntry, CronScheduleOut } from '../types'

function StatusIcon({ status }: { status: string }) {
  if (status === 'complete') return <CheckCircle className="w-5 h-5 text-green-500" />
  if (status === 'failed') return <XCircle className="w-5 h-5 text-red-500" />
  if (status === 'running') return <Loader2 className="w-5 h-5 text-brand-600 animate-spin" />
  if (status === 'pending') return <Clock className="w-5 h-5 text-yellow-500" />
  return <AlertTriangle className="w-5 h-5 text-gray-400" />
}

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    complete: 'bg-green-100 text-green-800',
    failed: 'bg-red-100 text-red-800',
    running: 'bg-blue-100 text-blue-800',
    pending: 'bg-yellow-100 text-yellow-800',
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

function formatCron(s: CronScheduleOut): string {
  const isEveryHour = s.hour === '*'
  const isEveryDay = s.day_of_month === '*'

  if (!isEveryHour && !isEveryDay) {
    return `Day ${s.day_of_month} of month at ${s.hour.padStart(2, '0')}:${s.minute.padStart(2, '0')} UTC`
  }
  if (isEveryHour && isEveryDay) {
    return `Every hour at :${s.minute.padStart(2, '0')}`
  }
  if (!isEveryHour && isEveryDay) {
    return `Daily at ${s.hour.padStart(2, '0')}:${s.minute.padStart(2, '0')} UTC`
  }
  return `${s.minute} ${s.hour} ${s.day_of_month} ${s.month_of_year} ${s.day_of_week}`
}

type EditState = {
  minute: string
  hour: string
  day_of_month: string
  month_of_year: string
  day_of_week: string
}

function ScheduleRow({ schedule }: { schedule: CronScheduleOut }) {
  const { addToast } = useToast()
  const queryClient = useQueryClient()
  const [editing, setEditing] = useState(false)
  const [form, setForm] = useState<EditState>({
    minute: schedule.minute,
    hour: schedule.hour,
    day_of_month: schedule.day_of_month,
    month_of_year: schedule.month_of_year,
    day_of_week: schedule.day_of_week,
  })

  const toggleMutation = useMutation({
    mutationFn: () => patchSchedule(schedule.id, { enabled: !schedule.enabled }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schedules'] })
      addToast(`Schedule ${schedule.enabled ? 'disabled' : 'enabled'}`, 'success')
    },
    onError: () => addToast('Failed to update schedule', 'error'),
  })

  const saveMutation = useMutation({
    mutationFn: () => patchSchedule(schedule.id, form),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schedules'] })
      setEditing(false)
      addToast('Schedule updated', 'success')
    },
    onError: () => addToast('Failed to save schedule', 'error'),
  })

  const runMutation = useMutation({
    mutationFn: () => triggerSchedule(schedule.id),
    onSuccess: (data) => addToast(`Job queued (${data.task_id.slice(0, 8)}…)`, 'success'),
    onError: () => addToast('Failed to trigger job', 'error'),
  })

  return (
    <>
      <tr className="hover:bg-gray-50">
        <td className="px-4 py-3 font-medium text-gray-900 text-sm">{schedule.name}</td>
        <td className="px-4 py-3 text-gray-500 text-sm">{schedule.description ?? '—'}</td>
        <td className="px-4 py-3 font-mono text-xs text-gray-600">{formatCron(schedule)}</td>
        <td className="px-4 py-3">
          <button
            onClick={() => toggleMutation.mutate()}
            disabled={toggleMutation.isPending}
            className={`relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors ${
              schedule.enabled ? 'bg-brand-600' : 'bg-gray-200'
            } disabled:opacity-50`}
            aria-label={schedule.enabled ? 'Disable' : 'Enable'}
          >
            <span
              className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${
                schedule.enabled ? 'translate-x-4' : 'translate-x-0'
              }`}
            />
          </button>
        </td>
        <td className="px-4 py-3 text-gray-400 text-xs">{formatDate(schedule.updated_at)}</td>
        <td className="px-4 py-3">
          <div className="flex items-center gap-2">
            <button
              onClick={() => { setForm({ minute: schedule.minute, hour: schedule.hour, day_of_month: schedule.day_of_month, month_of_year: schedule.month_of_year, day_of_week: schedule.day_of_week }); setEditing(true) }}
              className="p-1.5 rounded text-gray-400 hover:text-gray-700 hover:bg-gray-100"
              title="Edit schedule"
            >
              <Pencil className="w-3.5 h-3.5" />
            </button>
            <button
              onClick={() => runMutation.mutate()}
              disabled={runMutation.isPending}
              className="p-1.5 rounded text-gray-400 hover:text-brand-600 hover:bg-brand-50 disabled:opacity-50"
              title="Run now"
            >
              {runMutation.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
            </button>
          </div>
        </td>
      </tr>
      {editing && (
        <tr className="bg-blue-50 border-t border-blue-100">
          <td colSpan={6} className="px-4 py-3">
            <div className="flex items-end gap-3 flex-wrap">
              {(
                [
                  ['minute', 'Minute'],
                  ['hour', 'Hour'],
                  ['day_of_month', 'Day of Month'],
                  ['month_of_year', 'Month'],
                  ['day_of_week', 'Day of Week'],
                ] as [keyof EditState, string][]
              ).map(([field, label]) => (
                <div key={field}>
                  <label className="block text-xs font-medium text-gray-600 mb-1">{label}</label>
                  <input
                    type="text"
                    value={form[field]}
                    onChange={(e) => setForm((f) => ({ ...f, [field]: e.target.value }))}
                    className="w-24 text-sm border border-gray-300 rounded px-2 py-1.5 font-mono focus:ring-2 focus:ring-brand-600 outline-none"
                  />
                </div>
              ))}
              <div className="flex gap-2 pb-0.5">
                <button
                  onClick={() => saveMutation.mutate()}
                  disabled={saveMutation.isPending}
                  className="flex items-center gap-1 px-3 py-1.5 bg-brand-600 text-white rounded text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
                >
                  {saveMutation.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
                  Save
                </button>
                <button
                  onClick={() => setEditing(false)}
                  className="flex items-center gap-1 px-3 py-1.5 border border-gray-300 text-gray-600 rounded text-sm hover:bg-gray-50"
                >
                  <X className="w-3.5 h-3.5" />
                  Cancel
                </button>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

function SchedulesTab() {
  const { data: schedules, isLoading } = useQuery({
    queryKey: ['schedules'],
    queryFn: getSchedules,
  })

  if (isLoading) {
    return (
      <div className="space-y-2">
        {[1, 2].map((i) => <Skeleton key={i} className="h-14" />)}
      </div>
    )
  }

  if (!schedules || schedules.length === 0) {
    return (
      <div className="bg-white border border-gray-200 rounded-lg p-10 text-center text-gray-400">
        <RefreshCw className="w-10 h-10 mx-auto mb-3 opacity-50" />
        <p className="font-medium">No schedules configured</p>
        <p className="text-sm mt-1">Run <code className="text-xs bg-gray-100 px-1 py-0.5 rounded">make seed-schedules</code> to seed defaults</p>
      </div>
    )
  }

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 border-b border-gray-200">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Name</th>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Description</th>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Schedule</th>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Enabled</th>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Last Updated</th>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {schedules.map((s) => <ScheduleRow key={s.id} schedule={s} />)}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default function Sync() {
  const { addToast } = useToast()
  const [monthStr, setMonthStr] = useState('')
  const [activeTab, setActiveTab] = useState<'history' | 'schedules'>('history')

  const { data: jobs, isLoading, refetch } = useQuery({
    queryKey: ['sync-status'],
    queryFn: getSyncStatus,
    refetchInterval: (query) => {
      const data = query.state.data as SyncStatusEntry[] | undefined
      const hasRunning = data?.some((j) => j.status === 'running' || j.status === 'pending')
      return hasRunning ? 5000 : false
    },
  })

  const triggerMutation = useMutation({
    mutationFn: () => triggerSync(monthStr.trim() || undefined),
    onSuccess: () => {
      addToast('Sync job triggered', 'success')
      refetch()
    },
    onError: () => addToast('Failed to trigger sync', 'error'),
  })

  const latest = jobs?.[0]
  const recentJobs = jobs?.slice(0, 20) ?? []

  const chartData = (jobs ?? [])
    .slice(0, 12)
    .reverse()
    .map((j) => ({
      label: j.started_at ? new Date(j.started_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : `#${j.id}`,
      changes: j.changes_detected,
      updated: j.firms_updated,
    }))

  const totalFirmsProcessed = recentJobs.reduce((s, j) => s + (j.firms_processed ?? 0), 0)
  const totalChanges = recentJobs.reduce((s, j) => s + (j.changes_detected ?? 0), 0)
  const successCount = recentJobs.filter((j) => j.status === 'complete').length

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Data Sync</h1>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 mb-6 border-b border-gray-200">
        {(['history', 'schedules'] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-2 text-sm font-medium capitalize border-b-2 -mb-px transition-colors ${
              activeTab === tab
                ? 'border-brand-600 text-brand-600'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            {tab === 'history' ? 'Job History' : 'Schedules'}
          </button>
        ))}
      </div>

      {activeTab === 'schedules' ? (
        <SchedulesTab />
      ) : (
        <>
          {/* Trigger */}
          <div className="bg-white border border-gray-200 rounded-lg p-5 mb-6">
            <h2 className="text-sm font-semibold text-gray-700 mb-3">Trigger Manual Sync</h2>
            <div className="flex items-end gap-3">
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">
                  Month (optional, YYYY-MM)
                </label>
                <input
                  type="text"
                  value={monthStr}
                  onChange={(e) => setMonthStr(e.target.value)}
                  placeholder="e.g. 2024-12 (default: latest)"
                  className="w-64 text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                />
              </div>
              <button
                onClick={() => triggerMutation.mutate()}
                disabled={triggerMutation.isPending}
                className="flex items-center gap-2 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
              >
                {triggerMutation.isPending ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <RefreshCw className="w-4 h-4" />
                )}
                Run Sync
              </button>
            </div>
            <div className="flex flex-wrap gap-x-6 gap-y-1 mt-3 text-sm text-gray-500">
              <span>
                Last sync:{' '}
                <span className="font-medium text-gray-700">
                  {latest ? formatDate(latest.completed_at ?? latest.started_at) : 'Never'}
                </span>
              </span>
            </div>
          </div>

          {isLoading ? (
            <div className="space-y-4">
              <div className="grid grid-cols-3 gap-4">
                {[1, 2, 3].map((i) => <Skeleton key={i} className="h-20" />)}
              </div>
              <Skeleton className="h-48" />
            </div>
          ) : (
            <>
              {/* Stats */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                <StatCard label="Total Jobs" value={recentJobs.length} sub="last 20" />
                <StatCard
                  label="Successful"
                  value={successCount}
                  sub={`${recentJobs.length > 0 ? Math.round((successCount / recentJobs.length) * 100) : 0}% success rate`}
                />
                <StatCard label="Firms Processed" value={totalFirmsProcessed.toLocaleString()} sub="across recent jobs" />
                <StatCard label="Changes Detected" value={totalChanges.toLocaleString()} sub="across recent jobs" />
              </div>

              {/* Chart */}
              {chartData.length > 0 && (
                <div className="bg-white border border-gray-200 rounded-lg p-5 mb-6">
                  <h2 className="text-sm font-semibold text-gray-700 mb-4">Changes Detected per Sync</h2>
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={chartData} margin={{ top: 0, right: 0, left: -20, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                      <XAxis dataKey="label" tick={{ fontSize: 11 }} />
                      <YAxis tick={{ fontSize: 11 }} />
                      <Tooltip
                        contentStyle={{ fontSize: 12 }}
                        formatter={(value: number, name: string) => [
                          value.toLocaleString(),
                          name === 'changes' ? 'Changes' : 'Updated',
                        ]}
                      />
                      <Bar dataKey="changes" fill="#2563eb" radius={[3, 3, 0, 0]} />
                      <Bar dataKey="updated" fill="#93c5fd" radius={[3, 3, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                  <div className="flex gap-4 mt-2 justify-center">
                    <span className="flex items-center gap-1.5 text-xs text-gray-500">
                      <span className="w-3 h-3 rounded-sm bg-blue-600 inline-block" />
                      Changes Detected
                    </span>
                    <span className="flex items-center gap-1.5 text-xs text-gray-500">
                      <span className="w-3 h-3 rounded-sm bg-blue-300 inline-block" />
                      Firms Updated
                    </span>
                  </div>
                </div>
              )}

              {/* Job history table */}
              <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
                <div className="px-4 py-3 border-b border-gray-200">
                  <h2 className="text-sm font-semibold text-gray-700">Job History</h2>
                </div>
                {recentJobs.length === 0 ? (
                  <div className="p-10 text-center text-gray-400">
                    <RefreshCw className="w-10 h-10 mx-auto mb-3 opacity-50" />
                    <p>No sync jobs yet</p>
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50 border-b border-gray-200">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">ID</th>
                          <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Type</th>
                          <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Status</th>
                          <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Started</th>
                          <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Duration</th>
                          <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Processed</th>
                          <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Updated</th>
                          <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Changes</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {recentJobs.map((job) => (
                          <tr key={job.id} className="hover:bg-gray-50">
                            <td className="px-4 py-3 font-mono text-xs text-gray-400">#{job.id}</td>
                            <td className="px-4 py-3 text-gray-600">{job.job_type}</td>
                            <td className="px-4 py-3">
                              <div className="flex items-center gap-1.5">
                                <StatusIcon status={job.status} />
                                <StatusBadge status={job.status} />
                              </div>
                              {job.error_message && (
                                <p className="text-xs text-red-500 mt-0.5 max-w-[200px] truncate" title={job.error_message}>
                                  {job.error_message}
                                </p>
                              )}
                            </td>
                            <td className="px-4 py-3 text-gray-500 text-xs">{formatDate(job.started_at)}</td>
                            <td className="px-4 py-3 text-gray-500 text-xs font-mono">
                              {formatDuration(job.started_at, job.completed_at)}
                            </td>
                            <td className="px-4 py-3 text-right font-mono text-xs">{job.firms_processed.toLocaleString()}</td>
                            <td className="px-4 py-3 text-right font-mono text-xs">{job.firms_updated.toLocaleString()}</td>
                            <td className="px-4 py-3 text-right font-mono text-xs font-medium text-brand-600">
                              {job.changes_detected.toLocaleString()}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </>
          )}
        </>
      )}
    </div>
  )
}
