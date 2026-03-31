import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { RefreshCw, Loader2, CheckCircle, XCircle, Clock, AlertTriangle } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { getSyncStatus, triggerSync } from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { useToast } from '../components/Toast'
import { formatDate, formatDuration } from '../utils'
import type { SyncStatusEntry } from '../types'

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

export default function Sync() {
  const { addToast } = useToast()
  const [monthStr, setMonthStr] = useState('')

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

  // Last 12 jobs with changes_detected for bar chart
  const chartData = (jobs ?? [])
    .slice(0, 12)
    .reverse()
    .map((j) => ({
      label: j.started_at ? new Date(j.started_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : `#${j.id}`,
      changes: j.changes_detected,
      updated: j.firms_updated,
    }))

  // Summary stats from all loaded jobs
  const totalFirmsProcessed = recentJobs.reduce((s, j) => s + (j.firms_processed ?? 0), 0)
  const totalChanges = recentJobs.reduce((s, j) => s + (j.changes_detected ?? 0), 0)
  const successCount = recentJobs.filter((j) => j.status === 'complete').length

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Data Sync</h1>
      </div>

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
          <span>
            Next scheduled:{' '}
            <span className="font-medium text-gray-700">2nd of each month at 06:00 UTC</span>
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
            <StatCard
              label="Total Jobs"
              value={recentJobs.length}
              sub="last 20"
            />
            <StatCard
              label="Successful"
              value={successCount}
              sub={`${recentJobs.length > 0 ? Math.round((successCount / recentJobs.length) * 100) : 0}% success rate`}
            />
            <StatCard
              label="Firms Processed"
              value={totalFirmsProcessed.toLocaleString()}
              sub="across recent jobs"
            />
            <StatCard
              label="Changes Detected"
              value={totalChanges.toLocaleString()}
              sub="across recent jobs"
            />
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
    </div>
  )
}
