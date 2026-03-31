import { useState, useMemo } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, Download, Search, Trash2, Loader2 } from 'lucide-react'
import { getPlatformFirms, getPlatforms, removeFirmPlatform } from '../api/client'
import { SkeletonTable } from '../components/Skeleton'
import { StatusBadge } from '../components/StatusBadge'
import { useToast } from '../components/Toast'
import { formatAum } from '../utils'
import type { FirmSummary } from '../types'

function downloadCSV(firms: FirmSummary[], platformName: string) {
  const headers = ['crd_number', 'legal_name', 'main_city', 'main_state', 'aum_total', 'registration_status']
  const rows = firms.map((f) =>
    [
      f.crd_number,
      `"${f.legal_name.replace(/"/g, '""')}"`,
      f.main_city ?? '',
      f.main_state ?? '',
      f.aum_total ?? '',
      f.registration_status ?? '',
    ].join(',')
  )
  const csv = [headers.join(','), ...rows].join('\n')
  const blob = new Blob([csv], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `${platformName.replace(/\s+/g, '_')}_firms.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export default function PlatformDetail() {
  const { id } = useParams<{ id: string }>()
  const platformId = Number(id)
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { addToast } = useToast()

  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<Set<number>>(new Set())

  const { data: platforms } = useQuery({
    queryKey: ['platforms'],
    queryFn: getPlatforms,
  })

  const platform = platforms?.find((p) => p.id === platformId)

  const { data: firms, isLoading, error } = useQuery({
    queryKey: ['platform-firms', platformId],
    queryFn: () => getPlatformFirms(platformId),
  })

  const untagMutation = useMutation({
    mutationFn: async (crds: number[]) => {
      for (const crd of crds) {
        await removeFirmPlatform(crd, platformId)
      }
    },
    onSuccess: (_, crds) => {
      queryClient.invalidateQueries({ queryKey: ['platform-firms', platformId] })
      setSelected(new Set())
      addToast(`Untagged ${crds.length} firm${crds.length !== 1 ? 's' : ''}`, 'success')
    },
    onError: () => addToast('Failed to untag firms', 'error'),
  })

  const filtered = useMemo(() => {
    if (!firms) return []
    const q = filter.toLowerCase()
    if (!q) return firms
    return firms.filter(
      (f) =>
        f.legal_name.toLowerCase().includes(q) ||
        String(f.crd_number).includes(q) ||
        (f.main_city ?? '').toLowerCase().includes(q) ||
        (f.main_state ?? '').toLowerCase().includes(q)
    )
  }, [firms, filter])

  const allSelected = filtered.length > 0 && filtered.every((f) => selected.has(f.crd_number))

  function toggleAll() {
    if (allSelected) {
      setSelected(new Set())
    } else {
      setSelected(new Set(filtered.map((f) => f.crd_number)))
    }
  }

  function toggleRow(crd: number) {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(crd)) next.delete(crd)
      else next.add(crd)
      return next
    })
  }

  return (
    <div>
      <Link
        to="/platforms"
        className="inline-flex items-center gap-1 text-sm text-brand-600 hover:text-brand-700 mb-4"
      >
        <ArrowLeft className="w-4 h-4" />
        Back to Platforms
      </Link>

      <div className="flex items-start justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">
            {platform?.name ?? `Platform #${platformId}`}
          </h1>
          {platform?.description && (
            <p className="text-sm text-gray-500 mt-1">{platform.description}</p>
          )}
          {firms && (
            <p className="text-sm text-gray-400 mt-1">{firms.length} firms tagged</p>
          )}
        </div>
        <button
          onClick={() => firms && platform && downloadCSV(firms, platform.name)}
          disabled={!firms || firms.length === 0}
          className="flex items-center gap-2 px-3 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-50 text-gray-600 disabled:opacity-40"
        >
          <Download className="w-4 h-4" />
          Export CSV
        </button>
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-3 mb-4">
        <div className="relative flex-1 max-w-sm">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter by name, CRD, city, state..."
            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-brand-600 outline-none"
          />
        </div>

        {selected.size > 0 && (
          <button
            onClick={() => untagMutation.mutate(Array.from(selected))}
            disabled={untagMutation.isPending}
            className="flex items-center gap-2 px-3 py-2 text-sm bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50"
          >
            {untagMutation.isPending ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Trash2 className="w-4 h-4" />
            )}
            Untag {selected.size} selected
          </button>
        )}
      </div>

      {isLoading ? (
        <SkeletonTable rows={8} cols={6} />
      ) : error ? (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
          Failed to load firms for this platform.
        </div>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="px-4 py-3 w-10">
                    <input
                      type="checkbox"
                      checked={allSelected}
                      onChange={toggleAll}
                      className="rounded text-brand-600"
                    />
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Legal Name</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">CRD</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">City</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">State</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">AUM</th>
                  <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-4 py-8 text-center text-gray-400">
                      {filter ? 'No firms match your filter' : 'No firms tagged to this platform'}
                    </td>
                  </tr>
                ) : (
                  filtered.map((firm) => (
                    <tr
                      key={firm.crd_number}
                      className="hover:bg-brand-50 transition-colors"
                    >
                      <td className="px-4 py-3" onClick={(e) => e.stopPropagation()}>
                        <input
                          type="checkbox"
                          checked={selected.has(firm.crd_number)}
                          onChange={() => toggleRow(firm.crd_number)}
                          className="rounded text-brand-600"
                        />
                      </td>
                      <td
                        className="px-4 py-3 cursor-pointer"
                        onClick={() => navigate(`/firms/${firm.crd_number}`)}
                      >
                        <span className="font-medium text-gray-900 hover:text-brand-600">
                          {firm.legal_name}
                        </span>
                      </td>
                      <td className="px-4 py-3 font-mono text-xs text-gray-500">{firm.crd_number}</td>
                      <td className="px-4 py-3 text-gray-600">{firm.main_city ?? '—'}</td>
                      <td className="px-4 py-3 text-gray-600">{firm.main_state ?? '—'}</td>
                      <td className="px-4 py-3 font-mono text-sm">{formatAum(firm.aum_total)}</td>
                      <td className="px-4 py-3">
                        {firm.registration_status ? (
                          <StatusBadge status={firm.registration_status} />
                        ) : (
                          <span className="text-gray-400">—</span>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
