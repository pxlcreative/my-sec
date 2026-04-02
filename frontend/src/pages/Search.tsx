import { useState, useEffect, useCallback, useRef } from 'react'
import { useSearchParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  createColumnHelper,
  type SortingState,
} from '@tanstack/react-table'
import { Share2, ChevronLeft, ChevronRight, ChevronUp, ChevronDown, ChevronsUpDown, Database } from 'lucide-react'
import { searchFirms, getPlatforms } from '../api/client'
import { SkeletonTable } from '../components/Skeleton'
import { StatusBadge } from '../components/StatusBadge'
import { useToast } from '../components/Toast'
import { formatAum, US_STATES } from '../utils'
import type { FirmSummary } from '../types'

const columnHelper = createColumnHelper<FirmSummary>()

export default function Search() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const { addToast } = useToast()

  const [inputValue, setInputValue] = useState(searchParams.get('q') ?? '')
  const [debouncedQ, setDebouncedQ] = useState(searchParams.get('q') ?? '')
  const [sorting, setSorting] = useState<SortingState>([])
  const [platformDropdownOpen, setPlatformDropdownOpen] = useState(false)
  const platformDropdownRef = useRef<HTMLDivElement>(null)

  const state = searchParams.get('state') ?? ''
  const aumMin = searchParams.get('aum_min') ?? ''
  const aumMax = searchParams.get('aum_max') ?? ''
  const regStatus = searchParams.get('registration_status') ?? ''
  const platformIds = searchParams.getAll('platform_ids')
  const page = parseInt(searchParams.get('page') ?? '1', 10)

  // Debounce
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQ(inputValue)
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev)
        if (inputValue) {
          next.set('q', inputValue)
        } else {
          next.delete('q')
        }
        next.set('page', '1')
        return next
      })
    }, 300)
    return () => clearTimeout(timer)
  }, [inputValue, setSearchParams])

  useEffect(() => {
    if (!platformDropdownOpen) return
    function handleClick(e: MouseEvent) {
      if (platformDropdownRef.current && !platformDropdownRef.current.contains(e.target as Node)) {
        setPlatformDropdownOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [platformDropdownOpen])

  const queryParams: Record<string, unknown> = {
    page,
    page_size: 20,
  }
  if (state) queryParams.state = state
  if (aumMin) queryParams.aum_min = aumMin
  if (aumMax) queryParams.aum_max = aumMax
  if (regStatus) queryParams.registration_status = regStatus
  if (platformIds.length > 0) queryParams.platform_ids = platformIds

  const { data, isLoading, error } = useQuery({
    queryKey: ['firms', debouncedQ, queryParams],
    queryFn: () => searchFirms(debouncedQ, queryParams),
  })

  const { data: platforms } = useQuery({
    queryKey: ['platforms'],
    queryFn: getPlatforms,
  })

  const setParam = useCallback(
    (key: string, value: string) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev)
        if (value) {
          next.set(key, value)
        } else {
          next.delete(key)
        }
        next.set('page', '1')
        return next
      })
    },
    [setSearchParams]
  )

  const togglePlatform = useCallback(
    (id: string) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev)
        const existing = next.getAll('platform_ids')
        next.delete('platform_ids')
        if (existing.includes(id)) {
          existing.filter((v) => v !== id).forEach((v) => next.append('platform_ids', v))
        } else {
          ;[...existing, id].forEach((v) => next.append('platform_ids', v))
        }
        next.set('page', '1')
        return next
      })
    },
    [setSearchParams]
  )

  const columns = [
    columnHelper.accessor('legal_name', {
      header: 'Legal Name',
      cell: (info) => (
        <span className="font-medium text-gray-900">{info.getValue()}</span>
      ),
    }),
    columnHelper.accessor('crd_number', {
      header: 'CRD',
      cell: (info) => (
        <span className="text-gray-500 text-sm font-mono">{info.getValue()}</span>
      ),
    }),
    columnHelper.accessor('main_city', {
      header: 'City',
      cell: (info) => info.getValue() ?? '—',
    }),
    columnHelper.accessor('main_state', {
      header: 'State',
      cell: (info) => info.getValue() ?? '—',
    }),
    columnHelper.accessor('aum_total', {
      header: 'AUM',
      cell: (info) => (
        <span className="text-right font-mono text-sm">{formatAum(info.getValue())}</span>
      ),
    }),
    columnHelper.accessor('registration_status', {
      header: 'Status',
      cell: (info) => {
        const val = info.getValue()
        return val ? <StatusBadge status={val} /> : <span className="text-gray-400">—</span>
      },
    }),
    columnHelper.accessor('platforms', {
      header: 'Platforms',
      enableSorting: false,
      cell: (info) => {
        const plats = info.getValue()
        if (!plats || plats.length === 0) return <span className="text-gray-400 text-sm">—</span>
        return (
          <div className="flex flex-wrap gap-1">
            {plats.map((p) => (
              <span
                key={p}
                className="inline-flex items-center px-2 py-0.5 rounded-full text-xs bg-brand-100 text-brand-800 font-medium"
              >
                {p}
              </span>
            ))}
          </div>
        )
      },
    }),
  ]

  const table = useReactTable({
    data: data?.results ?? [],
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    manualPagination: true,
  })

  const totalPages = data ? Math.ceil(data.total / data.page_size) : 1

  function handleShare() {
    navigator.clipboard.writeText(window.location.href)
    addToast('Search URL copied to clipboard', 'success')
  }

  return (
    <div className="flex gap-6">
      {/* Filter sidebar */}
      <aside className="w-56 flex-shrink-0 space-y-4">
        <div className="bg-white rounded-lg border border-gray-200 p-4">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">Filters</h3>

          {/* State */}
          <div className="mb-4">
            <label className="block text-xs font-medium text-gray-600 mb-1">State</label>
            <select
              value={state}
              onChange={(e) => setParam('state', e.target.value)}
              className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 focus:border-brand-600 outline-none"
            >
              <option value="">All States</option>
              {US_STATES.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </div>

          {/* AUM Min */}
          <div className="mb-4">
            <label className="block text-xs font-medium text-gray-600 mb-1">AUM Min ($)</label>
            <input
              type="number"
              value={aumMin}
              onChange={(e) => setParam('aum_min', e.target.value)}
              placeholder="e.g. 100000000"
              className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
            />
          </div>

          {/* AUM Max */}
          <div className="mb-4">
            <label className="block text-xs font-medium text-gray-600 mb-1">AUM Max ($)</label>
            <input
              type="number"
              value={aumMax}
              onChange={(e) => setParam('aum_max', e.target.value)}
              placeholder="e.g. 1000000000"
              className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
            />
          </div>

          {/* Registration Status */}
          <div className="mb-4">
            <label className="block text-xs font-medium text-gray-600 mb-1">Status</label>
            <select
              value={regStatus}
              onChange={(e) => setParam('registration_status', e.target.value)}
              className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
            >
              <option value="">All Statuses</option>
              <option value="Registered">Registered</option>
              <option value="Withdrawn">Withdrawn</option>
              <option value="Exempt">Exempt</option>
            </select>
          </div>

          {/* Platform multi-select */}
          {platforms && platforms.length > 0 && (
            <div className="mb-2">
              <label className="block text-xs font-medium text-gray-600 mb-1">Platforms</label>
              <div className="relative" ref={platformDropdownRef}>
                <button
                  type="button"
                  onClick={() => setPlatformDropdownOpen((o) => !o)}
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 text-left bg-white focus:ring-2 focus:ring-brand-600 outline-none flex justify-between items-center"
                >
                  <span className="text-gray-600">
                    {platformIds.length > 0 ? `${platformIds.length} selected` : 'Any platform'}
                  </span>
                  <ChevronDown className="w-3 h-3 text-gray-400" />
                </button>
                {platformDropdownOpen && (
                  <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-gray-200 rounded-md shadow-lg z-10 max-h-48 overflow-y-auto">
                    {platforms.map((p) => (
                      <label
                        key={p.id}
                        className="flex items-center gap-2 px-3 py-2 hover:bg-gray-50 cursor-pointer"
                      >
                        <input
                          type="checkbox"
                          checked={platformIds.includes(String(p.id))}
                          onChange={() => togglePlatform(String(p.id))}
                          className="rounded text-brand-600"
                        />
                        <span className="text-sm text-gray-700">{p.name}</span>
                      </label>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </aside>

      {/* Main content */}
      <div className="flex-1 min-w-0">
        {/* Search bar */}
        <div className="flex items-center gap-3 mb-4">
          <input
            type="text"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            placeholder="Search firms by name or CRD..."
            className="flex-1 border border-gray-300 rounded-lg px-4 py-2 text-sm focus:ring-2 focus:ring-brand-600 focus:border-brand-600 outline-none"
          />
          <button
            onClick={handleShare}
            className="flex items-center gap-2 px-3 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-50 text-gray-600"
          >
            <Share2 className="w-4 h-4" />
            Share
          </button>
        </div>

        {/* Results count */}
        {data && (
          <p className="text-sm text-gray-500 mb-3">
            {data.total.toLocaleString()} firm{data.total !== 1 ? 's' : ''} found
          </p>
        )}

        {/* Table */}
        {isLoading ? (
          <SkeletonTable rows={8} cols={7} />
        ) : error ? (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
            Failed to load firms. Please try again.
          </div>
        ) : data && data.total === 0 && !debouncedQ && !state && !aumMin && !aumMax && !regStatus && platformIds.length === 0 ? (
          /* No data loaded yet — distinct from "no results for a query" */
          <div className="bg-white border border-gray-200 rounded-lg p-12 text-center">
            <Database className="w-12 h-12 mx-auto mb-4 text-gray-300" />
            <h2 className="text-lg font-semibold text-gray-700 mb-2">No firms loaded yet</h2>
            <p className="text-sm text-gray-500 max-w-sm mx-auto mb-4">
              Load SEC data to get started. Run{' '}
              <code className="bg-gray-100 px-1 py-0.5 rounded text-xs font-mono">make load-data</code>{' '}
              from the project root, or see the README for step-by-step instructions.
            </p>
            <a
              href="https://github.com/anthropics/claude-code"
              className="inline-flex items-center gap-1 text-sm text-brand-600 hover:text-brand-700 font-medium"
              onClick={(e) => { e.preventDefault(); window.open('/README.md', '_blank') }}
            >
              View Setup Guide →
            </a>
          </div>
        ) : (
          <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 border-b border-gray-200">
                  {table.getHeaderGroups().map((headerGroup) => (
                    <tr key={headerGroup.id}>
                      {headerGroup.headers.map((header) => (
                        <th
                          key={header.id}
                          onClick={header.column.getToggleSortingHandler()}
                          className={`px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wide ${
                            header.column.getCanSort()
                              ? 'cursor-pointer hover:text-gray-900 select-none'
                              : ''
                          }`}
                        >
                          <div className="flex items-center gap-1">
                            {flexRender(
                              header.column.columnDef.header,
                              header.getContext()
                            )}
                            {header.column.getCanSort() && (
                              <span className="text-gray-400">
                                {header.column.getIsSorted() === 'asc' ? (
                                  <ChevronUp className="w-3 h-3" />
                                ) : header.column.getIsSorted() === 'desc' ? (
                                  <ChevronDown className="w-3 h-3" />
                                ) : (
                                  <ChevronsUpDown className="w-3 h-3" />
                                )}
                              </span>
                            )}
                          </div>
                        </th>
                      ))}
                    </tr>
                  ))}
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {table.getRowModel().rows.length === 0 ? (
                    <tr>
                      <td colSpan={columns.length} className="px-4 py-10 text-center">
                        <p className="text-gray-500 mb-2">No firms match your search</p>
                        <button
                          onClick={() => {
                            setInputValue('')
                            setSearchParams(new URLSearchParams())
                          }}
                          className="text-sm text-brand-600 hover:text-brand-700 font-medium"
                        >
                          Clear filters
                        </button>
                      </td>
                    </tr>
                  ) : (
                    table.getRowModel().rows.map((row) => (
                      <tr
                        key={row.id}
                        onClick={() => navigate(`/firms/${row.original.crd_number}`)}
                        className="cursor-pointer hover:bg-brand-50 transition-colors"
                      >
                        {row.getVisibleCells().map((cell) => (
                          <td key={cell.id} className="px-4 py-3">
                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                          </td>
                        ))}
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {/* Pagination — only shown when there are multiple pages */}
            {data && data.total > 0 && data.total > data.page_size && (
              <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between">
                <p className="text-sm text-gray-500">
                  Page {page} of {totalPages} ({data.total.toLocaleString()} total)
                </p>
                <div className="flex items-center gap-2">
                  <button
                    disabled={page <= 1}
                    onClick={() => setParam('page', String(page - 1))}
                    className="p-1.5 rounded border border-gray-300 disabled:opacity-40 hover:bg-gray-50"
                  >
                    <ChevronLeft className="w-4 h-4" />
                  </button>
                  <button
                    disabled={page >= totalPages}
                    onClick={() => setParam('page', String(page + 1))}
                    className="p-1.5 rounded border border-gray-300 disabled:opacity-40 hover:bg-gray-50"
                  >
                    <ChevronRight className="w-4 h-4" />
                  </button>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
