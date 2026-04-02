import { useState, useEffect, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Download, Save, Loader2 } from 'lucide-react'
import { Button } from '../components/Button'
import {
  startExport,
  getExportJob,
  getExportTemplates,
  createExportTemplate,
  getPlatforms,
} from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { StatusBadge } from '../components/StatusBadge'
import { useToast } from '../components/Toast'
import { formatDate, US_STATES, DEFAULT_EXPORT_FIELDS } from '../utils'
import type { ExportJobOut, ExportTemplateOut } from '../types'

const FORMAT_OPTIONS = [
  { value: 'csv', label: 'CSV' },
  { value: 'json', label: 'JSON' },
  { value: 'xlsx', label: 'Excel (XLSX)' },
]


export default function Export() {
  const queryClient = useQueryClient()
  const { addToast } = useToast()

  // Filters
  const [state, setState] = useState('')
  const [aumMin, setAumMin] = useState('')
  const [aumMax, setAumMax] = useState('')
  const [regStatus, setRegStatus] = useState('')
  const [platformIds, setPlatformIds] = useState<string[]>([])

  // Fields
  const [selectedFields, setSelectedFields] = useState<Set<string>>(new Set(DEFAULT_EXPORT_FIELDS))

  // Format
  const [format, setFormat] = useState('csv')

  // Template
  const [templateName, setTemplateName] = useState('')
  const [showSaveForm, setShowSaveForm] = useState(false)

  // Active job
  const [activeJobId, setActiveJobId] = useState<string | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const { data: platforms } = useQuery({
    queryKey: ['platforms'],
    queryFn: getPlatforms,
  })

  const { data: templates, isLoading: templatesLoading } = useQuery({
    queryKey: ['export-templates'],
    queryFn: getExportTemplates,
  })

  const { data: activeJob, refetch: refetchJob } = useQuery({
    queryKey: ['export-job', activeJobId],
    queryFn: () => getExportJob(activeJobId!),
    enabled: !!activeJobId,
  })

  // Poll active job
  useEffect(() => {
    if (activeJob && (activeJob.status === 'complete' || activeJob.status === 'failed' || activeJob.status === 'expired')) {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [activeJob])

  useEffect(() => {
    if (activeJobId) {
      pollRef.current = setInterval(() => {
        refetchJob()
      }, 3000)
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
    }
  }, [activeJobId, refetchJob])

  const exportMutation = useMutation({
    mutationFn: () => {
      const filters: Record<string, unknown> = {}
      if (state) filters.state = state
      if (aumMin) filters.aum_min = Number(aumMin)
      if (aumMax) filters.aum_max = Number(aumMax)
      if (regStatus) filters.registration_status = regStatus
      if (platformIds.length > 0) filters.platform_ids = platformIds.map(Number)

      return startExport({
        format,
        filters,
        fields: Array.from(selectedFields),
      })
    },
    onSuccess: (result: unknown) => {
      const r = result as Record<string, unknown>
      if (r.job_id) {
        setActiveJobId(String(r.job_id))
        addToast('Export job queued', 'success')
      } else if (r.download_url || r.status === 'complete') {
        // Sync response — trigger download
        const jobResult = r as unknown as ExportJobOut
        if (jobResult.file_path) {
          window.open(`/api/export/jobs/${jobResult.id}/download`, '_blank')
        }
        const count = jobResult.row_count ?? 0
        addToast(`Export complete — ${count.toLocaleString()} record${count !== 1 ? 's' : ''} exported`, 'success')
      }
    },
    onError: () => addToast('Export failed', 'error'),
  })

  const saveTemplateMutation = useMutation({
    mutationFn: () => {
      const filters: Record<string, unknown> = {}
      if (state) filters.state = state
      if (aumMin) filters.aum_min = Number(aumMin)
      if (aumMax) filters.aum_max = Number(aumMax)
      if (regStatus) filters.registration_status = regStatus
      if (platformIds.length > 0) filters.platform_ids = platformIds.map(Number)

      return createExportTemplate({
        name: templateName.trim(),
        format,
        filter_criteria: filters,
        field_selection: { fields: Array.from(selectedFields) },
      })
    },
    onSuccess: (tmpl) => {
      queryClient.invalidateQueries({ queryKey: ['export-templates'] })
      addToast(`Template "${tmpl.name}" saved`, 'success')
      setShowSaveForm(false)
      setTemplateName('')
    },
    onError: () => addToast('Failed to save template', 'error'),
  })

  function loadTemplate(tmpl: ExportTemplateOut) {
    const fc = tmpl.filter_criteria ?? {}
    setState((fc.state as string) ?? '')
    setAumMin(fc.aum_min != null ? String(fc.aum_min) : '')
    setAumMax(fc.aum_max != null ? String(fc.aum_max) : '')
    setRegStatus((fc.registration_status as string) ?? '')
    setPlatformIds((fc.platform_ids as string[]) ?? [])
    setFormat(tmpl.format)
    if (tmpl.field_selection?.fields) {
      setSelectedFields(new Set(tmpl.field_selection.fields as string[]))
    }
    addToast(`Loaded template "${tmpl.name}"`, 'success')
  }

  function toggleField(field: string) {
    setSelectedFields((prev) => {
      const next = new Set(prev)
      if (next.has(field)) next.delete(field)
      else next.add(field)
      return next
    })
  }

  function toggleAllFields() {
    if (selectedFields.size === DEFAULT_EXPORT_FIELDS.length) {
      setSelectedFields(new Set())
    } else {
      setSelectedFields(new Set(DEFAULT_EXPORT_FIELDS))
    }
  }

  function togglePlatformId(id: string) {
    setPlatformIds((prev) =>
      prev.includes(id) ? prev.filter((v) => v !== id) : [...prev, id]
    )
  }

  const allFieldsSelected = selectedFields.size === DEFAULT_EXPORT_FIELDS.length

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Export</h1>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left: Filters + fields */}
        <div className="lg:col-span-2 space-y-4">
          {/* Templates */}
          {!templatesLoading && templates && templates.length > 0 && (
            <div className="bg-white border border-gray-200 rounded-lg p-4">
              <h2 className="text-sm font-semibold text-gray-700 mb-3">Saved Templates</h2>
              <div className="flex flex-wrap gap-2">
                {templates.map((tmpl) => (
                  <button
                    key={tmpl.id}
                    onClick={() => loadTemplate(tmpl)}
                    className="px-3 py-1.5 text-sm border border-gray-300 rounded-lg hover:bg-brand-50 hover:border-brand-300 text-gray-700"
                  >
                    {tmpl.name}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Filters */}
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <h2 className="text-sm font-semibold text-gray-700 mb-3">Filters</h2>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">State</label>
                <select
                  value={state}
                  onChange={(e) => setState(e.target.value)}
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
                >
                  <option value="">All States</option>
                  {US_STATES.map((s) => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>

              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Registration Status</label>
                <select
                  value={regStatus}
                  onChange={(e) => setRegStatus(e.target.value)}
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
                >
                  <option value="">All Statuses</option>
                  <option value="Registered">Registered</option>
                  <option value="Withdrawn">Withdrawn</option>
                  <option value="Exempt">Exempt</option>
                </select>
              </div>

              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">AUM Min ($)</label>
                <input
                  type="number"
                  value={aumMin}
                  onChange={(e) => setAumMin(e.target.value)}
                  placeholder="e.g. 100000000"
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
                />
              </div>

              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">AUM Max ($)</label>
                <input
                  type="number"
                  value={aumMax}
                  onChange={(e) => setAumMax(e.target.value)}
                  placeholder="e.g. 1000000000"
                  className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
                />
              </div>
            </div>

            {platforms && platforms.length > 0 && (
              <div className="mt-3">
                <label className="block text-xs font-medium text-gray-600 mb-1">Platforms</label>
                <div className="flex flex-wrap gap-3">
                  {platforms.map((p) => (
                    <label key={p.id} className="flex items-center gap-1.5 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={platformIds.includes(String(p.id))}
                        onChange={() => togglePlatformId(String(p.id))}
                        className="rounded text-brand-600"
                      />
                      <span className="text-sm text-gray-700">{p.name}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Field picker */}
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-semibold text-gray-700">Fields to Export</h2>
              <label className="flex items-center gap-1.5 cursor-pointer text-xs text-gray-500">
                <input
                  type="checkbox"
                  checked={allFieldsSelected}
                  onChange={toggleAllFields}
                  className="rounded text-brand-600"
                />
                Select all
              </label>
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-1.5">
              {DEFAULT_EXPORT_FIELDS.map((field) => (
                <label key={field} className="flex items-center gap-1.5 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={selectedFields.has(field)}
                    onChange={() => toggleField(field)}
                    className="rounded text-brand-600"
                  />
                  <span className="text-xs text-gray-700 font-mono">{field}</span>
                </label>
              ))}
            </div>
            <p className="text-xs text-gray-400 mt-2">{selectedFields.size} of {DEFAULT_EXPORT_FIELDS.length} fields selected</p>
          </div>
        </div>

        {/* Right: Format + actions */}
        <div className="space-y-4">
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <h2 className="text-sm font-semibold text-gray-700 mb-3">Format</h2>
            <div className="space-y-2">
              {FORMAT_OPTIONS.map((opt) => (
                <label key={opt.value} className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    name="format"
                    value={opt.value}
                    checked={format === opt.value}
                    onChange={() => setFormat(opt.value)}
                    className="text-brand-600"
                  />
                  <span className="text-sm text-gray-700">{opt.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="bg-white border border-gray-200 rounded-lg p-4 space-y-3">
            <button
              onClick={() => exportMutation.mutate()}
              disabled={exportMutation.isPending || selectedFields.size === 0}
              className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
            >
              {exportMutation.isPending ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Download className="w-4 h-4" />
              )}
              Export Now
            </button>

            {!showSaveForm ? (
              <button
                onClick={() => setShowSaveForm(true)}
                className="w-full flex items-center justify-center gap-2 px-4 py-2 text-sm border border-gray-300 rounded-lg text-gray-600 hover:bg-gray-50"
              >
                <Save className="w-4 h-4" />
                Save as Template
              </button>
            ) : (
              <div className="space-y-2">
                <input
                  type="text"
                  value={templateName}
                  onChange={(e) => setTemplateName(e.target.value)}
                  placeholder="Template name"
                  className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
                />
                <div className="flex gap-2">
                  <Button
                    className="flex-1 justify-center"
                    size="xs"
                    onClick={() => saveTemplateMutation.mutate()}
                    disabled={!templateName.trim()}
                    loading={saveTemplateMutation.isPending}
                    icon={<Save className="w-3 h-3" />}
                  >
                    Save
                  </Button>
                  <Button
                    className="flex-1 justify-center"
                    size="xs"
                    variant="outline"
                    onClick={() => { setShowSaveForm(false); setTemplateName('') }}
                  >
                    Cancel
                  </Button>
                </div>
              </div>
            )}
          </div>

          {/* Active job status */}
          {activeJob && (
            <div className="bg-white border border-gray-200 rounded-lg p-4">
              <h2 className="text-sm font-semibold text-gray-700 mb-3">Export Job</h2>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-gray-500">Status</span>
                  <StatusBadge status={activeJob.status} />
                </div>
                {activeJob.row_count != null && (
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-gray-500">Rows</span>
                    <span className="text-sm font-medium">{activeJob.row_count.toLocaleString()}</span>
                  </div>
                )}
                <div className="flex items-center justify-between">
                  <span className="text-xs text-gray-500">Format</span>
                  <span className="text-sm font-mono uppercase">{activeJob.format}</span>
                </div>
                {activeJob.created_at && (
                  <div className="flex items-center justify-between">
                    <span className="text-xs text-gray-500">Created</span>
                    <span className="text-xs text-gray-600">{formatDate(activeJob.created_at)}</span>
                  </div>
                )}
                {activeJob.error_message && (
                  <p className="text-xs text-red-600 mt-1">{activeJob.error_message}</p>
                )}
                {activeJob.status === 'complete' && (
                  <a
                    href={`/api/export/jobs/${activeJob.id}/download`}
                    target="_blank"
                    rel="noreferrer"
                    className="mt-2 w-full flex items-center justify-center gap-2 px-3 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700"
                  >
                    <Download className="w-4 h-4" />
                    Download
                  </a>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
