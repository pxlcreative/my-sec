import { useState, useCallback, useEffect, useRef } from 'react'
import { useDropzone } from 'react-dropzone'
import { Upload, CheckCircle, XCircle, Loader2, ChevronRight, Info } from 'lucide-react'
import { Button } from '../components/Button'
import { useMutation, useQuery } from '@tanstack/react-query'
import { bulkMatch, getMatchJob, bulkTag, getPlatforms } from '../api/client'
import { StatusBadge } from '../components/StatusBadge'
import { useToast } from '../components/Toast'
import type { MatchResult, BulkMatchSyncResponse } from '../types'

type Step = 1 | 2 | 3 | 4 | 5

interface ParsedRow {
  [key: string]: string
}

function parseAddress(address: string): { city?: string; state?: string; zip?: string } {
  let s = address.trim()
  const result: { city?: string; state?: string; zip?: string } = {}

  const zipMatch = s.match(/\b(\d{5})(?:-\d{4})?\s*$/)
  if (zipMatch) {
    result.zip = zipMatch[1]
    s = s.slice(0, zipMatch.index).trim().replace(/,\s*$/, '').trim()
  }

  const stateMatch = s.match(/,?\s*([A-Za-z]{2})\s*$/)
  if (stateMatch) {
    result.state = stateMatch[1].toUpperCase()
    s = s.slice(0, stateMatch.index).trim().replace(/,\s*$/, '').trim()
  }

  const parts = s.split(',')
  const city = parts[parts.length - 1].trim()
  if (city) result.city = city

  return result
}

function parseCSV(text: string): { headers: string[]; rows: ParsedRow[] } {
  const lines = text.split('\n').filter((l) => l.trim().length > 0)
  if (lines.length === 0) return { headers: [], rows: [] }
  const headers = lines[0].split(',').map((h) => h.trim().replace(/^"|"$/g, '')).filter((h) => h.length > 0)
  const rows = lines.slice(1).map((line) => {
    const values = line.split(',').map((v) => v.trim().replace(/^"|"$/g, ''))
    const row: ParsedRow = {}
    headers.forEach((h, i) => {
      row[h] = values[i] ?? ''
    })
    return row
  })
  return { headers, rows }
}

function downloadCSV(results: MatchResult[]) {
  const headers = [
    'input_name',
    'input_city',
    'input_state',
    'input_zip',
    'best_score',
    'best_status',
    'matched_crd',
    'matched_name',
    'matched_city',
    'matched_state',
  ]
  const rows = results.map((r) => {
    const top = r.candidates[0]
    return [
      r.input_name,
      r.input_city ?? '',
      r.input_state ?? '',
      r.input_zip ?? '',
      r.best_score,
      r.best_status,
      top?.crd_number ?? '',
      top?.legal_name ?? '',
      top?.main_city ?? '',
      top?.main_state ?? '',
    ].join(',')
  })
  const csv = [headers.join(','), ...rows].join('\n')
  const blob = new Blob([csv], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = 'match_results.csv'
  a.click()
  URL.revokeObjectURL(url)
}

export default function BulkMatch() {
  const [step, setStep] = useState<Step>(1)
  const [csvText, setCsvText] = useState('')
  const [headers, setHeaders] = useState<string[]>([])
  const [previewRows, setPreviewRows] = useState<ParsedRow[]>([])
  const [allRows, setAllRows] = useState<ParsedRow[]>([])

  // Column mapping
  const [nameCol, setNameCol] = useState('')
  const [addressCol, setAddressCol] = useState('')
  const [cityCol, setCityCol] = useState('')
  const [stateCol, setStateCol] = useState('')
  const [zipCol, setZipCol] = useState('')

  // Configure
  const [minScore, setMinScore] = useState(50)
  const [maxCandidates, setMaxCandidates] = useState(3)

  // Async job
  const [jobId, setJobId] = useState<number | null>(null)
  const [matchResults, setMatchResults] = useState<BulkMatchSyncResponse | null>(null)

  // Results
  const [approved, setApproved] = useState<Set<string>>(new Set())
  const [selectedPlatformId, setSelectedPlatformId] = useState('')

  const { addToast } = useToast()
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const { data: platforms } = useQuery({
    queryKey: ['platforms'],
    queryFn: getPlatforms,
  })

  // Poll job status
  const { data: jobStatus } = useQuery({
    queryKey: ['match-job', jobId],
    queryFn: () => getMatchJob(jobId!),
    enabled: step === 3 && jobId !== null,
    refetchInterval: step === 3 ? 3000 : false,
  })

  useEffect(() => {
    if (!jobStatus) return
    if (jobStatus.status === 'complete' && jobStatus.results) {
      setMatchResults(jobStatus.results)
      setStep(4)
    } else if (jobStatus.status === 'failed') {
      addToast(`Match job failed: ${jobStatus.error_message ?? 'Unknown error'}`, 'error')
      setStep(2)
    }
  }, [jobStatus, addToast])

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current)
    }
  }, [])

  const onDrop = useCallback((acceptedFiles: File[]) => {
    const file = acceptedFiles[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = (e) => {
      const text = e.target?.result as string
      setCsvText(text)
      const { headers: h, rows } = parseCSV(text)
      setHeaders(h)
      setPreviewRows(rows.slice(0, 5))
      setAllRows(rows)
      // Auto-detect columns
      const nameCandidates = ['name', 'firm_name', 'company', 'legal_name', 'adviser']
      const addressCandidates = ['address', 'full_address', 'mailing_address', 'addr', 'location']
      const cityCandidates = ['city', 'main_city', 'town']
      const stateCandidates = ['state', 'main_state', 'st']
      const zipCandidates = ['zip', 'zipcode', 'zip_code', 'postal']
      const lowerH = h.map((x) => x.toLowerCase())
      setNameCol(h[lowerH.findIndex((x) => nameCandidates.includes(x))] ?? h[0] ?? '')
      const detectedAddress = h[lowerH.findIndex((x) => addressCandidates.includes(x))] ?? ''
      const detectedCity = h[lowerH.findIndex((x) => cityCandidates.includes(x))] ?? ''
      const detectedState = h[lowerH.findIndex((x) => stateCandidates.includes(x))] ?? ''
      const detectedZip = h[lowerH.findIndex((x) => zipCandidates.includes(x))] ?? ''
      setAddressCol(detectedAddress)
      setCityCol(detectedCity)
      setStateCol(detectedState)
      setZipCol(detectedZip)
    }
    reader.readAsText(file)
  }, [])

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'text/csv': ['.csv'] },
    multiple: false,
  })

  const matchMutation = useMutation({
    mutationFn: () => {
      const firms = allRows
        .filter((row) => nameCol && row[nameCol])
        .map((row) => {
          const parsed = addressCol && row[addressCol] ? parseAddress(row[addressCol]) : {}
          return {
            id: row[nameCol],
            name: row[nameCol] ?? '',
            city: cityCol ? row[cityCol] : (parsed.city ?? null),
            state: stateCol ? row[stateCol] : (parsed.state ?? null),
            zip: zipCol ? row[zipCol] : (parsed.zip ?? null),
          }
        })
      return bulkMatch({
        firms,
        min_score: minScore,
        max_candidates: maxCandidates,
      })
    },
    onSuccess: (data) => {
      if ('job_id' in data) {
        setJobId(data.job_id)
        setStep(3)
      } else {
        setMatchResults(data as BulkMatchSyncResponse)
        setStep(4)
      }
    },
    onError: () => addToast('Match request failed', 'error'),
  })

  const tagMutation = useMutation({
    mutationFn: () => {
      if (!matchResults) return Promise.resolve()
      const approvedCrds = matchResults.results
        .filter((r) => {
          const key = String(r.input_name)
          return approved.has(key) && r.candidates.length > 0
        })
        .map((r) => r.candidates[0].crd_number)
      return bulkTag({
        crd_numbers: approvedCrds,
        platform_id: Number(selectedPlatformId),
      })
    },
    onSuccess: () => {
      addToast('Firms tagged successfully', 'success')
      setStep(5)
    },
    onError: () => addToast('Tagging failed', 'error'),
  })

  const stepLabels = ['Upload CSV', 'Configure', 'Processing', 'Review Results', 'Tag & Export']

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900 mb-6">Bulk Name Match</h1>

      {/* Step indicator */}
      <div className="flex items-center gap-0 mb-8">
        {stepLabels.map((label, i) => {
          const stepNum = (i + 1) as Step
          const isActive = step === stepNum
          const isDone = step > stepNum
          return (
            <div key={i} className="flex items-center">
              <div className="flex items-center gap-2">
                <div
                  className={`w-7 h-7 rounded-full flex items-center justify-center text-sm font-semibold ${
                    isDone
                      ? 'bg-green-500 text-white'
                      : isActive
                      ? 'bg-brand-600 text-white'
                      : 'bg-gray-200 text-gray-500'
                  }`}
                >
                  {isDone ? <CheckCircle className="w-4 h-4" /> : stepNum}
                </div>
                <span
                  className={`text-sm font-medium ${
                    isActive ? 'text-brand-600' : isDone ? 'text-green-600' : 'text-gray-400'
                  }`}
                >
                  {label}
                </span>
              </div>
              {i < stepLabels.length - 1 && (
                <ChevronRight className="w-4 h-4 text-gray-300 mx-2" />
              )}
            </div>
          )
        })}
      </div>

      <div className="bg-white rounded-lg border border-gray-200 p-6">
        {/* Step 1: Upload */}
        {step === 1 && (
          <div className="space-y-6">
            <h2 className="text-lg font-semibold text-gray-800">Upload CSV File</h2>
            <div className="flex items-start gap-2 p-3 bg-blue-50 border border-blue-200 rounded-lg text-sm text-blue-800">
              <Info className="w-4 h-4 mt-0.5 shrink-0" />
              <span>
                Matching runs against loaded SEC firm data. If no data has been loaded yet, all rows will return{' '}
                <code className="bg-blue-100 px-1 rounded text-xs font-mono">no_match</code>.
                Run <code className="bg-blue-100 px-1 rounded text-xs font-mono">make load-data</code> first.
              </span>
            </div>
            <div
              {...getRootProps()}
              className={`border-2 border-dashed rounded-lg p-10 text-center cursor-pointer transition-colors ${
                isDragActive ? 'border-brand-600 bg-brand-50' : 'border-gray-300 hover:border-gray-400'
              }`}
            >
              <input {...getInputProps()} />
              <Upload className="w-10 h-10 text-gray-400 mx-auto mb-3" />
              {isDragActive ? (
                <p className="text-brand-600 font-medium">Drop the CSV file here</p>
              ) : (
                <>
                  <p className="text-gray-700 font-medium">Drag & drop a CSV file here</p>
                  <p className="text-gray-400 text-sm mt-1">or click to browse</p>
                </>
              )}
            </div>

            {headers.length > 0 && (
              <div className="space-y-4">
                <h3 className="text-sm font-semibold text-gray-700">Column Mapping</h3>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                  {[
                    { label: 'Name column', value: nameCol, set: setNameCol, required: true },
                    { label: 'Address (combined)', value: addressCol, set: setAddressCol, required: false },
                    { label: 'City column', value: cityCol, set: setCityCol, required: false },
                    { label: 'State column', value: stateCol, set: setStateCol, required: false },
                    { label: 'ZIP column', value: zipCol, set: setZipCol, required: false },
                  ].map(({ label, value, set, required }) => (
                    <div key={label}>
                      <label className="block text-xs font-medium text-gray-600 mb-1">
                        {label} {required && <span className="text-red-500">*</span>}
                      </label>
                      <select
                        value={value}
                        onChange={(e) => set(e.target.value)}
                        className="w-full text-sm border border-gray-300 rounded-md px-2 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
                      >
                        <option value="">— none —</option>
                        {headers.map((h, i) => (
                          <option key={i} value={h}>{h}</option>
                        ))}
                      </select>
                    </div>
                  ))}
                </div>

                {addressCol && (
                  <p className="text-xs text-blue-700 bg-blue-50 border border-blue-200 rounded px-3 py-2">
                    Address column <strong>{addressCol}</strong> will be parsed automatically into city, state, and ZIP. Individual city/state/ZIP columns take priority if also set.
                  </p>
                )}

                <div>
                  <h3 className="text-sm font-semibold text-gray-700 mb-2">
                    Preview (first 5 rows)
                  </h3>
                  <div className="overflow-x-auto border border-gray-200 rounded-lg">
                    <table className="text-xs w-full">
                      <thead className="bg-gray-50">
                        <tr>
                          {headers.map((h, i) => (
                            <th key={i} className="px-3 py-2 text-left font-semibold text-gray-600 border-b border-gray-200">{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {previewRows.map((row, i) => (
                          <tr key={i}>
                            {headers.map((h, i) => (
                              <td key={i} className="px-3 py-1.5 text-gray-700">{row[h]}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <Button onClick={() => setStep(2)} disabled={!nameCol}>
                  Next: Configure
                </Button>
              </div>
            )}
          </div>
        )}

        {/* Step 2: Configure */}
        {step === 2 && (
          <div className="space-y-6 max-w-lg">
            <h2 className="text-lg font-semibold text-gray-800">Configure Match Parameters</h2>
            <p className="text-sm text-gray-500">
              {allRows.length} rows loaded from CSV
            </p>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Minimum Score: <span className="font-bold text-brand-600">{minScore}</span>
              </label>
              <input
                type="range"
                min={0}
                max={100}
                value={minScore}
                onChange={(e) => setMinScore(Number(e.target.value))}
                className="w-full accent-brand-600"
              />
              <div className="flex justify-between text-xs text-gray-400 mt-1">
                <span>0 (loose)</span>
                <span>100 (exact)</span>
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Max Candidates per Firm</label>
              <select
                value={maxCandidates}
                onChange={(e) => setMaxCandidates(Number(e.target.value))}
                className="text-sm border border-gray-300 rounded-md px-3 py-1.5 focus:ring-2 focus:ring-brand-600 outline-none"
              >
                {[1, 2, 3, 4, 5].map((n) => (
                  <option key={n} value={n}>{n}</option>
                ))}
              </select>
            </div>

            <div className="flex items-center gap-3">
              <button
                onClick={() => setStep(1)}
                className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg text-sm hover:bg-gray-50"
              >
                Back
              </button>
              <button
                onClick={() => matchMutation.mutate()}
                disabled={matchMutation.isPending}
                className="flex items-center gap-2 px-5 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
              >
                {matchMutation.isPending && <Loader2 className="w-4 h-4 animate-spin" />}
                Run Match
              </button>
            </div>
          </div>
        )}

        {/* Step 3: Progress */}
        {step === 3 && (
          <div className="text-center py-12 space-y-4">
            <Loader2 className="w-12 h-12 text-brand-600 animate-spin mx-auto" />
            <h2 className="text-lg font-semibold text-gray-800">Processing Match...</h2>
            <p className="text-sm text-gray-500">
              Job #{jobId} — checking every 3 seconds
            </p>
            {jobStatus && (
              <p className="text-xs text-gray-400">Status: {jobStatus.status}</p>
            )}
          </div>
        )}

        {/* Step 4: Results */}
        {step === 4 && matchResults && (
          <div className="space-y-5">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-gray-800">Match Results</h2>
            </div>

            {/* No-data warning banner */}
            {matchResults.stats.confirmed === 0 && matchResults.stats.total > 0 && (
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-4 py-3 text-sm text-yellow-800">
                <span className="font-medium">0 confirmed matches</span> — this may indicate the firm database hasn't been loaded yet.{' '}
                Run <code className="bg-yellow-100 px-1 py-0.5 rounded text-xs font-mono">make load-data</code> to import SEC data, or see the README for instructions.
              </div>
            )}

            {/* Stats bar */}
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
              {[
                { label: 'Total', value: matchResults.stats.total, color: 'bg-gray-100 text-gray-700' },
                { label: 'Confirmed', value: matchResults.stats.confirmed, color: 'bg-green-100 text-green-800' },
                { label: 'Probable', value: matchResults.stats.probable, color: 'bg-yellow-100 text-yellow-800' },
                { label: 'Possible', value: matchResults.stats.possible, color: 'bg-orange-100 text-orange-800' },
                { label: 'No Match', value: matchResults.stats.no_match, color: 'bg-red-100 text-red-800' },
              ].map(({ label, value, color }) => (
                <div key={label} className={`rounded-lg p-3 ${color}`}>
                  <p className="text-2xl font-bold">{value}</p>
                  <p className="text-xs font-medium">{label}</p>
                </div>
              ))}
            </div>

            {/* Results table */}
            <div className="overflow-x-auto border border-gray-200 rounded-lg">
              <table className="w-full text-sm">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600">#</th>
                    <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600">Input Name</th>
                    <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600">Matched Firm</th>
                    <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600">CRD</th>
                    <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600">Score</th>
                    <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600">Status</th>
                    <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {matchResults.results.map((result, idx) => {
                    const top = result.candidates[0]
                    const key = String(result.input_name)
                    const isApproved = approved.has(key)
                    const isRejected = !isApproved && approved.size > 0

                    return (
                      <tr key={idx} className="hover:bg-gray-50">
                        <td className="px-3 py-2 text-gray-400 text-xs">{idx + 1}</td>
                        <td className="px-3 py-2 font-medium max-w-[180px] truncate">
                          {result.input_name}
                        </td>
                        <td className="px-3 py-2 max-w-[180px] truncate">
                          {top?.legal_name ?? '—'}
                        </td>
                        <td className="px-3 py-2 font-mono text-xs text-gray-500">
                          {top?.crd_number ?? '—'}
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex items-center gap-1">
                            <div className="w-12 bg-gray-200 rounded-full h-1.5">
                              <div
                                className="bg-brand-600 h-1.5 rounded-full"
                                style={{ width: `${result.best_score}%` }}
                              />
                            </div>
                            <span className="text-xs text-gray-500">{result.best_score}</span>
                          </div>
                        </td>
                        <td className="px-3 py-2">
                          <StatusBadge status={result.best_status} />
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex items-center gap-1">
                            <button
                              onClick={() => {
                                setApproved((prev) => {
                                  const next = new Set(prev)
                                  if (next.has(key)) next.delete(key)
                                  else next.add(key)
                                  return next
                                })
                              }}
                              className={`p-1 rounded transition-colors ${
                                isApproved
                                  ? 'text-green-600 bg-green-50'
                                  : 'text-gray-400 hover:text-green-600 hover:bg-green-50'
                              }`}
                              title="Approve"
                            >
                              <CheckCircle className="w-4 h-4" />
                            </button>
                            <button
                              onClick={() => {
                                setApproved((prev) => {
                                  const next = new Set(prev)
                                  next.delete(key)
                                  return next
                                })
                              }}
                              className={`p-1 rounded transition-colors ${
                                !isApproved && approved.has(key) === false && isRejected
                                  ? 'text-red-600 bg-red-50'
                                  : 'text-gray-400 hover:text-red-600 hover:bg-red-50'
                              }`}
                              title="Reject"
                            >
                              <XCircle className="w-4 h-4" />
                            </button>
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>

            <div className="flex items-center gap-3">
              <span className="text-sm text-gray-500">{approved.size} approved</span>
              <button
                onClick={() => setStep(5)}
                disabled={approved.size === 0}
                className="px-5 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
              >
                Next: Tag & Export
              </button>
              <button
                onClick={() => matchResults && downloadCSV(matchResults.results)}
                className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg text-sm hover:bg-gray-50"
              >
                Export All CSV
              </button>
            </div>
          </div>
        )}

        {/* Step 5: Tag & Export */}
        {step === 5 && matchResults && (
          <div className="space-y-6 max-w-lg">
            <h2 className="text-lg font-semibold text-gray-800">Tag & Export</h2>
            <p className="text-sm text-gray-500">
              {approved.size} firms approved for tagging
            </p>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Assign to Platform
              </label>
              <select
                value={selectedPlatformId}
                onChange={(e) => setSelectedPlatformId(e.target.value)}
                className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none"
              >
                <option value="">Select a platform...</option>
                {platforms?.map((p) => (
                  <option key={p.id} value={String(p.id)}>{p.name}</option>
                ))}
              </select>
            </div>

            <div className="flex items-center gap-3">
              <button
                onClick={() => tagMutation.mutate()}
                disabled={!selectedPlatformId || tagMutation.isPending}
                className="flex items-center gap-2 px-5 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
              >
                {tagMutation.isPending && <Loader2 className="w-4 h-4 animate-spin" />}
                Tag Approved Firms
              </button>
              <button
                onClick={() => downloadCSV(matchResults.results)}
                className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg text-sm hover:bg-gray-50"
              >
                Export Results CSV
              </button>
            </div>

            <button
              onClick={() => {
                setStep(1)
                setCsvText('')
                setHeaders([])
                setPreviewRows([])
                setAllRows([])
                setMatchResults(null)
                setApproved(new Set())
                setJobId(null)
              }}
              className="text-sm text-gray-500 hover:text-gray-700"
            >
              Start Over
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
