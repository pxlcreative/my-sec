import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { HardDrive, Cloud, CheckCircle, XCircle, Eye, EyeOff, Loader2, Save, Zap } from 'lucide-react'
import { getStorageSettings, updateStorageSettings, testStorageConnection } from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { useToast } from '../components/Toast'
import type { StorageSettingsOut, StorageTestResult } from '../types'

type Backend = 'local' | 's3' | 'azure'

export default function Settings() {
  const queryClient = useQueryClient()
  const { addToast } = useToast()

  const [backend, setBackend] = useState<Backend>('local')
  const [form, setForm] = useState<Partial<StorageSettingsOut>>({})
  const [showSecrets, setShowSecrets] = useState(false)
  const [testResult, setTestResult] = useState<StorageTestResult | null>(null)

  const { data: current, isLoading, error } = useQuery({
    queryKey: ['storage-settings'],
    queryFn: getStorageSettings,
  })

  useEffect(() => {
    if (current) {
      setBackend(current.backend)
      setForm(current)
    }
  }, [current])

  const saveMutation = useMutation({
    mutationFn: () => updateStorageSettings({ ...form, backend }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['storage-settings'] })
      addToast('Storage settings saved', 'success')
    },
    onError: () => addToast('Failed to save settings', 'error'),
  })

  const testMutation = useMutation({
    mutationFn: testStorageConnection,
    onSuccess: (result) => {
      setTestResult(result)
      addToast(result.message, result.success ? 'success' : 'error')
    },
    onError: () => addToast('Connection test failed', 'error'),
  })

  const set = (field: keyof StorageSettingsOut, value: string) =>
    setForm((f) => ({ ...f, [field]: value || null }))

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <Skeleton className="h-64" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
        Failed to load settings.
      </div>
    )
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
        {current?.updated_at && (
          <span className="text-xs text-gray-400">
            Last saved {new Date(current.updated_at).toLocaleString()}
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Backend selector */}
        <div className="bg-white border border-gray-200 rounded-lg p-5">
          <h2 className="text-sm font-semibold text-gray-700 mb-4">Storage Backend</h2>
          <div className="space-y-3">
            <BackendOption
              value="local"
              current={backend}
              icon={<HardDrive className="w-4 h-4" />}
              label="Local Disk"
              description="Store files on the server filesystem (default)"
              onChange={setBackend}
            />
            <BackendOption
              value="s3"
              current={backend}
              icon={<Cloud className="w-4 h-4" />}
              label="Amazon S3"
              description="AWS S3 or S3-compatible (MinIO, Backblaze B2, etc.)"
              onChange={setBackend}
            />
            <BackendOption
              value="azure"
              current={backend}
              icon={<Cloud className="w-4 h-4" />}
              label="Azure Blob Storage"
              description="Microsoft Azure Blob Storage container"
              onChange={setBackend}
            />
          </div>
        </div>

        {/* Credential fields */}
        <div className="lg:col-span-2 bg-white border border-gray-200 rounded-lg p-5">
          <h2 className="text-sm font-semibold text-gray-700 mb-4">
            {backend === 'local' ? 'Local Storage' : backend === 's3' ? 'S3 Configuration' : 'Azure Configuration'}
          </h2>

          {backend === 'local' && (
            <div className="flex items-start gap-3 bg-gray-50 rounded-lg p-4 text-sm text-gray-600">
              <HardDrive className="w-5 h-5 text-gray-400 flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-gray-700 mb-1">No credentials required</p>
                <p>PDFs are stored on the server at <code className="bg-gray-200 px-1 rounded text-xs">DATA_DIR/brochures/</code>. This is the default and requires no additional configuration.</p>
              </div>
            </div>
          )}

          {backend === 's3' && (
            <div className="space-y-4">
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <Field
                  label="Bucket Name"
                  required
                  value={form.s3_bucket ?? ''}
                  onChange={(v) => set('s3_bucket', v)}
                  placeholder="my-brochures-bucket"
                />
                <Field
                  label="Region"
                  value={form.s3_region ?? ''}
                  onChange={(v) => set('s3_region', v)}
                  placeholder="us-east-1"
                />
                <Field
                  label="Access Key ID"
                  value={form.s3_access_key_id ?? ''}
                  onChange={(v) => set('s3_access_key_id', v)}
                  placeholder="AKIAIOSFODNN7EXAMPLE"
                />
                <SecretField
                  label="Secret Access Key"
                  value={form.s3_secret_access_key ?? ''}
                  onChange={(v) => set('s3_secret_access_key', v)}
                  show={showSecrets}
                  onToggle={() => setShowSecrets((s) => !s)}
                  placeholder="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
                />
              </div>
              <Field
                label="Endpoint URL"
                value={form.s3_endpoint_url ?? ''}
                onChange={(v) => set('s3_endpoint_url', v)}
                placeholder="https://s3.amazonaws.com (leave blank for AWS; set for MinIO etc.)"
              />
            </div>
          )}

          {backend === 'azure' && (
            <div className="space-y-4">
              <Field
                label="Container Name"
                required
                value={form.azure_container ?? ''}
                onChange={(v) => set('azure_container', v)}
                placeholder="brochures"
              />
              <SecretField
                label="Connection String"
                value={form.azure_connection_string ?? ''}
                onChange={(v) => set('azure_connection_string', v)}
                show={showSecrets}
                onToggle={() => setShowSecrets((s) => !s)}
                placeholder="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
                textarea
              />
            </div>
          )}

          {/* Actions */}
          <div className="mt-6 flex items-center gap-3 flex-wrap">
            <button
              onClick={() => saveMutation.mutate()}
              disabled={saveMutation.isPending}
              className="flex items-center gap-2 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
            >
              {saveMutation.isPending ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Save className="w-4 h-4" />
              )}
              Save Settings
            </button>

            <button
              onClick={() => testMutation.mutate()}
              disabled={testMutation.isPending}
              className="flex items-center gap-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg text-sm font-medium hover:bg-gray-50 disabled:opacity-50"
            >
              {testMutation.isPending ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Zap className="w-4 h-4" />
              )}
              Test Connection
            </button>

            {testResult && (
              <div className={`flex items-center gap-1.5 text-sm font-medium ${testResult.success ? 'text-green-600' : 'text-red-600'}`}>
                {testResult.success ? (
                  <CheckCircle className="w-4 h-4" />
                ) : (
                  <XCircle className="w-4 h-4" />
                )}
                {testResult.success ? 'Connected' : 'Failed'}
              </div>
            )}
          </div>

          {testResult && !testResult.success && (
            <p className="mt-2 text-xs text-red-600 bg-red-50 border border-red-200 rounded p-2">
              {testResult.message}
            </p>
          )}
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function BackendOption({
  value,
  current,
  icon,
  label,
  description,
  onChange,
}: {
  value: Backend
  current: Backend
  icon: React.ReactNode
  label: string
  description: string
  onChange: (v: Backend) => void
}) {
  const active = value === current
  return (
    <label
      className={`flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
        active ? 'border-brand-600 bg-brand-50' : 'border-gray-200 hover:border-gray-300'
      }`}
    >
      <input
        type="radio"
        name="backend"
        value={value}
        checked={active}
        onChange={() => onChange(value)}
        className="mt-0.5 accent-brand-600"
      />
      <div>
        <div className={`flex items-center gap-1.5 text-sm font-medium ${active ? 'text-brand-700' : 'text-gray-700'}`}>
          {icon}
          {label}
        </div>
        <p className="text-xs text-gray-500 mt-0.5">{description}</p>
      </div>
    </label>
  )
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  required,
}: {
  label: string
  value: string
  onChange: (v: string) => void
  placeholder?: string
  required?: boolean
}) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-600 mb-1">
        {label} {required && <span className="text-red-500">*</span>}
      </label>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 focus:border-brand-600 outline-none"
      />
    </div>
  )
}

function SecretField({
  label,
  value,
  onChange,
  show,
  onToggle,
  placeholder,
  textarea,
}: {
  label: string
  value: string
  onChange: (v: string) => void
  show: boolean
  onToggle: () => void
  placeholder?: string
  textarea?: boolean
}) {
  const inputClass =
    'w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 focus:border-brand-600 outline-none pr-10'

  return (
    <div>
      <label className="block text-xs font-medium text-gray-600 mb-1">{label}</label>
      <div className="relative">
        {textarea ? (
          <textarea
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            rows={3}
            className={`${inputClass} resize-none`}
            style={{ fontFamily: show ? undefined : 'monospace' }}
          />
        ) : (
          <input
            type={show ? 'text' : 'password'}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            className={inputClass}
          />
        )}
        <button
          type="button"
          onClick={onToggle}
          className="absolute right-2 top-2 p-1 text-gray-400 hover:text-gray-600"
          tabIndex={-1}
          title={show ? 'Hide' : 'Show'}
        >
          {show ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
        </button>
      </div>
      {value === '***' && (
        <p className="text-xs text-gray-400 mt-1">A value is saved. Re-enter to change it.</p>
      )}
    </div>
  )
}
