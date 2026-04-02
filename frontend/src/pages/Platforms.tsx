import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Tag, Eye, Loader2, Trash2, X, Check, FileText } from 'lucide-react'
import { getPlatforms, createPlatform, deletePlatform } from '../api/client'
import { Skeleton } from '../components/Skeleton'
import { useToast } from '../components/Toast'
import { formatDate } from '../utils'

export default function Platforms() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { addToast } = useToast()

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [saveBrochures, setSaveBrochures] = useState(false)
  const [confirmDeleteId, setConfirmDeleteId] = useState<number | null>(null)

  const { data: platforms, isLoading, error } = useQuery({
    queryKey: ['platforms'],
    queryFn: getPlatforms,
  })

  const deleteMutation = useMutation({
    mutationFn: (id: number) => deletePlatform(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['platforms'] })
      addToast('Platform deleted', 'success')
      setConfirmDeleteId(null)
    },
    onError: () => addToast('Failed to delete platform', 'error'),
  })

  const createMutation = useMutation({
    mutationFn: () => createPlatform({ name: name.trim(), description: description.trim() || undefined, save_brochures: saveBrochures }),
    onSuccess: (newPlatform) => {
      queryClient.invalidateQueries({ queryKey: ['platforms'] })
      addToast(`Platform "${newPlatform.name}" created`, 'success')
      setName('')
      setDescription('')
      setSaveBrochures(false)
    },
    onError: () => addToast('Failed to create platform', 'error'),
  })

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-8 w-48" />
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[1, 2, 3].map((i) => <Skeleton key={i} className="h-36" />)}
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">
        Failed to load platforms.
      </div>
    )
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Platforms</h1>
        <span className="text-sm text-gray-500">{platforms?.length ?? 0} platforms</span>
      </div>

      {/* Platform cards */}
      {!platforms || platforms.length === 0 ? (
        <div className="bg-white border border-gray-200 rounded-lg p-10 text-center text-gray-400 mb-8">
          <Tag className="w-10 h-10 mx-auto mb-3 opacity-50" />
          <p className="font-medium">No platforms yet</p>
          <p className="text-sm mt-1">Create your first platform below</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
          {platforms.map((platform) => (
            <div
              key={platform.id}
              className="bg-white border border-gray-200 rounded-lg p-5 flex flex-col justify-between hover:shadow-sm transition-shadow"
            >
              <div>
                <div className="flex items-start justify-between gap-3 mb-2">
                  <h3 className="font-bold text-gray-900 leading-tight">{platform.name}</h3>
                  <div className="flex items-center gap-1 flex-shrink-0">
                    <span className="text-xs bg-brand-100 text-brand-800 font-medium px-2 py-0.5 rounded-full whitespace-nowrap">
                      #{platform.id}
                    </span>
                    {confirmDeleteId === platform.id ? (
                      <>
                        <button
                          onClick={() => deleteMutation.mutate(platform.id)}
                          disabled={deleteMutation.isPending}
                          className="p-1 rounded text-red-600 hover:bg-red-50 disabled:opacity-50"
                          title="Confirm delete"
                        >
                          {deleteMutation.isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
                        </button>
                        <button
                          onClick={() => setConfirmDeleteId(null)}
                          className="p-1 rounded text-gray-400 hover:bg-gray-100"
                          title="Cancel"
                        >
                          <X className="w-3.5 h-3.5" />
                        </button>
                      </>
                    ) : (
                      <button
                        onClick={() => setConfirmDeleteId(platform.id)}
                        className="p-1 rounded text-gray-300 hover:text-red-500 hover:bg-red-50"
                        title="Delete platform"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    )}
                  </div>
                </div>
                {platform.description && (
                  <p className="text-sm text-gray-500 leading-relaxed mb-3">{platform.description}</p>
                )}
                <div className="flex items-center gap-2 flex-wrap mb-2">
                  {platform.save_brochures && (
                    <span className="inline-flex items-center gap-1 text-xs bg-green-50 text-green-700 border border-green-200 font-medium px-2 py-0.5 rounded-full">
                      <FileText className="w-3 h-3" />
                      Saves PDFs
                    </span>
                  )}
                </div>
                <p className="text-xs text-gray-400">
                  Created {formatDate(platform.created_at)}
                </p>
              </div>
              <div className="mt-4">
                <button
                  onClick={() => navigate(`/platforms/${platform.id}`)}
                  className="flex items-center gap-2 text-sm text-brand-600 hover:text-brand-700 font-medium"
                >
                  <Eye className="w-4 h-4" />
                  View Firms
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Create platform form */}
      <div className="bg-white border border-gray-200 rounded-lg p-5 max-w-lg">
        <h2 className="text-sm font-semibold text-gray-700 mb-4">Create New Platform</h2>
        <div className="space-y-3">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">
              Platform Name <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. RIA Aggregator Network"
              className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 focus:border-brand-600 outline-none"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">
              Description (optional)
            </label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Brief description of this platform..."
              rows={3}
              className="w-full text-sm border border-gray-300 rounded-md px-3 py-2 focus:ring-2 focus:ring-brand-600 outline-none resize-none"
            />
          </div>
          <label className="flex items-center gap-3 cursor-pointer select-none">
            <div
              onClick={() => setSaveBrochures(!saveBrochures)}
              className={`relative w-9 h-5 rounded-full transition-colors ${saveBrochures ? 'bg-brand-600' : 'bg-gray-300'}`}
            >
              <span
                className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform ${saveBrochures ? 'translate-x-4' : 'translate-x-0'}`}
              />
            </div>
            <span className="text-sm text-gray-700">Save Part 2 brochure PDFs for firms on this platform</span>
          </label>
          <button
            onClick={() => createMutation.mutate()}
            disabled={!name.trim() || createMutation.isPending}
            className="flex items-center gap-2 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 disabled:opacity-50"
          >
            {createMutation.isPending ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Plus className="w-4 h-4" />
            )}
            Create Platform
          </button>
        </div>
      </div>
    </div>
  )
}
