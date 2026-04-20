interface StatusBadgeProps {
  status: string
  className?: string
}

function getStatusColor(status: string): string {
  const s = status.toLowerCase()
  if (
    s === 'registered' ||
    s === 'active' ||
    s === 'complete' ||
    s === 'completed' ||
    s === 'confirmed' ||
    s === 'success' ||
    s === 'delivered'
  ) {
    return 'bg-green-100 text-green-800 border-green-200'
  }
  if (
    s === 'failed' ||
    s === 'withdrawn' ||
    s === 'error' ||
    s === 'no_match' ||
    s === 'deregistered'
  ) {
    return 'bg-red-100 text-red-800 border-red-200'
  }
  if (
    s === 'pending' ||
    s === 'running' ||
    s === 'probable' ||
    s === 'processing' ||
    s === 'in_progress'
  ) {
    return 'bg-yellow-100 text-yellow-800 border-yellow-200'
  }
  if (s === 'possible') {
    return 'bg-orange-100 text-orange-800 border-orange-200'
  }
  if (s === 'era' || s === 'exempt') {
    return 'bg-purple-100 text-purple-800 border-purple-200'
  }
  if (s === 'inactive') {
    return 'bg-gray-100 text-gray-500 border-gray-200'
  }
  return 'bg-gray-100 text-gray-700 border-gray-200'
}

export function StatusBadge({ status, className = '' }: StatusBadgeProps) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${getStatusColor(status)} ${className}`}
    >
      {status.replace(/_/g, ' ')}
    </span>
  )
}
