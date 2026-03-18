interface StateBadgeProps {
  state: string
}

const stateColors: Record<string, string> = {
  pending: 'bg-yellow-100 text-yellow-800',
  inflight: 'bg-blue-100 text-blue-800',
  completed: 'bg-green-100 text-green-800',
  failed: 'bg-red-100 text-red-800',
  archived: 'bg-gray-100 text-gray-800'
}

export const StateBadge = ({ state }: StateBadgeProps) => {
  const colorClass = stateColors[state] || 'bg-gray-100 text-gray-800'
  
  return (
    <span className={`px-2 py-1 rounded-full text-xs font-medium ${colorClass}`}>
      {state}
    </span>
  )
}
