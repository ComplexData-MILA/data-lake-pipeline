import { useApi } from '../hooks/useApi'
import { useRefresh } from '../hooks/useRefresh'
import { PipelineStatus } from '../components/PipelineStatus'

interface SourceStats {
  rows: number
  batches: number
  failed: number
  success_rate: number
}

interface StuckBatch {
  batch_id: string
  locked_by: string
  locked_at: string
  duration_seconds: number
}

interface RecentError {
  batch_id: string
  source: string
  error: string
}

interface PipelineStatusData {
  batches: Record<string, number>
  total_rows_processed: number
  sources: Record<string, SourceStats>
  stuck_batches: StuckBatch[]
  recent_errors: RecentError[]
  cache_fetched_at: string
}

export const Dashboard = () => {
  const [refreshing, triggerRefresh] = useRefresh()
  const { data: status, loading, error, refetch } = useApi<PipelineStatusData>(
    `/api/status?refresh=${refreshing ? 'true' : 'false'}`
  )

  const handleRefresh = () => {
    triggerRefresh()
    refetch()
  }

  if (loading) {
    return <div className="p-8 text-center text-gray-500">Loading...</div>
  }

  if (error) {
    return (
      <div className="p-8 text-center text-red-500">
        Error: {error.message}
        <button onClick={handleRefresh} className="ml-4 px-4 py-2 bg-blue-600 text-white rounded">
          Retry
        </button>
      </div>
    )
  }

  if (!status) {
    return <div className="p-8 text-center text-gray-500">No data available</div>
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Pipeline Dashboard</h1>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
        >
          {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      <PipelineStatus status={status} />

      {status.cache_fetched_at && (
        <div className="text-sm text-gray-400">
          Data fetched at: {new Date(status.cache_fetched_at).toLocaleString()}
        </div>
      )}
    </div>
  )
}
