import { useApi } from '../hooks/useApi'
import { useRefresh } from '../hooks/useRefresh'
import { StateBadge } from '../components/StateBadge'

interface PendingItem {
  batch_id: string
  source: string
  created_at: string
}

interface InflightItem {
  batch_id: string
  locked_by: string
  locked_at: string
}

interface FailedItem {
  batch_id: string
  error: string
}

interface QueueStatus {
  pending: PendingItem[]
  inflight: InflightItem[]
  failed: FailedItem[]
  cache_fetched_at: string
}

export const QueueMonitor = () => {
  const [refreshing, triggerRefresh] = useRefresh()
  const { data: status, loading, error, refetch } = useApi<QueueStatus>(
    `/api/queue/status?refresh=${refreshing ? 'true' : 'false'}`
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
        <h1 className="text-2xl font-semibold">Queue Monitor</h1>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
        >
          {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="space-y-4">
          <h2 className="text-lg font-medium flex items-center gap-2">
            <StateBadge state="pending" />
            Pending ({status.pending.length})
          </h2>
          <div className="bg-white rounded-lg border overflow-hidden">
            {status.pending.length === 0 ? (
              <div className="p-4 text-center text-gray-500 text-sm">No pending batches</div>
            ) : (
              <table className="min-w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Batch</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Source</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Created</th>
                  </tr>
                </thead>
                <tbody className="divide-y text-sm">
                  {status.pending.slice(0, 20).map((item) => (
                    <tr key={item.batch_id}>
                      <td className="px-4 py-2 font-mono text-xs">{item.batch_id}</td>
                      <td className="px-4 py-2">{item.source}</td>
                      <td className="px-4 py-2">{new Date(item.created_at).toLocaleTimeString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        <div className="space-y-4">
          <h2 className="text-lg font-medium flex items-center gap-2">
            <StateBadge state="inflight" />
            In Flight ({status.inflight.length})
          </h2>
          <div className="bg-white rounded-lg border overflow-hidden">
            {status.inflight.length === 0 ? (
              <div className="p-4 text-center text-gray-500 text-sm">No inflight batches</div>
            ) : (
              <table className="min-w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Batch</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Worker</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Locked</th>
                  </tr>
                </thead>
                <tbody className="divide-y text-sm">
                  {status.inflight.map((item) => (
                    <tr key={item.batch_id}>
                      <td className="px-4 py-2 font-mono text-xs">{item.batch_id}</td>
                      <td className="px-4 py-2">{item.locked_by}</td>
                      <td className="px-4 py-2">{new Date(item.locked_at).toLocaleTimeString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        <div className="space-y-4">
          <h2 className="text-lg font-medium flex items-center gap-2">
            <StateBadge state="failed" />
            Failed ({status.failed.length})
          </h2>
          <div className="bg-white rounded-lg border overflow-hidden">
            {status.failed.length === 0 ? (
              <div className="p-4 text-center text-gray-500 text-sm">No failed batches</div>
            ) : (
              <table className="min-w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Batch</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">Error</th>
                  </tr>
                </thead>
                <tbody className="divide-y text-sm">
                  {status.failed.slice(0, 20).map((item) => (
                    <tr key={item.batch_id}>
                      <td className="px-4 py-2 font-mono text-xs">{item.batch_id}</td>
                      <td className="px-4 py-2 text-red-600 truncate max-w-xs">{item.error}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>

      {status.cache_fetched_at && (
        <div className="text-sm text-gray-400">
          Data fetched at: {new Date(status.cache_fetched_at).toLocaleString()}
        </div>
      )}
    </div>
  )
}
