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

interface PipelineStatusProps {
  status: PipelineStatusData
}

const formatNumber = (n: number) => n.toLocaleString()

export const PipelineStatus = ({ status }: PipelineStatusProps) => {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-white p-4 rounded-lg border">
          <div className="text-sm text-gray-500">Pending</div>
          <div className="text-2xl font-semibold">{formatNumber(status.batches.pending || 0)}</div>
        </div>
        <div className="bg-white p-4 rounded-lg border">
          <div className="text-sm text-gray-500">In Flight</div>
          <div className="text-2xl font-semibold">{formatNumber(status.batches.inflight || 0)}</div>
        </div>
        <div className="bg-white p-4 rounded-lg border">
          <div className="text-sm text-gray-500">Completed</div>
          <div className="text-2xl font-semibold text-green-600">{formatNumber(status.batches.completed || 0)}</div>
        </div>
        <div className="bg-white p-4 rounded-lg border">
          <div className="text-sm text-gray-500">Failed</div>
          <div className="text-2xl font-semibold text-red-600">{formatNumber(status.batches.failed || 0)}</div>
        </div>
      </div>

      <div className="bg-white p-4 rounded-lg border">
        <div className="text-sm text-gray-500">Total Rows Processed</div>
        <div className="text-3xl font-bold">{formatNumber(status.total_rows_processed)}</div>
      </div>

      {Object.keys(status.sources).length > 0 && (
        <div className="bg-white rounded-lg border overflow-hidden">
          <table className="min-w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-2 text-left text-sm font-medium text-gray-500">Source</th>
                <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">Rows</th>
                <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">Batches</th>
                <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">Failed</th>
                <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">Success Rate</th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {Object.entries(status.sources).map(([source, stats]) => (
                <tr key={source}>
                  <td className="px-4 py-2 text-sm">{source}</td>
                  <td className="px-4 py-2 text-sm text-right">{formatNumber(stats.rows)}</td>
                  <td className="px-4 py-2 text-sm text-right">{formatNumber(stats.batches)}</td>
                  <td className="px-4 py-2 text-sm text-right">{formatNumber(stats.failed)}</td>
                  <td className="px-4 py-2 text-sm text-right">{stats.success_rate.toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {status.stuck_batches && status.stuck_batches.length > 0 && (
        <div className="bg-yellow-50 border border-yellow-200 p-4 rounded-lg">
          <h3 className="font-medium text-yellow-800 mb-2">Stuck Batches ({status.stuck_batches.length})</h3>
          <div className="text-sm text-yellow-700">
            {status.stuck_batches.map((batch) => (
              <div key={batch.batch_id} className="py-1">
                <span className="font-mono">{batch.batch_id}</span> - locked by {batch.locked_by} for {Math.round(batch.duration_seconds / 60)}min
              </div>
            ))}
          </div>
        </div>
      )}

      {status.recent_errors && status.recent_errors.length > 0 && (
        <div className="bg-red-50 border border-red-200 p-4 rounded-lg">
          <h3 className="font-medium text-red-800 mb-2">Recent Errors</h3>
          <div className="text-sm text-red-700 space-y-1">
            {status.recent_errors.slice(0, 5).map((err, i) => (
              <div key={i} className="py-1">
                <span className="font-mono">{err.batch_id}</span> ({err.source}): {err.error}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
