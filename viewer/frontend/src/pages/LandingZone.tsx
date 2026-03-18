import { useState } from 'react'
import { useApi } from '../hooks/useApi'
import { useRefresh } from '../hooks/useRefresh'
import { FileBrowser } from '../components/FileBrowser'

interface FileItem {
  key: string
  source: string
  size_bytes: number
  age_seconds: number
  last_modified: string
}

interface LandingZoneStatus {
  files: FileItem[]
  total_size_bytes: number
  cache_fetched_at: string
}

const formatBytes = (bytes: number) => {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

export const LandingZone = () => {
  const [refreshing, triggerRefresh] = useRefresh()
  const [sourceFilter, setSourceFilter] = useState<string>('')
  
  const { data: status, loading, error, refetch } = useApi<LandingZoneStatus>(
    `/api/landing/status?refresh=${refreshing ? 'true' : 'false'}`
  )

  const handleRefresh = () => {
    triggerRefresh()
    refetch()
  }

  const sources = status?.files
    ? [...new Set(status.files.map(f => f.source))]
    : []

  const filteredFiles = status?.files
    ? sourceFilter
      ? status.files.filter(f => f.source === sourceFilter)
      : status.files
    : []

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

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Landing Zone</h1>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
        >
          {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      {status && (
        <>
          <div className="flex gap-4 items-center">
            <div className="flex items-center gap-2">
              <label className="text-sm font-medium">Filter by source:</label>
              <select
                className="border rounded px-3 py-1.5"
                value={sourceFilter}
                onChange={(e) => setSourceFilter(e.target.value)}
              >
                <option value="">All sources</option>
                {sources.map(s => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
            </div>
            <div className="text-sm text-gray-500">
              {filteredFiles.length} files, {formatBytes(status.total_size_bytes)} total
            </div>
          </div>

          <FileBrowser files={filteredFiles} />

          {status.cache_fetched_at && (
            <div className="text-sm text-gray-400">
              Data fetched at: {new Date(status.cache_fetched_at).toLocaleString()}
            </div>
          )}
        </>
      )}
    </div>
  )
}
