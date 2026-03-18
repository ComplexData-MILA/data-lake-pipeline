import { useState } from 'react'
import { useApi } from '../hooks/useApi'
import { useRefresh } from '../hooks/useRefresh'
import { BatchTable } from '../components/BatchTable'

interface BatchManifest {
  batch_id: string
  source: string
  state: 'pending' | 'inflight' | 'completed' | 'failed' | 'archived'
  created_at: string
  locked_by?: string
  locked_at?: string
  row_count?: number
  output_key?: string
  error?: string
}

interface ManifestsData {
  batches: BatchManifest[]
  cache_fetched_at: string
}

const STATES = ['pending', 'inflight', 'completed', 'failed', 'archived'] as const

export const Manifests = () => {
  const [refreshing, triggerRefresh] = useRefresh()
  const [selectedStates, setSelectedStates] = useState<string[]>(['pending', 'inflight', 'failed'])

  const toggleState = (state: string) => {
    setSelectedStates(prev =>
      prev.includes(state)
        ? prev.filter(s => s !== state)
        : [...prev, state]
    )
  }

  const stateParam = selectedStates.join(',')
  const { data, loading, error, refetch } = useApi<ManifestsData>(
    `/api/manifests?state=${stateParam}&refresh=${refreshing ? 'true' : 'false'}`
  )

  const handleRefresh = () => {
    triggerRefresh()
    refetch()
  }

  if (loading && !data) {
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
        <h1 className="text-2xl font-semibold">Batch Manifests</h1>
        <button
          onClick={handleRefresh}
          disabled={refreshing}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
        >
          {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
      </div>

      <div className="flex gap-2 items-center">
        <span className="text-sm font-medium">Filter by state:</span>
        <div className="flex gap-1">
          {STATES.map((state) => (
            <button
              key={state}
              onClick={() => toggleState(state)}
              className={`px-3 py-1 text-sm rounded-full border ${
                selectedStates.includes(state)
                  ? 'bg-blue-100 border-blue-300 text-blue-800'
                  : 'bg-gray-50 border-gray-200 text-gray-500'
              }`}
            >
              {state}
            </button>
          ))}
        </div>
      </div>

      {data && (
        <>
          <div className="text-sm text-gray-500">
            {data.batches.length} batches found
          </div>
          <BatchTable batches={data.batches} />

          {data.cache_fetched_at && (
            <div className="text-sm text-gray-400">
              Data fetched at: {new Date(data.cache_fetched_at).toLocaleString()}
            </div>
          )}
        </>
      )}
    </div>
  )
}
