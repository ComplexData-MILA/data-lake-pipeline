import { useState } from 'react'
import { fetchJson } from '../lib/api'

interface QueryEditorProps {
  onResults?: (results: Record<string, unknown>[]) => void
}

export const QueryEditor = ({ onResults }: QueryEditorProps) => {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<Record<string, unknown>[]>([])
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const executeQuery = async () => {
    if (!query.trim()) return
    
    setLoading(true)
    setError(null)
    
    try {
      const response = await fetchJson<{ rows: Record<string, unknown>[] }>('/api/query', {
        method: 'POST',
        body: JSON.stringify({ query })
      })
      setResults(response.rows)
      onResults?.(response.rows)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Query failed')
    } finally {
      setLoading(false)
    }
  }

  const columns = results.length > 0 ? Object.keys(results[0]) : []

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <textarea
          className="w-full h-32 p-3 border rounded-lg font-mono text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          placeholder="SELECT * FROM lake.processed_posts LIMIT 100"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <div className="flex gap-2">
          <button
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
            onClick={executeQuery}
            disabled={loading || !query.trim()}
          >
            {loading ? 'Running...' : 'Execute'}
          </button>
          <button
            className="px-4 py-2 border rounded-lg hover:bg-gray-50"
            onClick={() => { setQuery(''); setResults([]); setError(null) }}
          >
            Clear
          </button>
        </div>
      </div>

      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
          {error}
        </div>
      )}

      {results.length > 0 && (
        <div className="bg-white rounded-lg border overflow-x-auto">
          <table className="min-w-full">
            <thead className="bg-gray-50">
              <tr>
                {columns.map((col) => (
                  <th key={col} className="px-4 py-2 text-left text-sm font-medium text-gray-500 whitespace-nowrap">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y">
              {results.slice(0, 100).map((row, i) => (
                <tr key={i} className="hover:bg-gray-50">
                  {columns.map((col) => (
                    <td key={col} className="px-4 py-2 text-sm whitespace-nowrap max-w-xs truncate">
                      {String(row[col] ?? '')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {results.length > 100 && (
            <div className="p-2 text-center text-sm text-gray-500 bg-gray-50">
              Showing first 100 of {results.length} rows
            </div>
          )}
        </div>
      )}
    </div>
  )
}
