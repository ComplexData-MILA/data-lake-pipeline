import { useState } from 'react'
import { StateBadge } from './StateBadge'

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

interface BatchTableProps {
  batches: BatchManifest[]
}

type SortKey = 'batch_id' | 'source' | 'state' | 'created_at' | 'row_count'
type SortDir = 'asc' | 'desc'

export const BatchTable = ({ batches }: BatchTableProps) => {
  const [sortKey, setSortKey] = useState<SortKey>('created_at')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const sortedBatches = [...batches].sort((a, b) => {
    let aVal = a[sortKey]
    let bVal = b[sortKey]
    
    if (sortKey === 'row_count') {
      aVal = aVal ?? 0
      bVal = bVal ?? 0
    }
    
    if (aVal === undefined || aVal === null) return 1
    if (bVal === undefined || bVal === null) return -1
    
    if (aVal < bVal) return sortDir === 'asc' ? -1 : 1
    if (aVal > bVal) return sortDir === 'asc' ? 1 : -1
    return 0
  })

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('desc')
    }
  }

  const SortHeader = ({ label, sortKey: key }: { label: string; sortKey: SortKey }) => (
    <th
      className="px-4 py-2 text-left text-sm font-medium text-gray-500 cursor-pointer hover:bg-gray-100"
      onClick={() => toggleSort(key)}
    >
      {label} {sortKey === key && (sortDir === 'asc' ? '↑' : '↓')}
    </th>
  )

  return (
    <div className="bg-white rounded-lg border overflow-hidden">
      <table className="min-w-full">
        <thead className="bg-gray-50">
          <tr>
            <SortHeader label="Batch ID" sortKey="batch_id" />
            <SortHeader label="Source" sortKey="source" />
            <SortHeader label="State" sortKey="state" />
            <SortHeader label="Created" sortKey="created_at" />
            <SortHeader label="Rows" sortKey="row_count" />
          </tr>
        </thead>
        <tbody className="divide-y">
          {sortedBatches.map((batch) => (
            <tr key={batch.batch_id} className="hover:bg-gray-50">
              <td className="px-4 py-2 text-sm font-mono">{batch.batch_id}</td>
              <td className="px-4 py-2 text-sm">{batch.source}</td>
              <td className="px-4 py-2 text-sm">
                <StateBadge state={batch.state} />
              </td>
              <td className="px-4 py-2 text-sm">
                {new Date(batch.created_at).toLocaleString()}
              </td>
              <td className="px-4 py-2 text-sm text-right">
                {batch.row_count?.toLocaleString() ?? '-'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {batches.length === 0 && (
        <div className="p-8 text-center text-gray-500">No batches found</div>
      )}
    </div>
  )
}
