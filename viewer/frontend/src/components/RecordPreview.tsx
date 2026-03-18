import { useState } from 'react'

interface RecordPreviewProps {
  records: Record<string, unknown>[]
  loading?: boolean
}

const PAGE_SIZE = 20

export const RecordPreview = ({ records, loading }: RecordPreviewProps) => {
  const [page, setPage] = useState(0)

  if (loading) {
    return <div className="p-8 text-center text-gray-500">Loading...</div>
  }

  if (!records || records.length === 0) {
    return <div className="p-8 text-center text-gray-500">No records</div>
  }

  const columns = Object.keys(records[0] || {})
  const totalPages = Math.ceil(records.length / PAGE_SIZE)
  const pageRecords = records.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  return (
    <div className="space-y-4">
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
            {pageRecords.map((record, i) => (
              <tr key={i} className="hover:bg-gray-50">
                {columns.map((col) => (
                  <td key={col} className="px-4 py-2 text-sm whitespace-nowrap max-w-xs truncate">
                    {String(record[col] ?? '')}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <div className="text-sm text-gray-500">
            Showing {page * PAGE_SIZE + 1}-{Math.min((page + 1) * PAGE_SIZE, records.length)} of {records.length}
          </div>
          <div className="flex gap-2">
            <button
              className="px-3 py-1 text-sm border rounded disabled:opacity-50"
              onClick={() => setPage(p => Math.max(0, p - 1))}
              disabled={page === 0}
            >
              Previous
            </button>
            <button
              className="px-3 py-1 text-sm border rounded disabled:opacity-50"
              onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
