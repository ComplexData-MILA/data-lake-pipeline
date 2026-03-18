import { useState, useEffect, useCallback } from 'react'
import { useRecords, RecordFilter } from '../hooks/useRecords'
import { RecordDetailDialog } from './RecordDetailDialog'

interface DataTableProps {
  stage: 'landing' | 'queue' | 'processed'
  filters: RecordFilter[]
  pageSize?: number
}

export const DataTable = ({ stage, filters, pageSize = 50 }: DataTableProps) => {
  const { data, loading, error, fetchRecords } = useRecords(stage)
  const [page, setPage] = useState(1)
  const [sortBy, setSortBy] = useState<string | undefined>()
  const [sortDesc, setSortDesc] = useState(false)
  const [selectedRecord, setSelectedRecord] = useState<Record<string, unknown> | null>(null)
  const [dialogOpen, setDialogOpen] = useState(false)

  useEffect(() => {
    fetchRecords(page, pageSize, filters, sortBy, sortDesc)
  }, [page, pageSize, filters, sortBy, sortDesc, fetchRecords])

  const handleSort = useCallback((column: string) => {
    if (sortBy === column) {
      setSortDesc(!sortDesc)
    } else {
      setSortBy(column)
      setSortDesc(false)
    }
  }, [sortBy, sortDesc])

  const handleRowClick = useCallback((record: Record<string, unknown>) => {
    setSelectedRecord(record)
    setDialogOpen(true)
  }, [])

  if (loading) {
    return <div className="p-8 text-center text-gray-500">Loading...</div>
  }

  if (error) {
    return (
      <div className="p-8 text-center text-red-500">
        Error: {error}
        <button
          onClick={() => fetchRecords(page, pageSize, filters, sortBy, sortDesc)}
          className="ml-4 px-4 py-2 bg-blue-600 text-white rounded"
        >
          Retry
        </button>
      </div>
    )
  }

  if (!data || data.records.length === 0) {
    return <div className="p-8 text-center text-gray-500">No records found</div>
  }

  const startRow = (page - 1) * pageSize + 1
  const endRow = Math.min(page * pageSize, data.total_count)

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-lg border overflow-x-auto">
        <table className="min-w-full">
          <thead className="bg-gray-50">
            <tr>
              {data.columns.map((col) => (
                <th
                  key={col}
                  className="px-4 py-2 text-left text-sm font-medium text-gray-500 whitespace-nowrap cursor-pointer hover:bg-gray-100"
                  onClick={() => handleSort(col)}
                >
                  <div className="flex items-center gap-1">
                    {col}
                    {sortBy === col && (
                      <span className="text-xs">{sortDesc ? '▼' : '▲'}</span>
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y">
            {data.records.map((record, i) => (
              <tr
                key={i}
                className="hover:bg-gray-50 cursor-pointer"
                onClick={() => handleRowClick(record)}
              >
                {data.columns.map((col) => (
                  <td key={col} className="px-4 py-2 text-sm whitespace-nowrap max-w-xs truncate">
                    {formatValue(record[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-between">
        <div className="text-sm text-gray-500">
          Showing {startRow}-{endRow} of {data.total_count}
        </div>
        <div className="flex gap-2 items-center">
          <button
            className="px-3 py-1 text-sm border rounded disabled:opacity-50"
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page === 1}
          >
            Previous
          </button>
          <span className="text-sm">
            Page {page} of {data.total_pages}
          </span>
          <button
            className="px-3 py-1 text-sm border rounded disabled:opacity-50"
            onClick={() => setPage(p => Math.min(data.total_pages, p + 1))}
            disabled={page >= data.total_pages}
          >
            Next
          </button>
        </div>
      </div>

      <RecordDetailDialog
        record={selectedRecord}
        open={dialogOpen}
        onOpenChange={setDialogOpen}
      />
    </div>
  )
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}
