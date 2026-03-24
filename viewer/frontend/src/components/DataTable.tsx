import { useState, useEffect, useCallback } from 'react'
import { useRecords, RecordFilter } from '../hooks/useRecords'
import { RecordDetailDialog } from './RecordDetailDialog'
import { FilterStates } from '../types/filters'

interface DataTableProps {
  filters: RecordFilter[]
  filterStates?: FilterStates
  pageSize?: number
  onWarning?: (warning: string | null) => void
}

export const DataTable = ({ filters, filterStates, pageSize = 50, onWarning }: DataTableProps) => {
  const { data, loading, error, warning, fetchRecords } = useRecords()
  const [page, setPage] = useState(1)
  const [sortBy, setSortBy] = useState<string | undefined>()
  const [sortDesc, setSortDesc] = useState(false)
  const [selectedRecord, setSelectedRecord] = useState<Record<string, unknown> | null>(null)
  const [dialogOpen, setDialogOpen] = useState(false)

  useEffect(() => {
    fetchRecords(page, pageSize, filters, sortBy, sortDesc, filterStates)
  }, [page, pageSize, filters, sortBy, sortDesc, filterStates, fetchRecords])

  useEffect(() => {
    if (onWarning) onWarning(warning)
  }, [warning, onWarning])

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
    return <div className="p-8 text-center text-muted-foreground">Loading...</div>
  }

  if (error) {
    return (
      <div className="p-8 text-center text-destructive">
        Error: {error}
        <button
          onClick={() => fetchRecords(page, pageSize, filters, sortBy, sortDesc, filterStates)}
          className="ml-4 px-4 py-2 bg-primary text-primary-foreground rounded"
        >
          Retry
        </button>
      </div>
    )
  }

  if (!data || data.records.length === 0) {
    return <div className="p-8 text-center text-muted-foreground">No records found</div>
  }

  const startRow = (page - 1) * pageSize + 1
  const endRow = Math.min(page * pageSize, data.total_count)

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-lg border overflow-x-auto">
        <table className="min-w-full">
          <thead className="bg-muted/50">
            <tr>
              {data.columns.map((col) => (
                <th
                  key={col}
                  className="px-4 py-2 text-left text-sm font-medium text-muted-foreground whitespace-nowrap cursor-pointer hover:bg-muted"
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
                className="hover:bg-muted/50 cursor-pointer"
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
        <div className="text-sm text-muted-foreground">
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
