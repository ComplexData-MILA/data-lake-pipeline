import { useState, useCallback } from 'react'
import { DataTable } from '../components/DataTable'
import { FilterBar } from '../components/FilterBar'
import { RecordFilter } from '../hooks/useRecords'

export const QueueMonitor = () => {
  const [filters, setFilters] = useState<RecordFilter[]>([
    { field: 'state', operator: 'eq', value: 'pending' }
  ])

  const handleFilterChange = useCallback((newFilters: RecordFilter[]) => {
    setFilters(newFilters)
  }, [])

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Queue Monitor</h1>
      </div>

      <div className="bg-gray-50 p-4 rounded-lg text-sm text-gray-600">
        <p>Monitor and query batch manifests across all states. Filter by state to see pending, in-flight, completed, or failed batches.</p>
      </div>

      <FilterBar stage="queue" filters={filters} onChange={handleFilterChange} />

      <DataTable stage="queue" filters={filters} />
    </div>
  )
}
