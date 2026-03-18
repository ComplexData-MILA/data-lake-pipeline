import { useState, useCallback } from 'react'
import { DataTable } from '../components/DataTable'
import { FilterBar } from '../components/FilterBar'
import { RecordFilter } from '../hooks/useRecords'

export const DataExplorer = () => {
  const [filters, setFilters] = useState<RecordFilter[]>([])

  const handleFilterChange = useCallback((newFilters: RecordFilter[]) => {
    setFilters(newFilters)
  }, [])

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Data Explorer</h1>
      </div>

      <div className="bg-gray-50 p-4 rounded-lg text-sm text-gray-600">
        <p>Query processed and annotated data from the data lake. Use filters to find specific content without writing SQL.</p>
      </div>

      <FilterBar stage="processed" filters={filters} onChange={handleFilterChange} />

      <DataTable stage="processed" filters={filters} />
    </div>
  )
}
