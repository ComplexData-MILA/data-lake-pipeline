import { useState, useCallback } from 'react'
import { DataTable } from '../components/DataTable'
import { FilterBar } from '../components/FilterBar'
import { RecordFilter } from '../hooks/useRecords'

export const LandingZone = () => {
  const [filters, setFilters] = useState<RecordFilter[]>([])

  const handleFilterChange = useCallback((newFilters: RecordFilter[]) => {
    setFilters(newFilters)
  }, [])

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Landing Zone</h1>
      </div>

      <div className="bg-gray-50 p-4 rounded-lg text-sm text-gray-600">
        <p>Query raw ingested data from all landing files. Use filters to narrow down results.</p>
      </div>

      <FilterBar stage="landing" filters={filters} onChange={handleFilterChange} />

      <DataTable stage="landing" filters={filters} />
    </div>
  )
}
