import { useState } from 'react'
import { RecordFilter } from './useRecords'
import { FilterStates } from '../types/filters'

interface UsePageFiltersOptions {
  initialFilters?: RecordFilter[]
}

interface UsePageFiltersReturn {
  filters: RecordFilter[]
  filterStates: FilterStates
  warning: string | null
  setFilters: (filters: RecordFilter[]) => void
  setFilterStates: (states: FilterStates) => void
  setWarning: (warning: string | null) => void
  clearWarning: () => void
}

export function usePageFilters(options: UsePageFiltersOptions = {}): UsePageFiltersReturn {
  const [filters, setFilters] = useState<RecordFilter[]>(options.initialFilters ?? [])
  const [filterStates, setFilterStates] = useState<FilterStates>({})
  const [warning, setWarning] = useState<string | null>(null)

  const clearWarning = () => setWarning(null)

  return {
    filters,
    filterStates,
    warning,
    setFilters,
    setFilterStates,
    setWarning,
    clearWarning,
  }
}
