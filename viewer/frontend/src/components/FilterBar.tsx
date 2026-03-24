import { useState, useEffect, useCallback, useRef } from 'react'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select'
import { FilterQueryBuilder } from './FilterQueryBuilder'
import { RecordFilter } from '../hooks/useRecords'
import { fetchJson } from '../lib/api'
import { NamedFilter, FilterStates } from '../types/filters'

interface FilterBarProps {
  filters: RecordFilter[]
  onChange: (filters: RecordFilter[]) => void
  filterStates: FilterStates
  onFilterStatesChange: (states: FilterStates) => void
  showStateFilter?: boolean
  showAnnotationFilters?: boolean
}

export const FilterBar = ({
  filters,
  onChange,
  filterStates,
  onFilterStatesChange,
  showStateFilter = false,
  showAnnotationFilters = true,
}: FilterBarProps) => {
  const [sources, setSources] = useState<string[]>([])
  const [availableFilters, setAvailableFilters] = useState<NamedFilter[]>([])
  const [textSearch, setTextSearch] = useState('')
  const [sourceFilter, setSourceFilter] = useState('')
  const [stateFilter, setStateFilter] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const abortControllerRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    abortControllerRef.current = controller
    fetchJson<string[]>('/api/sources', { signal: controller.signal })
      .then(setSources)
      .catch((err) => {
        if (err.name !== 'AbortError') setSources([])
      })
    return () => controller.abort()
  }, [])

  useEffect(() => {
    if (showAnnotationFilters) {
      const controller = new AbortController()
      abortControllerRef.current = controller
      fetchJson<{ filters: string[] }>('/api/filters', { signal: controller.signal })
        .then(data => setAvailableFilters(data.filters.map(f => ({ key: f, label: f }))))
        .catch((err) => {
          if (err.name !== 'AbortError') setAvailableFilters([])
        })
      return () => controller.abort()
    }
  }, [showAnnotationFilters])

  const updateFilters = useCallback(() => {
    const newFilters: RecordFilter[] = []
    if (textSearch) {
      newFilters.push({ field: 'text', operator: 'contains', value: textSearch })
    }
    if (sourceFilter) {
      newFilters.push({ field: 'source', operator: 'eq', value: sourceFilter })
    }
    if (stateFilter && showStateFilter) {
      newFilters.push({ field: 'state', operator: 'eq', value: stateFilter })
    }
    if (dateFrom && dateTo) {
      newFilters.push({ field: 'created_at', operator: 'between', value: [dateFrom, dateTo] })
    } else if (dateFrom) {
      newFilters.push({ field: 'created_at', operator: 'gt', value: dateFrom })
    } else if (dateTo) {
      newFilters.push({ field: 'created_at', operator: 'lt', value: dateTo })
    }
    onChange(newFilters)
  }, [textSearch, sourceFilter, stateFilter, dateFrom, dateTo, showStateFilter, onChange])

  useEffect(() => { updateFilters() }, [updateFilters])

  const handleClear = () => {
    setTextSearch('')
    setSourceFilter('')
    setStateFilter('')
    setDateFrom('')
    setDateTo('')
    onChange([])
    onFilterStatesChange({})
  }

  return (
    <div className="bg-white rounded-lg border p-4 space-y-4">
      <div className="flex flex-wrap gap-4 items-end">
        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-muted-foreground">Text Search</label>
          <input
            type="text"
            className="border rounded px-3 py-1.5 text-sm min-w-[200px]"
            placeholder="Search in text..."
            value={textSearch}
            onChange={(e) => setTextSearch(e.target.value)}
          />
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-muted-foreground">Source</label>
          <Select value={sourceFilter} onValueChange={setSourceFilter}>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="All sources" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="">All sources</SelectItem>
              {sources.map(s => (
                <SelectItem key={s} value={s}>{s}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {showStateFilter && (
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-muted-foreground">State</label>
            <Select value={stateFilter} onValueChange={setStateFilter}>
              <SelectTrigger className="w-[120px]">
                <SelectValue placeholder="All states" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="">All states</SelectItem>
                <SelectItem value="pending">Pending</SelectItem>
                <SelectItem value="inflight">In Flight</SelectItem>
                <SelectItem value="completed">Completed</SelectItem>
                <SelectItem value="failed">Failed</SelectItem>
                <SelectItem value="archived">Archived</SelectItem>
              </SelectContent>
            </Select>
          </div>
        )}

        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-muted-foreground">Date From</label>
          <input
            type="date"
            className="border rounded px-3 py-1.5 text-sm"
            value={dateFrom}
            onChange={(e) => setDateFrom(e.target.value)}
          />
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-muted-foreground">Date To</label>
          <input
            type="date"
            className="border rounded px-3 py-1.5 text-sm"
            value={dateTo}
            onChange={(e) => setDateTo(e.target.value)}
          />
        </div>

        <button className="px-4 py-1.5 text-sm border rounded hover:bg-muted" onClick={handleClear}>
          Clear Filters
        </button>
      </div>

      {showAnnotationFilters && availableFilters.length > 0 && (
        <div className="border-t pt-4">
          <label className="text-xs font-medium text-muted-foreground block mb-2">
            Filter Results
          </label>
          <FilterQueryBuilder
            filters={availableFilters}
            value={filterStates}
            onChange={onFilterStatesChange}
          />
        </div>
      )}

      {filters.length > 0 && (
        <div className="flex flex-wrap gap-2 pt-2">
          {filters.map((f, i) => (
            <span
              key={i}
              className="inline-flex items-center gap-1 px-2 py-1 bg-blue-50 text-blue-700 text-xs rounded"
            >
              {f.field}: {Array.isArray(f.value) ? f.value.join(' - ') : f.value}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}
