import { useState, useEffect, useCallback } from 'react'
import { RecordFilter } from '../hooks/useRecords'
import { fetchJson } from '../lib/api'

interface FilterBarProps {
  stage: 'landing' | 'queue' | 'processed'
  filters: RecordFilter[]
  onChange: (filters: RecordFilter[]) => void
}

export const FilterBar = ({ stage, filters, onChange }: FilterBarProps) => {
  const [sources, setSources] = useState<string[]>([])
  const [textSearch, setTextSearch] = useState('')
  const [sourceFilter, setSourceFilter] = useState('')
  const [stateFilter, setStateFilter] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')

  useEffect(() => {
    fetchJson<string[]>('/api/sources')
      .then(data => setSources(data))
      .catch(() => setSources([]))
  }, [])

  const updateFilters = useCallback(() => {
    const newFilters: RecordFilter[] = []
    
    if (textSearch) {
      newFilters.push({ field: 'text', operator: 'contains', value: textSearch })
    }
    
    if (sourceFilter) {
      newFilters.push({ field: 'source', operator: 'eq', value: sourceFilter })
    }
    
    if (stateFilter && stage === 'queue') {
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
  }, [textSearch, sourceFilter, stateFilter, dateFrom, dateTo, stage, onChange])

  useEffect(() => {
    updateFilters()
  }, [updateFilters])

  const handleClear = () => {
    setTextSearch('')
    setSourceFilter('')
    setStateFilter('')
    setDateFrom('')
    setDateTo('')
    onChange([])
  }

  return (
    <div className="bg-white rounded-lg border p-4 space-y-4">
      <div className="flex flex-wrap gap-4 items-end">
        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-gray-500">Text Search</label>
          <input
            type="text"
            className="border rounded px-3 py-1.5 text-sm min-w-[200px]"
            placeholder="Search in text..."
            value={textSearch}
            onChange={(e) => setTextSearch(e.target.value)}
          />
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-gray-500">Source</label>
          <select
            className="border rounded px-3 py-1.5 text-sm"
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value)}
          >
            <option value="">All sources</option>
            {sources.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>

        {stage === 'queue' && (
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-500">State</label>
            <select
              className="border rounded px-3 py-1.5 text-sm"
              value={stateFilter}
              onChange={(e) => setStateFilter(e.target.value)}
            >
              <option value="">All states</option>
              <option value="pending">Pending</option>
              <option value="inflight">In Flight</option>
              <option value="completed">Completed</option>
              <option value="failed">Failed</option>
              <option value="archived">Archived</option>
            </select>
          </div>
        )}

        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-gray-500">Date From</label>
          <input
            type="date"
            className="border rounded px-3 py-1.5 text-sm"
            value={dateFrom}
            onChange={(e) => setDateFrom(e.target.value)}
          />
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs font-medium text-gray-500">Date To</label>
          <input
            type="date"
            className="border rounded px-3 py-1.5 text-sm"
            value={dateTo}
            onChange={(e) => setDateTo(e.target.value)}
          />
        </div>

        <button
          className="px-4 py-1.5 text-sm border rounded hover:bg-gray-50"
          onClick={handleClear}
        >
          Clear Filters
        </button>
      </div>

      {filters.length > 0 && (
        <div className="flex flex-wrap gap-2">
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
