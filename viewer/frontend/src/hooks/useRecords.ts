import { useState, useCallback } from 'react'
import { postJson } from '../lib/api'
import { FilterStates } from '../types/filters'

export interface RecordFilter {
  field: string
  operator: 'eq' | 'contains' | 'gt' | 'lt' | 'between'
  value: string | string[]
}

export interface RecordQuery {
  page: number
  page_size: number
  filters: RecordFilter[]
  sort_by?: string
  sort_desc?: boolean
  filter_states?: FilterStates
}

export interface RecordResponse {
  records: Record<string, unknown>[]
  total_count: number
  page: number
  page_size: number
  total_pages: number
  columns: string[]
  warning?: string
}

export function useRecords() {
  const [data, setData] = useState<RecordResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [warning, setWarning] = useState<string | null>(null)

  const fetchRecords = useCallback(async (
    page: number,
    pageSize: number,
    filters: RecordFilter[],
    sortBy?: string,
    sortDesc?: boolean,
    filterStates?: FilterStates
  ) => {
    setLoading(true)
    setError(null)
    setWarning(null)

    try {
      const query: RecordQuery = {
        page,
        page_size: pageSize,
        filters,
        sort_by: sortBy,
        sort_desc: sortDesc,
        filter_states: filterStates,
      }
      const response = await postJson<RecordResponse>('/api/records', query)
      setData(response)
      if (response.warning) {
        setWarning(response.warning)
      }
      return response
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to fetch records'
      setError(message)
      return null
    } finally {
      setLoading(false)
    }
  }, [])

  return { data, loading, error, warning, fetchRecords }
}
