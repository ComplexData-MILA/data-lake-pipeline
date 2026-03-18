import { useState, useCallback } from 'react'
import { postJson } from '../lib/api'

export interface RecordFilter {
  field: string
  operator: 'eq' | 'contains' | 'gt' | 'lt' | 'between'
  value: string | string[]
}

export interface RecordQuery {
  stage: 'landing' | 'queue' | 'processed'
  page: number
  page_size: number
  filters: RecordFilter[]
  sort_by?: string
  sort_desc?: boolean
}

export interface RecordResponse {
  records: Record<string, unknown>[]
  total_count: number
  page: number
  page_size: number
  total_pages: number
  columns: string[]
}

export function useRecords(stage: string) {
  const [data, setData] = useState<RecordResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchRecords = useCallback(async (
    page: number,
    pageSize: number,
    filters: RecordFilter[],
    sortBy?: string,
    sortDesc?: boolean
  ) => {
    setLoading(true)
    setError(null)

    try {
      const query: RecordQuery = {
        stage: stage as 'landing' | 'queue' | 'processed',
        page,
        page_size: pageSize,
        filters,
        sort_by: sortBy,
        sort_desc: sortDesc,
      }
      const response = await postJson<RecordResponse>('/api/records', query)
      setData(response)
      return response
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to fetch records'
      setError(message)
      return null
    } finally {
      setLoading(false)
    }
  }, [stage])

  return { data, loading, error, fetchRecords }
}
