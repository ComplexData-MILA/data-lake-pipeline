import { useQuery } from '@tanstack/react-query'
import { fetchJson } from '../lib/api'

export function useApi<T>(path: string, refresh?: boolean) {
  const { data, isLoading, error, refetch } = useQuery<T>({
    queryKey: [path, refresh],
    queryFn: () => fetchJson<T>(path)
  })

  return {
    data: data ?? null,
    loading: isLoading,
    error: error as Error | null,
    refetch
  }
}
