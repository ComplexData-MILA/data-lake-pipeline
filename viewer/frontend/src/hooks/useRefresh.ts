import { useState, useCallback } from 'react'

export function useRefresh(): [boolean, () => void] {
  const [refreshing, setRefreshing] = useState(false)

  const refresh = useCallback(() => {
    setRefreshing(true)
    setTimeout(() => setRefreshing(false), 500)
  }, [])

  return [refreshing, refresh]
}
