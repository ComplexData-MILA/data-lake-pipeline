import { useState, useCallback, useMemo } from 'react'
import type { TriStateValue, NamedFilter } from '../types/filters'

export type { TriStateValue, NamedFilter }

export interface FilterQueryBuilderProps {
  filters: NamedFilter[]
  value?: Record<string, TriStateValue>
  onChange: (query: Record<string, TriStateValue>) => void
  className?: string
}

type SelectionState = 'any' | 'pass' | 'fail'

const stateToTriState: Record<SelectionState, TriStateValue> = {
  any: null,
  pass: true,
  fail: false,
}

const triStateToState: Record<string, SelectionState> = {
  'null': 'any',
  'true': 'pass',
  'false': 'fail',
}

export function FilterQueryBuilder({
  filters,
  value,
  onChange,
  className = '',
}: FilterQueryBuilderProps) {
  const internalState = useMemo(() => {
    if (value) return value
    return filters.reduce((acc, f) => ({ ...acc, [f.key]: null }), {} as Record<string, TriStateValue>)
  }, [value, filters])

  const getState = useCallback((key: string): SelectionState => {
    return triStateToState[String(internalState[key])] || 'any'
  }, [internalState])

  const setState = useCallback((key: string, state: SelectionState) => {
    onChange({
      ...internalState,
      [key]: stateToTriState[state],
    })
  }, [internalState, onChange])

  const clearAll = useCallback(() => {
    const cleared = filters.reduce((acc, f) => ({ ...acc, [f.key]: null }), {} as Record<string, TriStateValue>)
    onChange(cleared)
  }, [filters, onChange])

  const activeCount = useMemo(() => {
    return Object.values(internalState).filter(v => v !== null).length
  }, [internalState])

  const buttonBase = 'px-3 py-1 text-sm border transition-colors first:rounded-l-md last:rounded-r-md'
  const buttonInactive = 'bg-white hover:bg-gray-50 border-gray-300 text-gray-600'
  const buttonActive = 'border-blue-500 text-white font-medium'
  const buttonAnyActive = 'bg-gray-100 border-gray-400 text-gray-700 font-medium'

  return (
    <div className={`bg-white rounded-lg border p-4 ${className}`}>
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-gray-700">Filter Conditions</h3>
        {activeCount > 0 && (
          <button
            className="text-xs text-gray-500 hover:text-gray-700"
            onClick={clearAll}
          >
            Clear all ({activeCount})
          </button>
        )}
      </div>

      <div className="space-y-3">
        {filters.map((filter) => {
          const currentState = getState(filter.key)
          return (
            <div key={filter.key} className="flex items-center justify-between py-2">
              <div className="flex flex-col">
                <span className="text-sm font-medium text-gray-800">{filter.label}</span>
                {filter.description && (
                  <span className="text-xs text-gray-500">{filter.description}</span>
                )}
              </div>
              <div className="flex">
                <button
                  className={`${buttonBase} ${currentState === 'any' ? buttonAnyActive : buttonInactive}`}
                  onClick={() => setState(filter.key, 'any')}
                >
                  Any
                </button>
                <button
                  className={`${buttonBase} -ml-px ${currentState === 'pass' ? 'bg-green-500 ' + buttonActive : buttonInactive}`}
                  onClick={() => setState(filter.key, 'pass')}
                >
                  Pass
                </button>
                <button
                  className={`${buttonBase} -ml-px ${currentState === 'fail' ? 'bg-red-500 ' + buttonActive : buttonInactive}`}
                  onClick={() => setState(filter.key, 'fail')}
                >
                  Fail
                </button>
              </div>
            </div>
          )
        })}
      </div>

      {activeCount > 0 && (
        <div className="mt-4 pt-4 border-t">
          <div className="text-xs text-gray-500 mb-2">Active filters (AND logic):</div>
          <div className="flex flex-wrap gap-2">
            {filters.map((filter) => {
              const state = getState(filter.key)
              if (state === 'any') return null
              return (
                <span
                  key={filter.key}
                  className={`inline-flex items-center gap-1 px-2 py-1 text-xs rounded ${
                    state === 'pass' ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'
                  }`}
                >
                  {filter.label} = {state === 'pass' ? 'true' : 'false'}
                </span>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

export function useFilterQuery(initialFilters: NamedFilter[], initialValue?: Record<string, TriStateValue>) {
  const [query, setQuery] = useState<Record<string, TriStateValue>>(
    initialValue ?? initialFilters.reduce((acc, f) => ({ ...acc, [f.key]: null }), {})
  )

  const handleChange = useCallback((newQuery: Record<string, TriStateValue>) => {
    setQuery(newQuery)
  }, [])

  const activeFilters = useMemo(() => {
    return Object.entries(query)
      .filter(([, v]) => v !== null)
      .reduce((acc, [k, v]) => ({ ...acc, [k]: v as boolean }), {} as Record<string, boolean>)
  }, [query])

  return { query, setQuery, handleChange, activeFilters }
}
