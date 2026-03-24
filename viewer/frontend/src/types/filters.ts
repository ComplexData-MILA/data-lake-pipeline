export type TriStateValue = boolean | null

export type FilterStates = Record<string, TriStateValue>

export interface NamedFilter {
  key: string
  label: string
  description?: string
}
