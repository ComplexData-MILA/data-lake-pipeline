import { DataTable } from '../components/DataTable'
import { FilterBar } from '../components/FilterBar'
import { Alert, AlertDescription, AlertTitle } from '../components/ui/alert'
import { AlertTriangle } from 'lucide-react'
import { usePageFilters } from '../hooks/usePageFilters'

export const DataExplorer = () => {
  const { filters, filterStates, warning, setFilters, setFilterStates, setWarning } = usePageFilters()

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Data Explorer</h1>
      </div>

      <div className="bg-muted/50 p-4 rounded-lg text-sm text-muted-foreground">
        <p>Query processed and annotated data from the data lake. Use filters to find specific content.</p>
      </div>

      <FilterBar
        filters={filters}
        onChange={setFilters}
        filterStates={filterStates}
        onFilterStatesChange={setFilterStates}
        showAnnotationFilters={true}
      />

      {warning && (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Incomplete Data</AlertTitle>
          <AlertDescription>{warning}</AlertDescription>
        </Alert>
      )}

      <DataTable
        filters={filters}
        filterStates={filterStates}
        onWarning={setWarning}
      />
    </div>
  )
}
