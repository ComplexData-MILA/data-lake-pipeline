import { useState, useEffect } from 'react'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../components/ui/table'
import { Badge } from '../components/ui/badge'
import { fetchJson } from '../lib/api'

interface BatchFilterInfo {
  batch_id: string
  pipeline_stage: string
  state: string
  completed_filters: string[]
  has_merged: boolean
}

export const BatchFilterStatus = () => {
  const [batches, setBatches] = useState<BatchFilterInfo[]>([])
  const [availableFilters, setAvailableFilters] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    Promise.all([
      fetchJson<BatchFilterInfo[]>('/api/batch-filters'),
      fetchJson<{ filters: string[] }>('/api/filters')
    ])
      .then(([batchData, filterData]) => {
        setBatches(batchData)
        setAvailableFilters(filterData.filters)
      })
      .catch(err => setError(err instanceof Error ? err.message : 'Failed to load'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return <div className="p-8 text-center text-muted-foreground">Loading...</div>
  }

  if (error) {
    return (
      <div className="p-8 text-center text-destructive">
        Error: {error}
        <button
          onClick={() => window.location.reload()}
          className="ml-4 px-4 py-2 border rounded"
        >
          Retry
        </button>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Filter Completion Status</h1>
        <Badge variant="outline">{batches.length} batches</Badge>
      </div>

      <div className="rounded-lg border bg-white overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Batch ID</TableHead>
              <TableHead>Stage</TableHead>
              <TableHead>State</TableHead>
              {availableFilters.map(f => (
                <TableHead key={f} className="text-center">{f}</TableHead>
              ))}
              <TableHead className="text-center">Merged</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {batches.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4 + availableFilters.length} className="text-center text-muted-foreground">
                  No batches found
                </TableCell>
              </TableRow>
            ) : (
              batches.map(batch => (
                <TableRow key={batch.batch_id}>
                  <TableCell className="font-mono text-sm">{batch.batch_id}</TableCell>
                  <TableCell>{batch.pipeline_stage}</TableCell>
                  <TableCell>
                    <Badge variant={
                      batch.state === 'completed' ? 'default' :
                      batch.state === 'failed' ? 'destructive' :
                      'secondary'
                    }>
                      {batch.state}
                    </Badge>
                  </TableCell>
                  {availableFilters.map(f => (
                    <TableCell key={f} className="text-center">
                      {batch.completed_filters.includes(f) ? (
                        <span className="text-green-600">✓</span>
                      ) : (
                        <span className="text-muted-foreground/30">—</span>
                      )}
                    </TableCell>
                  ))}
                  <TableCell className="text-center">
                    {batch.has_merged ? (
                      <span className="text-green-600">✓</span>
                    ) : (
                      <span className="text-muted-foreground/30">—</span>
                    )}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}
