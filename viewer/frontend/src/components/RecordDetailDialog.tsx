import { useMemo } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from './ui/dialog'
import { Badge } from './ui/badge'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { vs } from 'react-syntax-highlighter/dist/esm/styles/prism'

interface RecordDetailDialogProps {
  record: Record<string, unknown> | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

function FieldRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="grid grid-cols-3 gap-2 py-2 border-b last:border-b-0">
      <dt className="font-medium text-muted-foreground text-sm">{label}</dt>
      <dd className="col-span-2 text-sm break-all">{value}</dd>
    </div>
  )
}

export function RecordDetailDialog({ record, open, onOpenChange }: RecordDetailDialogProps) {
  if (!record) return null

  const textFields = ['text', 'raw_text']
  const metadataFields = ['metadata']
  const specialFields = [...textFields, ...metadataFields]
  
  const regularEntries = Object.entries(record).filter(([key]) => !specialFields.includes(key) && !key.endsWith('_passed') && !key.endsWith('_score') && !key.endsWith('_reason'))
  const textEntries = Object.entries(record).filter(([key]) => textFields.includes(key))
  const metadataEntries = Object.entries(record).filter(([key]) => metadataFields.includes(key))
  
  const filterResults = useMemo(() => Object.entries(record)
    .filter(([key]) => key.endsWith('_passed') || key.endsWith('_score') || key.endsWith('_reason'))
    .reduce((acc, [key, value]) => {
      const match = key.match(/^(.+)_(passed|score|reason)$/)
      if (match) {
        const [, name, field] = match
        acc[name] = acc[name] || {}
        acc[name][field] = value
      }
      return acc
    }, {} as Record<string, Record<string, unknown>>),
    [record]
  )

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Record Details</DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-auto">
          <dl className="divide-y">
            {regularEntries.map(([key, value]) => (
              <FieldRow key={key} label={key} value={formatValue(value)} />
            ))}
          </dl>

          {textEntries.length > 0 && (
            <div className="py-2 border-t mt-2">
              {textEntries.map(([key, value]) => (
                <div key={key} className="mb-4 last:mb-0">
                  <dt className="font-medium text-muted-foreground text-sm mb-2">{key}</dt>
                  <dd className="text-sm whitespace-pre-wrap bg-muted/50 p-3 rounded-md max-h-64 overflow-auto">
                    {formatValue(value)}
                  </dd>
                </div>
              ))}
            </div>
          )}

          {metadataEntries.length > 0 && (
            <div className="py-2 border-t mt-2">
              {metadataEntries.map(([key, value]) => (
                <div key={key} className="mb-4 last:mb-0">
                  <dt className="font-medium text-muted-foreground text-sm mb-2">{key}</dt>
                  <dd className="text-sm max-h-64 overflow-auto rounded-md">
                    <SyntaxHighlighter language="json" style={vs}>
                      {formatValue(value)}
                    </SyntaxHighlighter>
                  </dd>
                </div>
              ))}
            </div>
          )}

          {Object.keys(filterResults).length > 0 && (
            <div className="py-2 border-t mt-2">
              <h3 className="font-medium text-muted-foreground text-sm mb-2">Filter Results</h3>
              <div className="space-y-2">
                {Object.entries(filterResults).map(([name, data]) => {
                  const passed = Boolean(data.passed)
                  const score = data.score != null ? String(data.score) : null
                  const reason = data.reason ? String(data.reason) : null
                  return (
                    <div key={name} className="flex items-center gap-2 p-2 bg-muted/50 rounded">
                      <span className="font-medium text-sm">{name}</span>
                      <Badge variant={passed ? 'default' : 'destructive'}>
                        {passed ? 'PASS' : 'FAIL'}
                      </Badge>
                      {score && (
                        <span className="text-xs text-muted-foreground">score: {score}</span>
                      )}
                      {reason && (
                        <span className="text-xs text-muted-foreground truncate max-w-[200px]">{reason}</span>
                      )}
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'object') {
    return JSON.stringify(value, null, 2)
  }
  return String(value)
}
