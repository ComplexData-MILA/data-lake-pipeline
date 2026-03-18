import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from './ui/dialog'
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
      <dt className="font-medium text-gray-500 text-sm">{label}</dt>
      <dd className="col-span-2 text-sm break-all">{value}</dd>
    </div>
  )
}

export function RecordDetailDialog({ record, open, onOpenChange }: RecordDetailDialogProps) {
  if (!record) return null

  const textFields = ['text', 'raw_text']
  const metadataFields = ['metadata']
  const specialFields = [...textFields, ...metadataFields]
  const regularEntries = Object.entries(record).filter(([key]) => !specialFields.includes(key))
  const textEntries = Object.entries(record).filter(([key]) => textFields.includes(key))
  const metadataEntries = Object.entries(record).filter(([key]) => metadataFields.includes(key))

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
                  <dt className="font-medium text-gray-500 text-sm mb-2">{key}</dt>
                  <dd className="text-sm whitespace-pre-wrap bg-gray-50 p-3 rounded-md max-h-64 overflow-auto">
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
                  <dt className="font-medium text-gray-500 text-sm mb-2">{key}</dt>
                  <dd className="text-sm max-h-64 overflow-auto rounded-md">
                    <SyntaxHighlighter language="json" style={vs}>
                      {formatValue(value)}
                    </SyntaxHighlighter>
                  </dd>
                </div>
              ))}
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
