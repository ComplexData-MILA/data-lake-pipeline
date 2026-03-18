interface FileItem {
  key: string
  source: string
  size_bytes: number
  age_seconds: number
  last_modified: string
}

interface FileBrowserProps {
  files: FileItem[]
  onSelect?: (file: FileItem) => void
}

const formatBytes = (bytes: number) => {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

const formatAge = (seconds: number) => {
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}

export const FileBrowser = ({ files, onSelect }: FileBrowserProps) => {
  return (
    <div className="bg-white rounded-lg border overflow-hidden">
      <table className="min-w-full">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-4 py-2 text-left text-sm font-medium text-gray-500">File</th>
            <th className="px-4 py-2 text-left text-sm font-medium text-gray-500">Source</th>
            <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">Size</th>
            <th className="px-4 py-2 text-right text-sm font-medium text-gray-500">Age</th>
            <th className="px-4 py-2 text-left text-sm font-medium text-gray-500">Modified</th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {files.map((file) => (
            <tr
              key={file.key}
              className={`hover:bg-gray-50 ${onSelect ? 'cursor-pointer' : ''}`}
              onClick={() => onSelect?.(file)}
            >
              <td className="px-4 py-2 text-sm font-mono truncate max-w-xs">{file.key}</td>
              <td className="px-4 py-2 text-sm">{file.source}</td>
              <td className="px-4 py-2 text-sm text-right">{formatBytes(file.size_bytes)}</td>
              <td className="px-4 py-2 text-sm text-right">{formatAge(file.age_seconds)}</td>
              <td className="px-4 py-2 text-sm">
                {new Date(file.last_modified).toLocaleString()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {files.length === 0 && (
        <div className="p-8 text-center text-gray-500">No files found</div>
      )}
    </div>
  )
}
