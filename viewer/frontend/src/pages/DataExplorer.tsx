import { QueryEditor } from '../components/QueryEditor'

export const DataExplorer = () => {
  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Data Explorer</h1>
      </div>

      <div className="bg-gray-50 p-4 rounded-lg text-sm text-gray-600">
        <p className="font-medium mb-2">Query the data lake using SQL:</p>
        <ul className="list-disc list-inside space-y-1">
          <li><code className="bg-gray-200 px-1 rounded">lake.processed_posts</code> - Processed social posts</li>
          <li><code className="bg-gray-200 px-1 rounded">lake.annotations</code> - Post annotations</li>
          <li><code className="bg-gray-200 px-1 rounded">lake.metrics</code> - Pipeline metrics</li>
        </ul>
      </div>

      <QueryEditor />
    </div>
  )
}
