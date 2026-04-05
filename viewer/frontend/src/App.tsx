import { DatasetSelector } from "@/components/DatasetSelector";
import { AnnotatorSelector } from "@/components/AnnotatorSelector";
import { ColumnSelector } from "@/components/ColumnSelector";
import { FilterPanel } from "@/components/FilterPanel";
import { DataTable } from "@/components/DataTable";
import { RowDetailModal } from "@/components/RowDetailModal";
import { useViewerStore } from "@/hooks/useUrlState";
import { Button } from "@/components/ui/button";
import { Database, RefreshCw } from "lucide-react";

export function App() {
  const { dataset, reset } = useViewerStore();

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b bg-card">
        <div className="container mx-auto px-4 py-4">
          <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
            <div className="flex items-center gap-2">
              <Database className="h-6 w-6" />
              <h1 className="text-xl font-bold">S3 Data Viewer</h1>
            </div>
            <DatasetSelector />
          </div>
          
          {dataset && (
            <div className="mt-4 flex flex-wrap items-center gap-4">
              <div className="flex flex-col gap-2">
                <span className="text-xs text-muted-foreground">Annotators</span>
                <AnnotatorSelector />
              </div>
              <div className="flex items-center gap-2 ml-auto">
                <ColumnSelector />
                <FilterPanel />
                <Button variant="ghost" size="sm" onClick={reset} className="gap-1">
                  <RefreshCw className="h-4 w-4" />
                  <span className="hidden sm:inline">Reset</span>
                </Button>
              </div>
            </div>
          )}
        </div>
      </header>

      <main className="container mx-auto px-4 py-6">
        <DataTable />
      </main>

      <RowDetailModal />
    </div>
  );
}