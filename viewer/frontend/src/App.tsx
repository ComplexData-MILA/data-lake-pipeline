import { useState, useEffect } from "react";
import { DatasetSelector } from "@/components/DatasetSelector";
import { DatasetSelectionDialog } from "@/components/DatasetSelectionDialog";
import { ColumnConfigurator } from "@/components/ColumnConfigurator";
import { FilterPanel } from "@/components/FilterPanel";
import { DataTable } from "@/components/DataTable";
import { RowDetailModal } from "@/components/RowDetailModal";
import { useViewerStore } from "@/hooks/useUrlState";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { RefreshCw } from "lucide-react";

export function App() {
  const { dataset, reset } = useViewerStore();
  const [datasetDialogOpen, setDatasetDialogOpen] = useState(false);

  useEffect(() => {
    if (!dataset) {
      setDatasetDialogOpen(true);
    }
  }, [dataset]);

  return (
    <div className="container mx-auto py-8 px-4">
      <h1 className="text-3xl font-bold mb-2">S3 Data Viewer</h1>
      <p className="text-muted-foreground mb-6">Browse and filter your S3-hosted dataset</p>

      {dataset && (
        <Card className="w-full mb-6">
          <CardContent className="pt-6">
            <div className="flex flex-wrap items-center gap-4">
              <DatasetSelector onOpenChange={setDatasetDialogOpen} />
              <ColumnConfigurator />
              <FilterPanel />
              <Button variant="ghost" size="sm" onClick={reset} className="gap-1 ml-auto">
                <RefreshCw className="h-4 w-4" />
                <span className="hidden sm:inline">Reset</span>
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      <main className="w-full">
        <DataTable />
      </main>

      <RowDetailModal />

      <DatasetSelectionDialog
        open={datasetDialogOpen}
        onOpenChange={setDatasetDialogOpen}
      />
    </div>
  );
}
