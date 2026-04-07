import { useEffect, useState } from "react";
import { fetchDatasets } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Database, Search, FolderOpen } from "lucide-react";

interface DatasetSelectionDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function DatasetSelectionDialog({ open, onOpenChange }: DatasetSelectionDialogProps) {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const { setDataset } = useViewerStore();

  useEffect(() => {
    if (open) {
      setLoading(true);
      fetchDatasets()
        .then(setDatasets)
        .catch((err) => console.error("Failed to load datasets:", err))
        .finally(() => setLoading(false));
    }
  }, [open]);

  const filteredDatasets = datasets.filter((d) =>
    d.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const handleSelect = (dataset: string) => {
    setDataset(dataset);
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-hidden flex flex-col p-0">
        <DialogHeader className="px-6 pt-6 pb-4 border-b">
          <DialogTitle className="flex items-center gap-2 text-2xl">
            <Database className="h-6 w-6" />
            Select Dataset
          </DialogTitle>
          <DialogDescription>
            Choose a dataset to view and analyze
          </DialogDescription>
        </DialogHeader>

        <div className="px-6 py-4 border-b">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search datasets..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9"
            />
          </div>
        </div>

        <div className="flex-1 overflow-auto px-6 py-4">
          {loading ? (
            <div className="space-y-3">
              {Array.from({ length: 8 }).map((_, i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : filteredDatasets.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground">
              {searchQuery ? (
                <>
                  <p className="text-lg font-medium">No datasets found</p>
                  <p className="text-sm">Try adjusting your search query</p>
                </>
              ) : (
                <>
                  <FolderOpen className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p className="text-lg font-medium">No datasets available</p>
                </>
              )}
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[50px]"></TableHead>
                  <TableHead>Dataset Name</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredDatasets.map((dataset) => (
                  <TableRow
                    key={dataset}
                    className="cursor-pointer hover:bg-muted/50"
                    onClick={() => handleSelect(dataset)}
                  >
                    <TableCell>
                      <Database className="h-4 w-4 text-muted-foreground" />
                    </TableCell>
                    <TableCell className="font-medium">{dataset}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </div>

        <div className="px-6 py-4 border-t flex justify-between items-center">
          <div className="text-sm text-muted-foreground">
            {loading ? (
              <Skeleton className="h-4 w-24" />
            ) : (
              `${filteredDatasets.length} dataset${filteredDatasets.length !== 1 ? "s" : ""} available`
            )}
          </div>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
