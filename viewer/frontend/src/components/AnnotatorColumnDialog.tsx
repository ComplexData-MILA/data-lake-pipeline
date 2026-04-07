import { useEffect, useState } from "react";
import { fetchAnnotatorColumns } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Loader2 } from "lucide-react";

interface AnnotatorColumnDialogProps {
  annotator: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function AnnotatorColumnDialog({
  annotator,
  open,
  onOpenChange,
}: AnnotatorColumnDialogProps) {
  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const { dataset, annotatorColumns, setAnnotatorColumns } = useViewerStore();

  useEffect(() => {
    if (open && annotator && dataset) {
      setLoading(true);
      setSelectedColumns(annotatorColumns[annotator] || []);
      fetchAnnotatorColumns(dataset, annotator)
        .then((cols) => {
          setAvailableColumns(cols);
          const hasSelection = annotatorColumns[annotator] && annotatorColumns[annotator].length > 0;
          setSelectedColumns(hasSelection ? annotatorColumns[annotator] : cols);
        })
        .catch((err) => console.error("Failed to load annotator columns:", err))
        .finally(() => setLoading(false));
    }
  }, [open, annotator, dataset, annotatorColumns]);

  const handleToggle = (column: string, checked: boolean) => {
    if (checked) {
      setSelectedColumns((prev) => [...prev, column]);
    } else {
      setSelectedColumns((prev) => prev.filter((c) => c !== column));
    }
  };

  const handleSelectAll = () => {
    setSelectedColumns([...availableColumns]);
  };

  const handleSelectNone = () => {
    setSelectedColumns([]);
  };

  const handleSave = () => {
    if (annotator) {
      setAnnotatorColumns({ ...annotatorColumns, [annotator]: selectedColumns });
    }
    onOpenChange(false);
  };

  const handleCancel = () => {
    onOpenChange(false);
  };

  if (!annotator) return null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Configure Columns for {annotator}</DialogTitle>
          <DialogDescription>
            Select which columns to display from this annotator
          </DialogDescription>
        </DialogHeader>

        <div className="flex gap-2 py-2">
          <Button variant="outline" size="sm" onClick={handleSelectAll}>
            Select All
          </Button>
          <Button variant="outline" size="sm" onClick={handleSelectNone}>
            Select None
          </Button>
        </div>

        <div className="flex-1 overflow-auto border rounded-md">
          {loading ? (
            <div className="p-4 space-y-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          ) : availableColumns.length === 0 ? (
            <div className="p-8 text-center text-muted-foreground">
              No columns available for this annotator
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[50px]"></TableHead>
                  <TableHead>Column Name</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {availableColumns.map((column) => (
                  <TableRow key={column}>
                    <TableCell>
                      <Checkbox
                        checked={selectedColumns.includes(column)}
                        onCheckedChange={(checked) =>
                          handleToggle(column, checked as boolean)
                        }
                      />
                    </TableCell>
                    <TableCell>
                      <Label
                        htmlFor={`col-${column}`}
                        className="cursor-pointer"
                      >
                        {column}
                      </Label>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </div>

        <div className="pt-2 text-sm text-muted-foreground">
          {selectedColumns.length} of {availableColumns.length} columns selected
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Loading...
              </>
            ) : (
              "Save"
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
