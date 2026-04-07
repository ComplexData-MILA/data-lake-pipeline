import { useEffect, useState } from "react";
import { fetchSchema, fetchAnnotators } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
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
import { Settings2 } from "lucide-react";

export function ColumnSelector() {
  const [baseColumnsList, setBaseColumnsList] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const [tempSelection, setTempSelection] = useState<string[]>([]);
  const { dataset, baseColumns, setBaseColumns } = useViewerStore();

  useEffect(() => {
    if (!dataset) {
      setBaseColumnsList([]);
      return;
    }
    setLoading(true);
    Promise.all([fetchSchema(dataset, []), fetchAnnotators(dataset)])
      .then(([schemaRes, annotators]) => {
        const annotatorPrefixes = annotators.map((a) => `${a}.`);
        const baseCols = schemaRes.columns
          .map((c) => c.name)
          .filter((name) => !annotatorPrefixes.some((prefix) => name.startsWith(prefix)));
        setBaseColumnsList(baseCols);
      })
      .catch((err) => console.error("Failed to load schema:", err))
      .finally(() => setLoading(false));
  }, [dataset]);

  useEffect(() => {
    if (open) {
      setTempSelection(baseColumns.length > 0 ? baseColumns : baseColumnsList);
    }
  }, [open, baseColumns, baseColumnsList]);

  const handleToggle = (col: string, checked: boolean) => {
    if (checked) {
      setTempSelection((prev) => [...prev, col]);
    } else {
      setTempSelection((prev) => prev.filter((c) => c !== col));
    }
  };

  const handleSelectAll = () => {
    setTempSelection([...baseColumnsList]);
  };

  const handleSelectNone = () => {
    setTempSelection([]);
  };

  const handleSave = () => {
    setBaseColumns(tempSelection);
    setOpen(false);
  };

  if (!dataset) {
    return null;
  }

  const selectedCount = baseColumns.length > 0 ? baseColumns.length : baseColumnsList.length;

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="gap-2">
          <Settings2 className="h-4 w-4" />
          Columns ({selectedCount})
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Configure Base Columns</DialogTitle>
          <DialogDescription>
            Select which base dataset columns to display
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
          ) : baseColumnsList.length === 0 ? (
            <div className="p-8 text-center text-muted-foreground">
              No base columns available
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
                {baseColumnsList.map((col) => (
                  <TableRow key={col}>
                    <TableCell>
                      <Checkbox
                        id={`col-${col}`}
                        checked={tempSelection.includes(col)}
                        onCheckedChange={(checked) =>
                          handleToggle(col, checked as boolean)
                        }
                      />
                    </TableCell>
                    <TableCell>
                      <Label
                        htmlFor={`col-${col}`}
                        className="cursor-pointer"
                      >
                        {col}
                      </Label>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </div>

        <div className="pt-2 text-sm text-muted-foreground">
          {tempSelection.length} of {baseColumnsList.length} columns selected
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={loading}>
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
