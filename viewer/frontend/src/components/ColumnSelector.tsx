import { useEffect, useState } from "react";
import { fetchSchema } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Settings2 } from "lucide-react";

export function ColumnSelector() {
  const [columns, setColumnsList] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [open, setOpen] = useState(false);
  const { dataset, annotators, columns: selected, setColumns } = useViewerStore();

  useEffect(() => {
    if (!dataset) {
      setColumnsList([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    fetchSchema(dataset, annotators)
      .then((res) => setColumnsList(res.columns.map((c) => c.name)))
      .catch((err) => console.error("Failed to load schema:", err))
      .finally(() => setLoading(false));
  }, [dataset, annotators]);

  const handleToggle = (col: string, checked: boolean) => {
    if (checked) {
      setColumns([...selected, col]);
    } else {
      setColumns(selected.filter((c) => c !== col));
    }
  };

  const handleSelectAll = () => {
    setColumns([...columns]);
  };

  const handleSelectNone = () => {
    setColumns([]);
  };

  if (loading || columns.length === 0) {
    return null;
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="gap-2">
          <Settings2 className="h-4 w-4" />
          Columns ({selected.length || "All"})
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[280px]" align="end">
        <div className="flex gap-2 mb-3">
          <Button variant="outline" size="sm" onClick={handleSelectAll}>
            All
          </Button>
          <Button variant="outline" size="sm" onClick={handleSelectNone}>
            None
          </Button>
        </div>
        <div className="max-h-[300px] overflow-y-auto space-y-2">
          {columns.map((col) => (
            <div key={col} className="flex items-center space-x-2">
              <Checkbox
                id={`col-${col}`}
                checked={selected.length === 0 || selected.includes(col)}
                onCheckedChange={(checked) =>
                  handleToggle(col, checked as boolean)
                }
              />
              <Label
                htmlFor={`col-${col}`}
                className="text-sm font-normal cursor-pointer truncate"
              >
                {col}
              </Label>
            </div>
          ))}
        </div>
      </PopoverContent>
    </Popover>
  );
}