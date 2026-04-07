import { useState, useEffect } from "react";
import { fetchSchema, fetchAnnotators } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Filter, Plus, X, Loader2 } from "lucide-react";
import type { FilterOp, FilterSpec } from "@/types";

const OPS: { value: FilterOp; label: string }[] = [
  { value: "eq", label: "Equals" },
  { value: "neq", label: "Not equals" },
  { value: "gt", label: "Greater than" },
  { value: "gte", label: "Greater or equal" },
  { value: "lt", label: "Less than" },
  { value: "lte", label: "Less or equal" },
  { value: "contains", label: "Contains" },
  { value: "startswith", label: "Starts with" },
  { value: "endswith", label: "Ends with" },
];

interface FilterRowProps {
  columns: string[];
  filter: FilterSpec | undefined;
  onChange: (filter: FilterSpec | undefined) => void;
}

function FilterRow({ columns, filter, onChange }: FilterRowProps) {
  return (
    <div className="flex flex-wrap gap-2 items-end">
      <div className="flex-1 min-w-[120px]">
        <Label className="text-xs text-muted-foreground">Field</Label>
        <Select
          value={filter?.field || ""}
          onValueChange={(field) =>
            onChange({ field, op: "eq", value: "" })
          }
        >
          <SelectTrigger>
            <SelectValue placeholder="Select field" />
          </SelectTrigger>
          <SelectContent>
            {columns.map((col) => (
              <SelectItem key={col} value={col}>
                {col}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="w-[130px]">
        <Label className="text-xs text-muted-foreground">Operation</Label>
        <Select
          value={filter?.op || "eq"}
          onValueChange={(op) =>
            onChange({ ...filter!, op: op as FilterOp })
          }
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {OPS.map((op) => (
              <SelectItem key={op.value} value={op.value}>
                {op.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="flex-1 min-w-[120px]">
        <Label className="text-xs text-muted-foreground">Value</Label>
        <Input
          placeholder="Value"
          value={filter?.value?.toString() || ""}
          onChange={(e) =>
            onChange({ ...filter!, value: e.target.value })
          }
        />
      </div>
      <Button
        variant="ghost"
        size="icon"
        onClick={() => onChange(undefined)}
        className="flex-shrink-0"
      >
        <X className="h-4 w-4" />
      </Button>
    </div>
  );
}

export function FilterPanel() {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [schemaColumns, setSchemaColumns] = useState<string[]>([]);
  const [annotators, setAnnotators] = useState<string[]>([]);
  const { dataset, baseColumns, filters, setBaseFilter, setAnnotatorFilter, resetFilters } =
    useViewerStore();

  useEffect(() => {
    if (!dataset) return;
    setLoading(true);
    Promise.all([
      fetchSchema(dataset, []),
      fetchAnnotators(dataset),
    ])
      .then(([schemaRes, annotatorsRes]) => {
        setSchemaColumns(schemaRes.columns.map((c) => c.name));
        setAnnotators(annotatorsRes);
      })
      .catch((err) => console.error("Failed to load filter options:", err))
      .finally(() => setLoading(false));
  }, [dataset]);

  const handleReset = () => {
    resetFilters();
    setOpen(false);
  };

  const availableBaseColumns = baseColumns.length > 0
    ? baseColumns
    : schemaColumns.filter(
        (c) => !annotators.some((a) => c.startsWith(`${a}.`))
      );

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="gap-2">
          <Filter className="h-4 w-4" />
          Filters
          {(filters.base || Object.keys(filters.annotators).length > 0) && (
            <span className="ml-1 bg-primary text-primary-foreground rounded-full px-1.5 py-0.5 text-xs">
              {(filters.base ? 1 : 0) + Object.keys(filters.annotators).length}
            </span>
          )}
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Filter Data</DialogTitle>
          <DialogDescription>
            Add filters for base data or specific annotators.
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-6 w-6 animate-spin" />
          </div>
        ) : (
          <Tabs defaultValue="base" className="w-full">
            <TabsList className="w-full">
              <TabsTrigger value="base" className="flex-1">
                Base Data
              </TabsTrigger>
              {annotators.map((ann) => (
                <TabsTrigger key={ann} value={ann} className="flex-1">
                  {ann}
                </TabsTrigger>
              ))}
            </TabsList>

            <TabsContent value="base" className="space-y-4 mt-4">
              {filters.base ? (
                <FilterRow
                  columns={availableBaseColumns}
                  filter={filters.base}
                  onChange={setBaseFilter}
                />
              ) : (
                <Button
                  variant="outline"
                  onClick={() =>
                    setBaseFilter({ field: availableBaseColumns[0] || "", op: "eq", value: "" })
                  }
                  className="gap-2"
                >
                  <Plus className="h-4 w-4" />
                  Add Filter
                </Button>
              )}
            </TabsContent>

            {annotators.map((ann) => {
              const annColumns = schemaColumns
                .filter((col: string) => col.startsWith(`${ann}.`))
                .map((col: string) => col.replace(`${ann}.`, ""));
              return (
                <TabsContent key={ann} value={ann} className="space-y-4 mt-4">
                  {filters.annotators[ann] ? (
                    <FilterRow
                      columns={annColumns}
                      filter={filters.annotators[ann]}
                      onChange={(f) => setAnnotatorFilter(ann, f)}
                    />
                  ) : (
                    <Button
                      variant="outline"
                      onClick={() =>
                        setAnnotatorFilter(ann, {
                          field: annColumns[0] || "",
                          op: "eq",
                          value: "",
                        })
                      }
                      className="gap-2"
                    >
                      <Plus className="h-4 w-4" />
                      Add Filter
                    </Button>
                  )}
                </TabsContent>
              );
            })}
          </Tabs>
        )}

        <DialogFooter className="gap-2">
          <Button variant="outline" onClick={handleReset}>
            Clear All
          </Button>
          <Button onClick={() => setOpen(false)}>Done</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}