import { useEffect, useState, useCallback, useRef } from "react";
import { fetchSchema, fetchAnnotators, fetchAnnotatorColumns } from "@/lib/api";
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
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Settings2, ChevronDown, ChevronRight } from "lucide-react";

interface AnnotatorSection {
  name: string;
  columns: string[];
  selected: string[];
  loading: boolean;
  expanded: boolean;
  loaded: boolean;
}

export function ColumnConfigurator() {
  const {
    dataset,
    baseColumns,
    setBaseColumns,
    annotatorColumns,
    setAnnotatorColumns,
    setAnnotators,
  } = useViewerStore();

  const [open, setOpen] = useState(false);
  const [baseColumnsList, setBaseColumnsList] = useState<string[]>([]);
  const [baseColumnsLoading, setBaseColumnsLoading] = useState(false);
  const [tempBaseColumns, setTempBaseColumns] = useState<string[]>([]);

  const [annotatorsList, setAnnotatorsList] = useState<string[]>([]);
  const [annotatorSections, setAnnotatorSections] = useState<
    Record<string, AnnotatorSection>
  >({});

  const cachedDatasetRef = useRef<string | null>(null);
  const loadedAnnotatorsRef = useRef<Set<string>>(new Set());

  const totalSelected =
    tempBaseColumns.length +
    Object.values(annotatorSections).reduce(
      (acc, section) => acc + section.selected.length,
      0,
    );

  useEffect(() => {
    if (!open || !dataset) return;

    const isNewDataset = cachedDatasetRef.current !== dataset;

    if (isNewDataset) {
      cachedDatasetRef.current = dataset;
      loadedAnnotatorsRef.current = new Set();
      setBaseColumnsList([]);
      setAnnotatorsList([]);
      setAnnotatorSections({});
    }

    if (baseColumnsList.length === 0 || isNewDataset) {
      setBaseColumnsLoading(true);
      Promise.all([fetchSchema(dataset, []), fetchAnnotators(dataset)])
        .then(([schemaRes, annotators]) => {
          const annotatorPrefixes = annotators.map((a) => `${a}.`);
          const baseCols = schemaRes.columns
            .map((c) => c.name)
            .filter(
              (name) =>
                !annotatorPrefixes.some((prefix) => name.startsWith(prefix)),
            );

          setBaseColumnsList(baseCols);
          setAnnotatorsList(annotators);
          setAnnotators(annotators);
          setTempBaseColumns(baseColumns.length > 0 ? baseColumns : baseCols);

          const sections: Record<string, AnnotatorSection> = {};
          for (const annotator of annotators) {
            const hasExistingSelection =
              annotatorColumns[annotator] &&
              annotatorColumns[annotator].length > 0;
            sections[annotator] = {
              name: annotator,
              columns: [],
              selected: hasExistingSelection ? annotatorColumns[annotator] : [],
              loading: false,
              expanded: false,
              loaded: false,
            };
          }
          setAnnotatorSections(sections);
        })
        .catch((err) => console.error("Failed to load columns config:", err))
        .finally(() => setBaseColumnsLoading(false));
    } else {
      setTempBaseColumns(
        baseColumns.length > 0 ? baseColumns : baseColumnsList,
      );

      setAnnotatorSections((prev) => {
        const updated: Record<string, AnnotatorSection> = {};
        for (const annotator of annotatorsList) {
          const hasExistingSelection =
            annotatorColumns[annotator] &&
            annotatorColumns[annotator].length > 0;
          const existing = prev[annotator];
          updated[annotator] = existing
            ? {
                ...existing,
                selected: hasExistingSelection
                  ? annotatorColumns[annotator]
                  : existing.selected,
              }
            : {
                name: annotator,
                columns: [],
                selected: hasExistingSelection
                  ? annotatorColumns[annotator]
                  : [],
                loading: false,
                expanded: false,
                loaded: false,
              };
        }
        return updated;
      });
    }
  }, [
    open,
    dataset,
    baseColumns,
    annotatorColumns,
    baseColumnsList.length,
    annotatorsList,
  ]);

  const handleExpandAnnotator = useCallback(
    async (annotator: string) => {
      if (!dataset) return;

      const alreadyLoaded = loadedAnnotatorsRef.current.has(annotator);

      setAnnotatorSections((prev) => ({
        ...prev,
        [annotator]: { ...prev[annotator], expanded: true },
      }));

      if (alreadyLoaded) return;

      setAnnotatorSections((prev) => ({
        ...prev,
        [annotator]: { ...prev[annotator], loading: true, expanded: true },
      }));

      try {
        const columns = await fetchAnnotatorColumns(dataset, annotator);
        loadedAnnotatorsRef.current.add(annotator);
        setAnnotatorSections((prev) => {
          const section = prev[annotator];
          // Preserve existing selections that are valid for the loaded columns
          const validSelections = section.selected.filter((col) =>
            columns.includes(col),
          );
          return {
            ...prev,
            [annotator]: {
              ...section,
              columns,
              selected: validSelections,
              loading: false,
              loaded: true,
            },
          };
        });
      } catch (err) {
        console.error(`Failed to load columns for ${annotator}:`, err);
        setAnnotatorSections((prev) => ({
          ...prev,
          [annotator]: { ...prev[annotator], loading: false },
        }));
      }
    },
    [dataset],
  );

  const handleCollapseAnnotator = useCallback((annotator: string) => {
    setAnnotatorSections((prev) => ({
      ...prev,
      [annotator]: { ...prev[annotator], expanded: false },
    }));
  }, []);

  const handleToggleBaseColumn = (column: string, checked: boolean) => {
    if (checked) {
      setTempBaseColumns((prev) => [...prev, column]);
    } else {
      setTempBaseColumns((prev) => prev.filter((c) => c !== column));
    }
  };

  const handleToggleAnnotatorColumn = (
    annotator: string,
    column: string,
    checked: boolean,
  ) => {
    setAnnotatorSections((prev) => {
      const section = prev[annotator];
      const selected = checked
        ? [...section.selected, column]
        : section.selected.filter((c) => c !== column);
      return { ...prev, [annotator]: { ...section, selected } };
    });
  };

  const handleSelectAllBase = () => setTempBaseColumns([...baseColumnsList]);
  const handleSelectNoneBase = () => setTempBaseColumns([]);

  const handleSelectAllAnnotator = (annotator: string) => {
    setAnnotatorSections((prev) => ({
      ...prev,
      [annotator]: {
        ...prev[annotator],
        selected: [...prev[annotator].columns],
      },
    }));
  };
  const handleSelectNoneAnnotator = (annotator: string) => {
    setAnnotatorSections((prev) => ({
      ...prev,
      [annotator]: { ...prev[annotator], selected: [] },
    }));
  };

  const handleSave = () => {
    // Selecting every base column is saved as [] — the store's "all columns"
    // default — which keeps the URL state short and stays correct when new
    // columns appear later.
    const allSelected =
      tempBaseColumns.length === baseColumnsList.length &&
      baseColumnsList.length > 0;
    setBaseColumns(allSelected ? [] : tempBaseColumns);

    const newAnnotatorColumns: Record<string, string[]> = {};
    for (const [name, section] of Object.entries(annotatorSections)) {
      if (section.selected.length > 0) {
        newAnnotatorColumns[name] = section.selected;
      }
    }
    setAnnotatorColumns(newAnnotatorColumns);

    setOpen(false);
  };

  const handleCancel = () => setOpen(false);

  if (!dataset) {
    return null;
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm" className="gap-2">
          <Settings2 className="h-4 w-4" />
          Columns and Annotators
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Configure Columns</DialogTitle>
          <DialogDescription>
            Select columns from the base dataset and annotators to display
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-auto py-4 space-y-4">
          <div className="border rounded-md">
            <div className="bg-muted px-4 py-3 font-medium flex items-center justify-between">
              <span>Base Dataset</span>
              <span className="text-sm text-muted-foreground">
                {tempBaseColumns.length} selected
              </span>
            </div>
            <div className="p-4">
              <div className="flex gap-2 mb-3">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleSelectAllBase}
                >
                  Select All
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleSelectNoneBase}
                >
                  Select None
                </Button>
              </div>

              {baseColumnsLoading ? (
                <div className="space-y-2">
                  {Array.from({ length: 5 }).map((_, i) => (
                    <Skeleton key={i} className="h-8 w-full" />
                  ))}
                </div>
              ) : baseColumnsList.length === 0 ? (
                <div className="text-center text-muted-foreground py-4">
                  No base columns available
                </div>
              ) : (
                <div className="space-y-2 max-h-64 overflow-y-auto">
                  {baseColumnsList.map((col) => (
                    <div key={col} className="flex items-center space-x-2">
                      <Checkbox
                        id={`base-${col}`}
                        checked={tempBaseColumns.includes(col)}
                        onCheckedChange={(checked) =>
                          handleToggleBaseColumn(col, checked as boolean)
                        }
                      />
                      <Label
                        htmlFor={`base-${col}`}
                        className="cursor-pointer text-sm"
                      >
                        {col}
                      </Label>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {annotatorsList.length > 0 && (
            <div className="space-y-2">
              {annotatorsList.map((annotator) => {
                const section = annotatorSections[annotator];
                if (!section) return null;

                return (
                  <Collapsible
                    key={annotator}
                    open={section.expanded}
                    onOpenChange={(isOpen) => {
                      if (isOpen) {
                        handleExpandAnnotator(annotator);
                      } else {
                        handleCollapseAnnotator(annotator);
                      }
                    }}
                  >
                    <div className="border rounded-md">
                      <CollapsibleTrigger asChild>
                        <button className="w-full bg-muted px-4 py-3 font-medium flex items-center justify-between hover:bg-muted/80 transition-colors">
                          <div className="flex items-center gap-2">
                            {section.expanded ? (
                              <ChevronDown className="h-4 w-4" />
                            ) : (
                              <ChevronRight className="h-4 w-4" />
                            )}
                            <span>{annotator}</span>
                          </div>
                          <span className="text-sm text-muted-foreground">
                            {section.selected.length} selected
                          </span>
                        </button>
                      </CollapsibleTrigger>

                      <CollapsibleContent>
                        <div className="p-4">
                          {section.loading ? (
                            <div className="space-y-2">
                              <Skeleton className="h-8 w-full" />
                              <Skeleton className="h-8 w-full" />
                              <Skeleton className="h-8 w-3/4" />
                            </div>
                          ) : section.columns.length === 0 ? (
                            <div className="text-center text-muted-foreground py-4">
                              {section.expanded
                                ? "No columns available"
                                : "Click to load columns"}
                            </div>
                          ) : (
                            <>
                              <div className="flex gap-2 mb-3">
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() =>
                                    handleSelectAllAnnotator(annotator)
                                  }
                                >
                                  Select All
                                </Button>
                                <Button
                                  variant="outline"
                                  size="sm"
                                  onClick={() =>
                                    handleSelectNoneAnnotator(annotator)
                                  }
                                >
                                  Select None
                                </Button>
                              </div>
                              <div className="space-y-2 max-h-48 overflow-y-auto">
                                {section.columns.map((col) => (
                                  <div
                                    key={col}
                                    className="flex items-center space-x-2"
                                  >
                                    <Checkbox
                                      id={`${annotator}-${col}`}
                                      checked={section.selected.includes(col)}
                                      onCheckedChange={(checked) =>
                                        handleToggleAnnotatorColumn(
                                          annotator,
                                          col,
                                          checked as boolean,
                                        )
                                      }
                                    />
                                    <Label
                                      htmlFor={`${annotator}-${col}`}
                                      className="cursor-pointer text-sm"
                                    >
                                      {col}
                                    </Label>
                                  </div>
                                ))}
                              </div>
                            </>
                          )}
                        </div>
                      </CollapsibleContent>
                    </div>
                  </Collapsible>
                );
              })}
            </div>
          )}
        </div>

        <div className="pt-2 text-sm text-muted-foreground border-t">
          {totalSelected} total columns selected
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={baseColumnsLoading}>
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
