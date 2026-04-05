import { useEffect, useState } from "react";
import { fetchAnnotators, fetchAnnotatorColumns } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Loader2, ChevronDown, ChevronRight } from "lucide-react";

interface AnnotatorColumnState {
  [annotator: string]: string[];
}

export function AnnotatorSelector() {
  const [annotators, setAnnotatorsList] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedAnnotator, setExpandedAnnotator] = useState<string | null>(null);
  const [columnLoading, setColumnLoading] = useState(false);
  const [availableColumns, setAvailableColumns] = useState<AnnotatorColumnState>({});
  
  const { 
    dataset, 
    annotators: selected, 
    setAnnotators,
    annotatorColumns,
    setAnnotatorColumns,
  } = useViewerStore();

  useEffect(() => {
    if (!dataset) {
      setAnnotatorsList([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    fetchAnnotators(dataset)
      .then(setAnnotatorsList)
      .catch((err) => console.error("Failed to load annotators:", err))
      .finally(() => setLoading(false));
  }, [dataset]);

  const handleExpandClick = async (annotator: string) => {
    if (expandedAnnotator === annotator) {
      setExpandedAnnotator(null);
      return;
    }
    
    setExpandedAnnotator(annotator);
    
    if (!availableColumns[annotator]) {
      setColumnLoading(true);
      try {
        const cols = await fetchAnnotatorColumns(dataset!, annotator);
        setAvailableColumns((prev) => ({ ...prev, [annotator]: cols }));
        if (!annotatorColumns[annotator]) {
          setAnnotatorColumns({ ...annotatorColumns, [annotator]: cols });
        }
      } catch (err) {
        console.error("Failed to load annotator columns:", err);
      } finally {
        setColumnLoading(false);
      }
    }
  };

  const handleColumnToggle = (annotator: string, column: string, checked: boolean) => {
    const currentColumns = annotatorColumns[annotator] || [];
    let newColumns: string[];
    if (checked) {
      newColumns = [...currentColumns, column];
    } else {
      newColumns = currentColumns.filter((c) => c !== column);
    }
    setAnnotatorColumns({ ...annotatorColumns, [annotator]: newColumns });
  };

  const handleToggle = (annotator: string, checked: boolean) => {
    if (checked) {
      setAnnotators([...selected, annotator]);
      if (!annotatorColumns[annotator] && availableColumns[annotator]) {
        setAnnotatorColumns({ ...annotatorColumns, [annotator]: availableColumns[annotator] });
      }
    } else {
      setAnnotators(selected.filter((a) => a !== annotator));
      const newAnnotatorColumns = { ...annotatorColumns };
      delete newAnnotatorColumns[annotator];
      setAnnotatorColumns(newAnnotatorColumns);
    }
  };

  if (loading || annotators.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-3">
        {annotators.map((annotator) => (
          <div key={annotator} className="flex items-center space-x-2">
            <Checkbox
              id={`annotator-${annotator}`}
              checked={selected.includes(annotator)}
              onCheckedChange={(checked) =>
                handleToggle(annotator, checked as boolean)
              }
            />
            <Label
              htmlFor={`annotator-${annotator}`}
              className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70 cursor-pointer"
            >
              {annotator}
            </Label>
            <Button
              variant="ghost"
              size="sm"
              className="h-6 w-6 p-0"
              onClick={() => handleExpandClick(annotator)}
              title="Select columns"
            >
              {columnLoading && expandedAnnotator === annotator ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : expandedAnnotator === annotator ? (
                <ChevronDown className="h-3 w-3" />
              ) : (
                <ChevronRight className="h-3 w-3" />
              )}
            </Button>
          </div>
        ))}
      </div>
      
      {expandedAnnotator && availableColumns[expandedAnnotator] && (
        <div className="ml-6 p-3 bg-muted/30 rounded-md">
          <Label className="text-xs text-muted-foreground mb-2 block">
            Select columns for {expandedAnnotator}:
          </Label>
          <div className="flex flex-wrap gap-2">
            {availableColumns[expandedAnnotator].map((col) => (
              <div key={col} className="flex items-center space-x-1">
                <Checkbox
                  id={`col-${expandedAnnotator}-${col}`}
                  checked={(annotatorColumns[expandedAnnotator] || []).includes(col)}
                  onCheckedChange={(checked) =>
                    handleColumnToggle(expandedAnnotator, col, checked as boolean)
                  }
                />
                <Label
                  htmlFor={`col-${expandedAnnotator}-${col}`}
                  className="text-xs cursor-pointer"
                >
                  {col}
                </Label>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}