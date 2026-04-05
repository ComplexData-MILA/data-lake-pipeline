import { useEffect, useState } from "react";
import { fetchAnnotators } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";

export function AnnotatorSelector() {
  const [annotators, setAnnotatorsList] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const { dataset, annotators: selected, setAnnotators } = useViewerStore();

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

  if (loading || annotators.length === 0) {
    return null;
  }

  const handleToggle = (annotator: string, checked: boolean) => {
    if (checked) {
      setAnnotators([...selected, annotator]);
    } else {
      setAnnotators(selected.filter((a) => a !== annotator));
    }
  };

  return (
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
        </div>
      ))}
    </div>
  );
}