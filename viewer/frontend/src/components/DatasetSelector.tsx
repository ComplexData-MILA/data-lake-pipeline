import { useEffect, useState } from "react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { fetchDatasets } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";

export function DatasetSelector() {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const { dataset, setDataset, setColumns, setAnnotators, resetFilters } =
    useViewerStore();

  useEffect(() => {
    fetchDatasets()
      .then(setDatasets)
      .catch((err) => console.error("Failed to load datasets:", err))
      .finally(() => setLoading(false));
  }, []);

  const handleChange = (value: string) => {
    setDataset(value);
    setColumns([]);
    setAnnotators([]);
    resetFilters();
  };

  if (loading) {
    return (
      <Select disabled>
        <SelectTrigger className="w-full sm:w-[200px]">
          <SelectValue placeholder="Loading..." />
        </SelectTrigger>
      </Select>
    );
  }

  return (
    <Select value={dataset} onValueChange={handleChange}>
      <SelectTrigger className="w-full sm:w-[200px]">
        <SelectValue placeholder="Select dataset" />
      </SelectTrigger>
      <SelectContent>
        {datasets.map((d) => (
          <SelectItem key={d} value={d}>
            {d}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}