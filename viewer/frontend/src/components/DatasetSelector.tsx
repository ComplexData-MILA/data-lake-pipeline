import { Button } from "@/components/ui/button";
import { Database, ChevronDown } from "lucide-react";
import { useViewerStore } from "@/hooks/useUrlState";

interface DatasetSelectorProps {
  onOpenChange: (open: boolean) => void;
}

export function DatasetSelector({ onOpenChange }: DatasetSelectorProps) {
  const { dataset } = useViewerStore();

  return (
    <Button
      variant="outline"
      onClick={() => onOpenChange(true)}
      className="gap-2 min-w-[200px] justify-between"
    >
      <div className="flex items-center gap-2">
        <Database className="h-4 w-4 text-muted-foreground" />
        <span className={dataset ? "" : "text-muted-foreground"}>
          {dataset || "Select dataset"}
        </span>
      </div>
      <ChevronDown className="h-4 w-4 text-muted-foreground" />
    </Button>
  );
}
