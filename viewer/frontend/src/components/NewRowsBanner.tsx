import { useLiveStore } from "@/hooks/useLiveStore";
import { Button } from "@/components/ui/button";
import { RefreshCw, X } from "lucide-react";

export function NewRowsBanner() {
  const pendingRows = useLiveStore((s) => s.pendingRows);
  const bumpRefresh = useLiveStore((s) => s.bumpRefresh);
  const resetPending = useLiveStore((s) => s.resetPending);

  if (pendingRows <= 0) return null;

  return (
    <div className="mb-4 flex items-center justify-between gap-4 rounded-lg border border-green-500/30 bg-green-500/10 px-4 py-2.5 text-sm">
      <span className="font-medium text-green-700 dark:text-green-400">
        +{pendingRows.toLocaleString()} new row{pendingRows === 1 ? "" : "s"} · auto-refreshing
      </span>
      <span className="flex items-center gap-1">
        <Button
          variant="ghost"
          size="sm"
          className="gap-1 h-7"
          onClick={() => {
            resetPending();
            bumpRefresh();
          }}
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh now
        </Button>
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7"
          onClick={resetPending}
          aria-label="Dismiss"
        >
          <X className="h-3.5 w-3.5" />
        </Button>
      </span>
    </div>
  );
}
