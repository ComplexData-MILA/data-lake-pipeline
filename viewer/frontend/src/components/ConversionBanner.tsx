import { useEffect, useState } from "react";
import { fetchConversion } from "@/lib/api";
import { useLiveStore } from "@/hooks/useLiveStore";
import type { ConversionResponse } from "@/types";

/**
 * Shows parquet -> JSONL conversion progress while it is incomplete for the
 * selected dataset ("Converting X/Y batches…"), refreshing on live events and
 * every 15s. Hidden once conversion completes (or when nothing to convert).
 */
export function ConversionBanner({ dataset }: { dataset: string }) {
  const [status, setStatus] = useState<ConversionResponse | null>(null);
  const refreshNonce = useLiveStore((s) => s.refreshNonce);

  useEffect(() => {
    if (!dataset) return;
    let cancelled = false;
    const load = async () => {
      try {
        const next = await fetchConversion(dataset);
        if (!cancelled) setStatus(next);
      } catch {
        // backend without /conversion support: stay hidden
      }
    };
    load();
    const interval = setInterval(load, 15000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [dataset, refreshNonce]);

  if (!status) return null;
  const { total_batches, converted, error } = status;
  if (error && converted === 0) return null;
  if (!total_batches || converted >= total_batches) return null;

  return (
    <div className="mb-4 flex items-center gap-3 rounded-lg border border-blue-500/30 bg-blue-500/10 px-4 py-2.5 text-sm">
      <span className="h-3 w-3 animate-pulse rounded-full bg-blue-500" />
      <span className="font-medium text-blue-700 dark:text-blue-400">
        Converting storage format: {converted}/{total_batches} batches
        {status.in_progress_batch ? ` (now: ${status.in_progress_batch})` : ""}
      </span>
      <span className="text-muted-foreground">
        The viewer keeps working while the conversion job runs.
      </span>
    </div>
  );
}
