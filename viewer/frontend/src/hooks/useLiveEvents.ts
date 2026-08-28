import { useEffect } from "react";
import { useLiveStore } from "./useLiveStore";

let debounceTimer: ReturnType<typeof setTimeout> | null = null;

/** Debounced refresh bump so event bursts trigger a single refetch. */
function scheduleRefresh(bump: () => void): void {
  if (debounceTimer) clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    debounceTimer = null;
    bump();
  }, 800);
}

export interface ViewerEventPayload {
  event_id?: string;
  type: string;
  dataset?: string;
  batch?: string | null;
  annotator?: string | null;
  row_count?: number | null;
  ts?: string;
  source?: string;
}

/**
 * Subscribe to the SSE event stream.
 *
 * *dataset* is the dataset filter ("" subscribes to ALL datasets — used by
 * the global activity/charts streams; the backend watcher watches every
 * dataset while such a subscriber is connected). Pass *null* to disable.
 *
 * Reconnects with exponential backoff (1s -> 30s cap + jitter). Named events
 * accumulate a "+N new rows" counter (only direct producer events carry
 * row_count — watcher events are count-less, so nothing double-counts) and
 * schedule a debounced refetch of the visible data. *countRows* (default
 * true) disables the counter for secondary streams so the pending-rows
 * banner doesn't double-count.
 */
export function useLiveEvents(
  dataset: string | null,
  opts?: { countRows?: boolean }
): void {
  const countRows = opts?.countRows !== false;
  const setStatus = useLiveStore((s) => s.setStatus);
  const addRows = useLiveStore((s) => s.addRows);
  const bumpRefresh = useLiveStore((s) => s.bumpRefresh);

  useEffect(() => {
    if (dataset === null) {
      setStatus("offline");
      return;
    }

    let es: EventSource | null = null;
    let closed = false;
    let retry = 1000;

    const handleEvent = (event: MessageEvent) => {
      try {
        const data: ViewerEventPayload = JSON.parse(event.data);
        const type = data.type || event.type;
        if (type === "rows_ingested" || type === "batch_merged") {
          if (countRows) {
            addRows(typeof data.row_count === "number" ? data.row_count : 0);
          }
          scheduleRefresh(bumpRefresh);
        } else if (type === "annotation_updated" || type === "run_completed") {
          scheduleRefresh(bumpRefresh);
        }
      } catch {
        // ignore malformed events
      }
    };

    const connect = () => {
      setStatus(retry > 1000 ? "reconnecting" : "connecting");
      es = new EventSource(`/api/events?dataset=${encodeURIComponent(dataset)}`);

      es.onopen = () => {
        retry = 1000;
        setStatus("connected");
        // Missed events while offline -> full refetch.
        bumpRefresh();
      };
      es.onerror = () => {
        es?.close();
        if (closed) return;
        setStatus("reconnecting");
        retry = Math.min(retry * 2, 30000) + Math.random() * 1000;
        setTimeout(connect, retry);
      };

      es.addEventListener("rows_ingested", handleEvent);
      es.addEventListener("batch_merged", handleEvent);
      es.addEventListener("annotation_updated", handleEvent);
      es.addEventListener("run_completed", handleEvent);
    };

    connect();
    return () => {
      closed = true;
      es?.close();
    };
  }, [dataset, countRows, setStatus, addRows, bumpRefresh]);
}
