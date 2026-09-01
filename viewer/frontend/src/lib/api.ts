import type {
  DatasetListResponse,
  AnnotationListResponse,
  SchemaResponse,
  DataResponse,
  CountResponse,
  ConversionResponse,
  ActivityResponse,
  ActivityDataset,
  CategoricalResponse,
} from "@/types";

export const API_BASE = import.meta.env.VITE_API_BASE ?? "/api";

export async function fetchDatasets(): Promise<string[]> {
  const res = await fetch(`${API_BASE}/datasets`);
  if (!res.ok) throw new Error("Failed to fetch datasets");
  const data: DatasetListResponse = await res.json();
  return data.datasets;
}

export async function fetchConversion(
  dataset: string
): Promise<ConversionResponse> {
  const res = await fetch(`${API_BASE}/datasets/${dataset}/conversion`);
  if (!res.ok) throw new Error("Failed to fetch conversion status");
  return res.json();
}

export async function fetchAnnotators(dataset: string): Promise<string[]> {
  const res = await fetch(`${API_BASE}/datasets/${dataset}/annotations`);
  if (!res.ok) throw new Error("Failed to fetch annotators");
  const data: AnnotationListResponse = await res.json();
  return data.annotators;
}

export async function fetchAnnotatorColumns(
  dataset: string,
  annotator: string
): Promise<string[]> {
  const res = await fetch(
    `${API_BASE}/datasets/${dataset}/annotations/${annotator}/columns`
  );
  if (!res.ok) throw new Error("Failed to fetch annotator columns");
  const data = await res.json();
  return data.columns;
}

export async function fetchSchema(
  dataset: string,
  annotators: string[]
): Promise<SchemaResponse> {
  const params = new URLSearchParams();
  if (annotators.length > 0) {
    params.set("annotators", annotators.join(","));
  }
  const url = `${API_BASE}/datasets/${dataset}/schema${params.toString() ? `?${params}` : ""}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to fetch schema");
  return res.json();
}

export async function fetchCount(
  dataset: string,
  filters: object
): Promise<number> {
  const params = new URLSearchParams();
  params.set("filters", JSON.stringify(filters));
  const res = await fetch(
    `${API_BASE}/datasets/${dataset}/count?${params}`
  );
  if (!res.ok) throw new Error("Failed to fetch count");
  const data: CountResponse = await res.json();
  return data.count;
}

export interface FetchDataParams {
  page: number;
  pageSize: number;
  columns: string[];
  annotatorColumns?: Record<string, string[]>;
  filters: object;
  sort?: string;
  sortDir?: "asc" | "desc";
  cursor?: string | null;
}

export async function fetchData(
  dataset: string,
  params: FetchDataParams
): Promise<DataResponse> {
  const urlParams = new URLSearchParams();
  urlParams.set("page", String(params.page));
  urlParams.set("page_size", String(params.pageSize));
  urlParams.set("columns", params.columns.join(","));
  if (params.annotatorColumns && Object.keys(params.annotatorColumns).length > 0) {
    urlParams.set("annotator_columns", JSON.stringify(params.annotatorColumns));
  }
  urlParams.set("filters", JSON.stringify(params.filters));
  if (params.sort) {
    urlParams.set("sort", params.sort);
    urlParams.set("sort_dir", params.sortDir || "asc");
  }
  if (params.cursor) {
    urlParams.set("cursor", params.cursor);
  }
  const res = await fetch(
    `${API_BASE}/datasets/${dataset}/data?${urlParams}`
  );
  if (!res.ok) throw new Error("Failed to fetch data");
  return res.json();
}

export async function fetchActivity(
  bucket: string,
  minutes?: number
): Promise<ActivityResponse> {
  const params = new URLSearchParams();
  params.set("bucket", bucket);
  if (minutes !== undefined) {
    params.set("minutes", String(minutes));
  }
  const res = await fetch(`${API_BASE}/activity?${params}`);
  if (!res.ok) throw new Error("Failed to fetch activity");
  return res.json();
}

export interface ActivityStreamCallbacks {
  onWindow?: (w: {
    window: { start: string | null; end: string | null };
    bucket: string;
    generated_at: string;
  }) => void;
  onDataset?: (d: ActivityDataset) => void;
  onDone?: () => void;
  onError?: (message: string) => void;
}

/**
 * Fetch /activity in NDJSON streaming mode: per-dataset buckets arrive as the
 * backend computes them (progressive chart population), and cancelling aborts
 * the fetch so the server stops computing the remaining datasets.
 */
export function fetchActivityStream(
  bucket: string,
  minutes: number,
  callbacks: ActivityStreamCallbacks
): StreamControl {
  const controller = new AbortController();
  const params = new URLSearchParams();
  params.set("bucket", bucket);
  params.set("minutes", String(minutes));
  params.set("format", "ndjson");

  const done = (async () => {
    try {
      const res = await fetch(`${API_BASE}/activity?${params}`, {
        signal: controller.signal,
      });
      if (!res.ok || !res.body) throw new Error("Failed to stream activity");
      await readNdjson(res, (msg) => {
        switch (msg.type) {
          case "window":
            callbacks.onWindow?.(
              msg as unknown as {
                window: { start: string | null; end: string | null };
                bucket: string;
                generated_at: string;
              }
            );
            break;
          case "dataset":
            callbacks.onDataset?.(msg as unknown as ActivityDataset);
            break;
          case "done":
            callbacks.onDone?.();
            break;
          case "error":
            callbacks.onError?.(String(msg.message ?? "Activity failed"));
            break;
        }
      });
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        callbacks.onError?.((err as Error).message);
      }
    }
  })();

  return { cancel: () => controller.abort(), done };
}

export interface CategoricalParams {
  column: string;
  mode: "counts" | "trend";
  bucket?: string;
  limit?: number;
  minutes?: number;
}

export async function fetchCategorical(
  dataset: string,
  params: CategoricalParams
): Promise<CategoricalResponse> {
  const urlParams = new URLSearchParams();
  urlParams.set("column", params.column);
  urlParams.set("mode", params.mode);
  if (params.bucket) urlParams.set("bucket", params.bucket);
  if (params.limit !== undefined) urlParams.set("limit", String(params.limit));
  if (params.minutes !== undefined) urlParams.set("minutes", String(params.minutes));
  const res = await fetch(
    `${API_BASE}/datasets/${dataset}/categorical?${urlParams}`
  );
  if (!res.ok) throw new Error("Failed to fetch categorical data");
  return res.json();
}

export async function fetchRow(
  dataset: string,
  rowId: string,
  params: {
    columns: string[];
    annotatorColumns?: Record<string, string[]>;
  }
): Promise<DataResponse> {
  const urlParams = new URLSearchParams();
  urlParams.set("row_id", rowId);
  urlParams.set("columns", params.columns.join(","));
  if (params.annotatorColumns && Object.keys(params.annotatorColumns).length > 0) {
    urlParams.set("annotator_columns", JSON.stringify(params.annotatorColumns));
  }
  const res = await fetch(`${API_BASE}/datasets/${dataset}/data?${urlParams}`);
  if (!res.ok) throw new Error("Failed to fetch row");
  return res.json();
}

export interface StreamCallbacks {
  onMeta?: (meta: { columns: string[]; annotator_columns: Record<string, string[]> }) => void;
  onRows?: (rows: Record<string, unknown>[]) => void;
  onDone?: () => void;
  onError?: (message: string) => void;
}

export interface StreamControl {
  cancel: () => void;
  done: Promise<void>;
}

/**
 * Consume an NDJSON response body line by line, invoking *onMessage* per
 * parsed message. Stops dispatching once a terminal message ("done"/"error")
 * was seen (mid-stream error lines after that point are ignored).
 */
async function readNdjson(
  res: Response,
  onMessage: (msg: Record<string, unknown>) => void
): Promise<void> {
  if (!res.body) throw new Error("Empty response body");
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let finished = false;
  const handleLine = (line: string) => {
    if (!line.trim() || finished) return;
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(line);
    } catch {
      return; // ignore malformed lines
    }
    if (msg.type === "done" || msg.type === "error") finished = true;
    onMessage(msg);
  };
  for (;;) {
    const { value, done: streamDone } = await reader.read();
    if (streamDone) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    lines.forEach(handleLine);
  }
  if (buffer) handleLine(buffer);
}

/**
 * Fetch /data in NDJSON streaming mode: rows arrive in batches as the backend
 * produces them (progressive table population for slow filtered scans).
 */
export function fetchDataStream(
  dataset: string,
  params: FetchDataParams,
  callbacks: StreamCallbacks
): StreamControl {
  const controller = new AbortController();
  const urlParams = new URLSearchParams();
  urlParams.set("format", "ndjson");
  urlParams.set("page", String(params.page));
  urlParams.set("page_size", String(params.pageSize));
  urlParams.set("columns", params.columns.join(","));
  if (params.annotatorColumns && Object.keys(params.annotatorColumns).length > 0) {
    urlParams.set("annotator_columns", JSON.stringify(params.annotatorColumns));
  }
  urlParams.set("filters", JSON.stringify(params.filters));
  if (params.sort) {
    urlParams.set("sort", params.sort);
    urlParams.set("sort_dir", params.sortDir || "asc");
  }

  const done = (async () => {
    try {
      const res = await fetch(`${API_BASE}/datasets/${dataset}/data?${urlParams}`, {
        signal: controller.signal,
      });
      if (!res.ok || !res.body) throw new Error("Failed to stream data");
      let finished = false;
      await readNdjson(res, (msg) => {
        if (msg.type === "meta") {
          callbacks.onMeta?.(
            msg as unknown as {
              columns: string[];
              annotator_columns: Record<string, string[]>;
            }
          );
        } else if (msg.type === "row") {
          callbacks.onRows?.([msg.row as Record<string, unknown>]);
        } else if (msg.type === "done" || msg.type === "error") {
          finished = true;
          if (msg.type === "done") callbacks.onDone?.();
          else callbacks.onError?.(String(msg.message ?? "Stream error"));
        }
      });
      if (!finished) callbacks.onDone?.();
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        callbacks.onError?.((err as Error).message);
      }
    }
  })();

  return { cancel: () => controller.abort(), done };
}