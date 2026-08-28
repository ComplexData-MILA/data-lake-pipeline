export interface SchemaColumn {
  name: string;
  type: string;
}

export interface DatasetListResponse {
  datasets: string[];
}

export interface AnnotationListResponse {
  annotators: string[];
}

export interface SchemaResponse {
  columns: SchemaColumn[];
}

export interface DataResponse {
  rows: Record<string, unknown>[];
  columns: string[];
  annotator_columns: Record<string, string[]>;
  next_cursor?: string | null;
  has_more?: boolean | null;
}

export interface ViewerEvent {
  event_id?: string;
  type: string;
  dataset?: string;
  batch?: string | null;
  annotator?: string | null;
  row_count?: number | null;
  ts?: string;
  source?: string;
}

export interface CountResponse {
  count: number;
}

export interface ConversionResponse {
  total_batches: number;
  converted: number;
  in_progress_batch?: string | null;
  error?: string | null;
  oversized?: boolean;
  started_at?: string | null;
  updated_at?: string | null;
  annotation_total?: number;
  annotation_converted?: number;
}

export type FilterOp = 
  | "eq" 
  | "neq" 
  | "gt" 
  | "gte" 
  | "lt" 
  | "lte" 
  | "contains" 
  | "startswith" 
  | "endswith";

export interface FilterSpec {
  field: string;
  op: FilterOp;
  value: string | number | boolean;
}

export type ThemeMode = "light" | "dark" | "system";

export type TabId = "data" | "activity" | "charts";

export interface ViewerState {
  dataset: string;
  tab: TabId;
  pageSize: number; // fetch batch size for infinite scroll
  annotators: string[];
  annotatorColumns: Record<string, string[]>;
  baseColumns: string[]; // [] means "all columns"
  filters: {
    base?: FilterSpec;
    annotators: Record<string, FilterSpec | undefined>;
  };
  selectedId: string | null;
  sort?: string;
  sortDir?: "asc" | "desc";
  theme: ThemeMode;
  stream: boolean;
}

export type ViewerStateKey = keyof ViewerState;

export const defaultState: ViewerState = {
  dataset: "",
  tab: "data",
  pageSize: 50,
  annotators: [],
  annotatorColumns: {},
  baseColumns: [],
  filters: {
    base: undefined,
    annotators: {},
  },
  selectedId: null,
  sort: undefined,
  sortDir: "asc",
  theme: "system",
  stream: false,
};

export interface ActivityBucket {
  ts: string;
  count: number;
}

export interface ActivityDataset {
  dataset: string;
  buckets: ActivityBucket[];
}

export interface ChartWindow {
  start: string | null;
  end: string | null;
}

export interface ActivityResponse {
  datasets: ActivityDataset[];
  window: ChartWindow;
  bucket: string;
  generated_at: string;
}

export interface CategoricalResponse {
  mode: "counts" | "trend";
  column: string;
  values?: { value: string; count: number }[];
  total: number;
  distinct?: number | null;
  truncated?: boolean | null;
  top_values?: string[];
  series?: { ts: string; value: string; count: number }[];
  window: ChartWindow;
  generated_at: string;
}