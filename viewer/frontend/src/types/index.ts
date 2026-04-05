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
}

export interface CountResponse {
  count: number;
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

export interface ViewerState {
  dataset: string;
  page: number;
  pageSize: number;
  annotators: string[];
  annotatorColumns: Record<string, string[]>;
  columns: string[];
  filters: {
    base?: FilterSpec;
    annotators: Record<string, FilterSpec | undefined>;
  };
  selectedId: string | null;
  sort?: string;
  sortDir?: "asc" | "desc";
}

export type ViewerStateKey = keyof ViewerState;

export const defaultState: ViewerState = {
  dataset: "",
  page: 1,
  pageSize: 50,
  annotators: [],
  annotatorColumns: {},
  columns: [],
  filters: {
    base: undefined,
    annotators: {},
  },
  selectedId: null,
  sort: undefined,
  sortDir: "asc",
};