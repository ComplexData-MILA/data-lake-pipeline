import type {
  DatasetListResponse,
  AnnotationListResponse,
  SchemaResponse,
  DataResponse,
  CountResponse,
  RowResponse,
} from "@/types";

const API_BASE = "/api";

export async function fetchDatasets(): Promise<string[]> {
  const res = await fetch(`${API_BASE}/datasets`);
  if (!res.ok) throw new Error("Failed to fetch datasets");
  const data: DatasetListResponse = await res.json();
  return data.datasets;
}

export async function fetchAnnotators(dataset: string): Promise<string[]> {
  const res = await fetch(`${API_BASE}/datasets/${dataset}/annotations`);
  if (!res.ok) throw new Error("Failed to fetch annotators");
  const data: AnnotationListResponse = await res.json();
  return data.annotators;
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
  annotators: string[],
  filters: object
): Promise<number> {
  const params = new URLSearchParams();
  if (annotators.length > 0) {
    params.set("annotators", annotators.join(","));
  }
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
  annotators: string[];
  filters: object;
  sort?: string;
  sortDir?: "asc" | "desc";
}

export async function fetchData(
  dataset: string,
  params: FetchDataParams
): Promise<DataResponse> {
  const urlParams = new URLSearchParams();
  urlParams.set("page", String(params.page));
  urlParams.set("page_size", String(params.pageSize));
  urlParams.set("columns", params.columns.join(","));
  if (params.annotators.length > 0) {
    urlParams.set("annotators", params.annotators.join(","));
  }
  urlParams.set("filters", JSON.stringify(params.filters));
  if (params.sort) {
    urlParams.set("sort", params.sort);
    urlParams.set("sort_dir", params.sortDir || "asc");
  }
  const res = await fetch(
    `${API_BASE}/datasets/${dataset}/data?${urlParams}`
  );
  if (!res.ok) throw new Error("Failed to fetch data");
  return res.json();
}

export async function fetchRow(
  dataset: string,
  rowId: string
): Promise<RowResponse> {
  const res = await fetch(`${API_BASE}/datasets/${dataset}/row/${rowId}`);
  if (!res.ok) throw new Error("Failed to fetch row");
  return res.json();
}