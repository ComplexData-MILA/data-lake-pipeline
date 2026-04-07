import { create } from "zustand";
import type { ViewerState, FilterSpec } from "@/types";
import { defaultState } from "@/types";

function encodeState(state: ViewerState): string {
  return btoa(JSON.stringify(state));
}

function decodeState(encoded: string): ViewerState | null {
  try {
    return JSON.parse(atob(encoded)) as ViewerState;
  } catch {
    return null;
  }
}

function getInitialState(): ViewerState {
  const params = new URLSearchParams(window.location.search);
  const encoded = params.get("s");
  if (encoded) {
    const decoded = decodeState(encoded);
    if (decoded) {
      return { ...defaultState, ...decoded };
    }
  }
  return { ...defaultState };
}

function updateUrl(state: ViewerState) {
  const encoded = encodeState(state);
  const url = new URL(window.location.href);
  url.searchParams.set("s", encoded);
  window.history.replaceState({}, "", url.toString());
}

interface ViewerStore extends ViewerState {
  setDataset: (dataset: string) => void;
  setPage: (page: number) => void;
  setPageSize: (pageSize: number) => void;
  setAnnotators: (annotators: string[]) => void;
  setAnnotatorColumns: (annotatorColumns: Record<string, string[]>) => void;
  setBaseColumns: (baseColumns: string[]) => void;
  setBaseFilter: (filter: FilterSpec | undefined) => void;
  setAnnotatorFilter: (annotator: string, filter: FilterSpec | undefined) => void;
  setSort: (sort: string | undefined, sortDir?: "asc" | "desc") => void;
  setSelectedId: (id: string | null) => void;
  resetFilters: () => void;
  reset: () => void;
}

export const useViewerStore = create<ViewerStore>((set, get) => ({
  ...getInitialState(),

  setDataset: (dataset) => {
    set({ dataset, page: 1, selectedId: null, baseColumns: [], annotatorColumns: {} });
    updateUrl({ ...get(), dataset, page: 1, selectedId: null, baseColumns: [], annotatorColumns: {} });
  },

  setPage: (page) => {
    set({ page });
    updateUrl({ ...get(), page });
  },

  setPageSize: (pageSize) => {
    set({ pageSize, page: 1 });
    updateUrl({ ...get(), pageSize, page: 1 });
  },

  setAnnotators: (annotators) => {
    set({ annotators, page: 1 });
    updateUrl({ ...get(), annotators, page: 1 });
  },

  setAnnotatorColumns: (annotatorColumns) => {
    set({ annotatorColumns, page: 1 });
    updateUrl({ ...get(), annotatorColumns, page: 1 });
  },

  setBaseColumns: (baseColumns) => {
    set({ baseColumns });
    updateUrl({ ...get(), baseColumns });
  },

  setBaseFilter: (filter) => {
    set((state) => ({
      filters: { ...state.filters, base: filter },
      page: 1,
    }));
    const currentState = get();
    updateUrl({ ...currentState, filters: { ...currentState.filters, base: filter }, page: 1 });
  },

  setAnnotatorFilter: (annotator, filter) => {
    set((state) => ({
      filters: {
        ...state.filters,
        annotators: {
          ...state.filters.annotators,
          [annotator]: filter,
        },
      },
      page: 1,
    }));
    const currentState = get();
    updateUrl({
      ...currentState,
      filters: {
        ...currentState.filters,
        annotators: {
          ...currentState.filters.annotators,
          [annotator]: filter,
        },
      },
      page: 1,
    });
  },

  setSort: (sort, sortDir = "asc") => {
    set({ sort, sortDir });
    updateUrl({ ...get(), sort, sortDir });
  },

  setSelectedId: (selectedId) => {
    set({ selectedId });
    updateUrl({ ...get(), selectedId });
  },

  resetFilters: () => {
    set({ filters: { base: undefined, annotators: {} }, page: 1 });
    updateUrl({ ...get(), filters: { base: undefined, annotators: {} }, page: 1 });
  },

  reset: () => {
    set({ ...defaultState });
    updateUrl({ ...defaultState });
  },
}));