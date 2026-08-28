import { create } from "zustand";
import type { ViewerState, FilterSpec, TabId } from "@/types";
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
      // Old URLs carrying page/cursor fields still decode — unknown keys are
      // ignored and page/cursor are no longer part of the state.
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
  setTab: (tab: TabId) => void;
  setPageSize: (pageSize: number) => void;
  setAnnotators: (annotators: string[]) => void;
  setAnnotatorColumns: (annotatorColumns: Record<string, string[]>) => void;
  setBaseColumns: (baseColumns: string[]) => void;
  setBaseFilter: (filter: FilterSpec | undefined) => void;
  setAnnotatorFilter: (annotator: string, filter: FilterSpec | undefined) => void;
  setSort: (sort: string | undefined, sortDir?: "asc" | "desc") => void;
  setSelectedId: (id: string | null) => void;
  setTheme: (theme: "light" | "dark" | "system") => void;
  setStream: (stream: boolean) => void;
  resetFilters: () => void;
  reset: () => void;
}

export const useViewerStore = create<ViewerStore>((set, get) => ({
  ...getInitialState(),

  setDataset: (dataset) => {
    set({ dataset, selectedId: null, baseColumns: [], annotatorColumns: {} });
    updateUrl({ ...get(), dataset, selectedId: null, baseColumns: [], annotatorColumns: {} });
  },

  setTab: (tab) => {
    set({ tab });
    updateUrl({ ...get(), tab });
  },

  setPageSize: (pageSize) => {
    set({ pageSize });
    updateUrl({ ...get(), pageSize });
  },

  setTheme: (theme) => {
    set({ theme });
    updateUrl({ ...get(), theme });
  },

  setStream: (stream) => {
    set({ stream });
    updateUrl({ ...get(), stream });
  },

  setAnnotators: (annotators) => {
    set({ annotators });
    updateUrl({ ...get(), annotators });
  },

  setAnnotatorColumns: (annotatorColumns) => {
    set({ annotatorColumns });
    updateUrl({ ...get(), annotatorColumns });
  },

  setBaseColumns: (baseColumns) => {
    set({ baseColumns });
    updateUrl({ ...get(), baseColumns });
  },

  setBaseFilter: (filter) => {
    set((state) => ({
      filters: { ...state.filters, base: filter },
    }));
    const currentState = get();
    updateUrl({ ...currentState, filters: { ...currentState.filters, base: filter } });
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
    set({ filters: { base: undefined, annotators: {} } });
    updateUrl({ ...get(), filters: { base: undefined, annotators: {} } });
  },

  reset: () => {
    set({ ...defaultState });
    updateUrl({ ...defaultState });
  },
}));
