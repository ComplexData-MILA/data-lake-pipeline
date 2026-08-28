import { create } from "zustand";

export type LiveStatus = "connecting" | "connected" | "reconnecting" | "offline";

interface LiveState {
  status: LiveStatus;
  pendingRows: number;
  refreshNonce: number;
  lastEventAt: number | null;
  setStatus: (status: LiveStatus) => void;
  addRows: (n: number) => void;
  bumpRefresh: () => void;
  resetPending: () => void;
}

/**
 * Ephemeral live-connection state — deliberately NOT serialized to the URL.
 */
export const useLiveStore = create<LiveState>((set) => ({
  status: "offline",
  pendingRows: 0,
  refreshNonce: 0,
  lastEventAt: null,
  setStatus: (status) => set({ status }),
  addRows: (n) =>
    set((s) => ({
      pendingRows: s.pendingRows + (n || 0),
      lastEventAt: Date.now(),
    })),
  bumpRefresh: () =>
    set((s) => ({ refreshNonce: s.refreshNonce + 1, lastEventAt: Date.now() })),
  resetPending: () => set({ pendingRows: 0 }),
}));
