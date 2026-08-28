import { useSyncExternalStore } from "react";
import { useViewerStore } from "./useUrlState";

function subscribeSystemDark(callback: () => void): () => void {
  const mq = window.matchMedia("(prefers-color-scheme: dark)");
  mq.addEventListener("change", callback);
  return () => mq.removeEventListener("change", callback);
}

/** Resolve the effective dark mode from the viewer theme (light/dark/system). */
export function useDarkMode(): boolean {
  const theme = useViewerStore((s) => s.theme);
  const systemDark = useSyncExternalStore(
    subscribeSystemDark,
    () => window.matchMedia("(prefers-color-scheme: dark)").matches
  );
  return theme === "dark" || (theme === "system" && systemDark);
}
