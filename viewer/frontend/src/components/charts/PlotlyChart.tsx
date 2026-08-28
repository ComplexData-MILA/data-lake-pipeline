import { useEffect, useRef } from "react";
import type { Data, Layout, Config, LegendClickEvent } from "plotly.js";

type PlotlyNS = typeof import("plotly.js");

let plotlyPromise: Promise<PlotlyNS> | null = null;

function loadPlotly(): Promise<PlotlyNS> {
  plotlyPromise ??= import("plotly.js-dist-min").then((m) => m.default);
  return plotlyPromise;
}

interface PlotlyEventTarget {
  on: (event: string, handler: (evt: unknown) => void) => void;
  removeListener: (event: string, handler: (evt: unknown) => void) => void;
}

function isPlotlyEventTarget(
  el: HTMLDivElement
): el is HTMLDivElement & PlotlyEventTarget {
  return typeof (el as unknown as PlotlyEventTarget).on === "function";
}

const BASE_CONFIG = { responsive: true, displaylogo: false } as const;

export interface PlotlyChartProps {
  data: Data[];
  layout: Partial<Layout>;
  config?: Partial<Config>;
  onLegendClick?: (evt: LegendClickEvent) => void;
  className?: string;
}

/**
 * Thin plotly.js wrapper: newPlot on mount, react() on prop changes (no
 * re-plot on theme/layout updates), purge on unmount, resize-aware, and an
 * optional plotly_legendclick forwarder so callers can sync external toggles.
 */
export function PlotlyChart({
  data,
  layout,
  config,
  onLegendClick,
  className,
}: PlotlyChartProps) {
  const elRef = useRef<HTMLDivElement>(null);
  const dataRef = useRef(data);
  dataRef.current = data;
  const layoutRef = useRef(layout);
  layoutRef.current = layout;
  // Latest legend handler via ref: the mount effect binds once (after plotly
  // has attached .on to the div), so a prop change must not require a rebind.
  const onLegendClickRef = useRef(onLegendClick);
  onLegendClickRef.current = onLegendClick;

  useEffect(() => {
    // Mount. The legend listener is bound only after newPlot resolves: the
    // bare div has no .on before plotly touches it (calling it earlier
    // throws "el.on is not a function").
    let cancelled = false;
    const el = elRef.current;
    if (!el) return;
    const legendHandler = (evt: unknown) =>
      onLegendClickRef.current?.(evt as LegendClickEvent);
    loadPlotly().then((Plotly) => {
      if (cancelled || !elRef.current) return;
      Plotly.newPlot(elRef.current, dataRef.current, layoutRef.current, {
        ...BASE_CONFIG,
        ...config,
      });
      if (onLegendClickRef.current && isPlotlyEventTarget(el)) {
        el.on("plotly_legendclick", legendHandler);
      }
    });
    return () => {
      cancelled = true;
      if (isPlotlyEventTarget(el)) {
        el.removeListener("plotly_legendclick", legendHandler);
      }
      loadPlotly().then((Plotly) => {
        if (elRef.current) Plotly.purge(elRef.current);
      });
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    // Data/layout updates
    loadPlotly().then((Plotly) => {
      if (!elRef.current) return;
      Plotly.react(elRef.current, dataRef.current, layoutRef.current, {
        ...BASE_CONFIG,
        ...config,
      });
    });
  }, [data, layout, config]);

  useEffect(() => {
    // Resize
    const el = elRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      loadPlotly().then((Plotly) => Plotly.Plots.resize(el));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return <div ref={elRef} className={className} />;
}
