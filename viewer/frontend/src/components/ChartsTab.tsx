import { useEffect, useMemo, useState } from "react";
import type { Data, Layout } from "plotly.js";
import { fetchSchema, fetchCategorical } from "@/lib/api";
import type { CategoricalResponse } from "@/types";
import { useLiveStore } from "@/hooks/useLiveStore";
import { useDarkMode } from "@/hooks/useDarkMode";
import { useViewerStore } from "@/hooks/useUrlState";
import { chartInk, OTHER_COLOR, SERIES_CAP } from "@/lib/chartTheme";
import { PlotlyChart } from "@/components/charts/PlotlyChart";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertTriangle } from "lucide-react";

const SYSTEM_COLUMNS = new Set(["id", "_batch", "_created_at"]);

const BUCKET_OPTIONS = [
  { value: "1m", label: "per minute" },
  { value: "5m", label: "per 5 minutes" },
  { value: "1h", label: "per hour" },
  { value: "1d", label: "per day" },
] as const;

const WINDOW_OPTIONS = [
  { value: 60, label: "last hour" },
  { value: 1440, label: "last 24 hours" },
  { value: 10080, label: "last 7 days" },
  { value: 43200, label: "last 30 days" },
  { value: -1, label: "all time" },
] as const;

const LIMIT_OPTIONS = [3, 4, 5, 6, 7, 8] as const;

export function ChartsTab({ onOpenDatasetDialog }: { onOpenDatasetDialog?: () => void }) {
  const dark = useDarkMode();
  const ink = chartInk(dark);
  const dataset = useViewerStore((s) => s.dataset);
  const refreshNonce = useLiveStore((s) => s.refreshNonce);

  const [columns, setColumns] = useState<string[]>([]);
  const [column, setColumn] = useState<string>("");
  const [mode, setMode] = useState<"counts" | "trend">("counts");
  const [bucket, setBucket] = useState<string>("1h");
  const [limit, setLimit] = useState<number>(8);
  const [minutes, setMinutes] = useState<number>(-1);
  const [data, setData] = useState<CategoricalResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [retryNonce, setRetryNonce] = useState(0);

  // Column list (schema "type" is always "unknown", so no type filtering).
  useEffect(() => {
    if (!dataset) return;
    let cancelled = false;
    fetchSchema(dataset, [])
      .then((res) => {
        if (cancelled) return;
        const cols = res.columns
          .map((c) => c.name)
          .filter((n) => !SYSTEM_COLUMNS.has(n) && !n.includes("."));
        setColumns(cols);
        setColumn((current) => (current && cols.includes(current) ? current : cols[0] ?? ""));
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [dataset]);

  useEffect(() => {
    if (!dataset || !column) {
      setData(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchCategorical(dataset, {
      column,
      mode,
      bucket,
      limit,
      minutes: minutes === -1 ? undefined : minutes,
    })
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((e) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Failed to load chart data");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dataset, column, mode, bucket, limit, minutes, refreshNonce, retryNonce]);

  // Colors follow the value identity: slot by alphabetical position in the
  // value set (stable across refreshes even when counts reorder).
  const valueColors = useMemo(() => {
    const values =
      mode === "counts"
        ? (data?.values ?? []).map((v) => v.value)
        : (data?.top_values ?? []);
    const sorted = [...values].sort();
    const map = new Map<string, string>();
    sorted.forEach((v, i) => {
      map.set(v, ink.palette[i % SERIES_CAP]);
    });
    return map;
  }, [data, mode, ink]);

  const { traces, layout } = useMemo((): { traces: Data[]; layout: Partial<Layout> } => {
    const base: Partial<Layout> = {
      height: 460,
      margin: { l: 56, r: 16, t: 8, b: 40 },
      font: {
        family: 'system-ui, -apple-system, "Segoe UI", sans-serif',
        color: ink.ink,
      },
      paper_bgcolor: ink.paper,
      plot_bgcolor: ink.plot,
      hovermode: "x unified",
      xaxis: {
        gridcolor: ink.grid,
        zerolinecolor: ink.axis,
        linecolor: ink.axis,
        tickfont: { color: ink.secondary },
      },
      yaxis: {
        gridcolor: ink.grid,
        zeroline: false,
        linecolor: ink.axis,
        tickfont: { color: ink.secondary },
      },
      legend: {
        orientation: "h",
        y: 1.08,
        x: 0,
        font: { color: ink.secondary },
      },
    };

    if (!data) return { traces: [], layout: base };

    if (data.mode === "counts") {
      const values = data.values ?? [];
      const tracesForCounts: Data[] = values.map((v) => ({
        x: [v.value],
        y: [v.count],
        type: "bar",
        name: v.value,
        marker: { color: valueColors.get(v.value) ?? OTHER_COLOR },
        hovertemplate: "%{y} rows<extra>" + v.value + "</extra>",
      }));
      if (data.truncated && data.distinct != null) {
        const otherCount = data.total - values.reduce((a, v) => a + v.count, 0);
        if (otherCount > 0) {
          tracesForCounts.push({
            x: ["other"],
            y: [otherCount],
            type: "bar",
            name: `other (${data.distinct - values.length} values)`,
            marker: { color: OTHER_COLOR },
            hovertemplate: "%{y} rows<extra>other</extra>",
          });
        }
      }
      return {
        traces: tracesForCounts,
        layout: {
          ...base,
          showlegend: tracesForCounts.length > 1,
          bargap: 0.75,
          xaxis: { ...base.xaxis, type: "category" },
          yaxis: { ...base.yaxis, title: { text: "rows" } },
        },
      };
    }

    // Trend: stacked bars, one trace per top value + other, 2px surface gap
    // between stacked segments via a surface-colored ring.
    const byTs = new Map<string, Record<string, number>>();
    for (const row of data.series ?? []) {
      const entry = byTs.get(row.ts) ?? {};
      entry[row.value] = (entry[row.value] ?? 0) + row.count;
      byTs.set(row.ts, entry);
    }
    const ts = [...byTs.keys()].sort();
    const cats = [...(data.top_values ?? [])];
    if (byTs.size > 0) {
      const other = [...byTs.values()].some((e) => "other" in e);
      if (other) cats.push("other");
    }
    const tracesForTrend: Data[] = cats.map((cat) => ({
      x: ts,
      y: ts.map((t) => byTs.get(t)?.[cat] ?? 0),
      type: "bar",
      name: cat === "other" ? "other" : cat,
      marker: {
        color: cat === "other" ? OTHER_COLOR : valueColors.get(cat) ?? OTHER_COLOR,
        line: { width: 2, color: ink.surface },
      },
      hovertemplate: "%{y} rows<extra>" + cat + "</extra>",
    }));
    return {
      traces: tracesForTrend,
      layout: {
        ...base,
        barmode: "stack",
        showlegend: tracesForTrend.length > 1,
        xaxis: { ...base.xaxis, type: "date", rangeslider: { visible: true } },
        yaxis: { ...base.yaxis, title: { text: "rows / bucket" } },
      },
    };
  }, [data, ink, valueColors]);

  if (!dataset) {
    return (
      <div className="py-24 text-center">
        <p className="text-muted-foreground mb-4">
          Select a dataset to chart its columns.
        </p>
        {onOpenDatasetDialog && (
          <Button onClick={onOpenDatasetDialog}>Choose dataset</Button>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-4">
        <Select value={column || undefined} onValueChange={setColumn}>
          <SelectTrigger className="w-[220px]">
            <SelectValue placeholder="Pick a column" />
          </SelectTrigger>
          <SelectContent>
            {columns.map((c) => (
              <SelectItem key={c} value={c}>
                {c}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <div className="flex items-center rounded-lg border p-1 gap-1">
          <Button
            variant={mode === "counts" ? "default" : "ghost"}
            size="sm"
            onClick={() => setMode("counts")}
          >
            Counts
          </Button>
          <Button
            variant={mode === "trend" ? "default" : "ghost"}
            size="sm"
            onClick={() => setMode("trend")}
          >
            Trend
          </Button>
        </div>

        <Select value={String(limit)} onValueChange={(v) => setLimit(Number(v))}>
          <SelectTrigger className="w-[100px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LIMIT_OPTIONS.map((l) => (
              <SelectItem key={l} value={String(l)}>
                top {l}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        {mode === "trend" && (
          <>
            <Select value={bucket} onValueChange={setBucket}>
              <SelectTrigger className="w-[160px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {BUCKET_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={o.value}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={String(minutes)} onValueChange={(v) => setMinutes(Number(v))}>
              <SelectTrigger className="w-[160px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {WINDOW_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={String(o.value)}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </>
        )}
      </div>

      {error && (
        <div className="flex items-center justify-between gap-4 rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm">
          <span className="flex items-center gap-2 text-destructive">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            <span className="truncate">{error}</span>
          </span>
          <button
            className="text-destructive underline"
            onClick={() => setRetryNonce((n) => n + 1)}
          >
            Retry
          </button>
        </div>
      )}

      {data?.mode === "counts" && (
        <div className="flex gap-8 text-sm">
          <div>
            <div className="text-muted-foreground">total rows</div>
            <div className="text-xl font-semibold">{data.total.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-muted-foreground">distinct values</div>
            <div className="text-xl font-semibold">
              {data.distinct != null ? data.distinct.toLocaleString() : "—"}
            </div>
          </div>
        </div>
      )}

      <Card>
        <CardContent className="pt-6">
          {loading && !data ? (
            <Skeleton className="h-[460px] w-full" />
          ) : data && traces.length > 0 ? (
            <PlotlyChart data={traces} layout={layout} />
          ) : data && traces.length === 0 ? (
            <div className="py-24 text-center text-sm text-muted-foreground">
              No rows with a <code>_created_at</code> timestamp for column{" "}
              <span className="font-medium">{column}</span>. Rows ingested
              before the timestamp field was added are not charted.
            </div>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}

export default ChartsTab;
