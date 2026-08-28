import { useEffect, useMemo, useState } from "react";
import type { Data, Layout } from "plotly.js";
import { fetchActivity } from "@/lib/api";
import type { ActivityResponse } from "@/types";
import { useLiveStore } from "@/hooks/useLiveStore";
import { useDarkMode } from "@/hooks/useDarkMode";
import { useViewerStore } from "@/hooks/useUrlState";
import { chartInk, OTHER_COLOR, SERIES_CAP } from "@/lib/chartTheme";
import { PlotlyChart } from "@/components/charts/PlotlyChart";
import { Card, CardContent } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { AlertTriangle } from "lucide-react";

const BUCKET_OPTIONS = [
  { value: "1m", label: "per minute" },
  { value: "5m", label: "per 5 minutes" },
  { value: "1h", label: "per hour" },
] as const;

const WINDOW_OPTIONS = [
  { value: 60, label: "last hour" },
  { value: 240, label: "last 4 hours" },
  { value: 1440, label: "last 24 hours" },
  { value: 10080, label: "last 7 days" },
  { value: 43200, label: "last 30 days" },
] as const;

export function ActivityTab() {
  const dark = useDarkMode();
  const ink = chartInk(dark);
  const refreshNonce = useLiveStore((s) => s.refreshNonce);
  const selectedDataset = useViewerStore((s) => s.dataset);

  const [bucket, setBucket] = useState<string>("1m");
  const [minutes, setMinutes] = useState<number>(1440);
  const [cumulative, setCumulative] = useState(false);
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const [data, setData] = useState<ActivityResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryNonce, setRetryNonce] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    // Debounce: SSE bursts trigger a single refetch.
    const t = setTimeout(() => {
      fetchActivity(bucket, minutes)
        .then((d) => {
          if (!cancelled) {
            setData(d);
            setError(null);
          }
        })
        .catch((e) => {
          if (!cancelled) {
            setError(e instanceof Error ? e.message : "Failed to load activity");
          }
        })
        .finally(() => {
          if (!cancelled) setLoading(false);
        });
    }, 300);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [bucket, minutes, refreshNonce, retryNonce]);

  // Dataset name -> stable color slot (index in the sorted full list).
  const { traces, chips } = useMemo(() => {
    if (!data) return { traces: [] as Data[], chips: [] as Chips };
    const sorted = [...data.datasets].sort((a, b) =>
      a.dataset.localeCompare(b.dataset)
    );
    const totals = new Map(
      sorted.map((d) => [
        d.dataset,
        d.buckets.reduce((acc, b) => acc + b.count, 0),
      ])
    );
    const colorSlot = new Map(sorted.map((d, i) => [d.dataset, i]));
    const primaries = sorted
      .filter((d) => totals.get(d.dataset)! > 0)
      .sort((a, b) => totals.get(b.dataset)! - totals.get(a.dataset)!)
      .slice(0, SERIES_CAP)
      .map((d) => d.dataset);
    const primarySet = new Set(primaries);

    const visible = sorted.filter((d) => !hidden.has(d.dataset));
    const otherDatasets = visible.filter((d) => !primarySet.has(d.dataset));
    const hasOther = otherDatasets.length > 0;

    const makeTrace = (d: (typeof sorted)[number]): Data => {
      let y = d.buckets.map((b) => b.count);
      if (cumulative) {
        let acc = 0;
        y = d.buckets.map((b) => (acc += b.count));
      }
      const isSelected = d.dataset === selectedDataset;
      return {
        x: d.buckets.map((b) => b.ts),
        y,
        type: "scatter",
        mode: "lines",
        name: d.dataset,
        line: {
          color: ink.palette[colorSlot.get(d.dataset)! % SERIES_CAP],
          width: isSelected ? 3 : 2,
        },
        hovertemplate: "%{y} rows<extra>" + d.dataset + "</extra>",
      };
    };

    const out: Data[] = visible.filter((d) => primarySet.has(d.dataset)).map(makeTrace);
    if (hasOther) {
      // Aggregate the remainder into one "other" trace (never more than
      // SERIES_CAP hues — color follows the entity, overflow folds).
      const byTs = new Map<string, number>();
      for (const d of otherDatasets) {
        for (const b of d.buckets) {
          byTs.set(b.ts, (byTs.get(b.ts) ?? 0) + b.count);
        }
      }
      const ts = [...byTs.keys()].sort();
      let acc = 0;
      const y = ts.map((t) => {
        const v = byTs.get(t)!;
        return cumulative ? (acc += v) : v;
      });
      out.push({
        x: ts,
        y,
        type: "scatter",
        mode: "lines",
        name: `other (${otherDatasets.length})`,
        line: { color: OTHER_COLOR, width: 2 },
        hovertemplate: "%{y} rows<extra>other</extra>",
      });
    }

    const chips: Chips = sorted
      .filter((d) => totals.get(d.dataset)! > 0 || d.dataset === selectedDataset)
      .map((d) => ({
        dataset: d.dataset,
        total: totals.get(d.dataset) ?? 0,
        color: ink.palette[colorSlot.get(d.dataset)! % SERIES_CAP],
        isSelected: d.dataset === selectedDataset,
      }));
    return { traces: out, chips };
  }, [data, hidden, cumulative, selectedDataset, ink]);

  const layout = useMemo((): Partial<Layout> => {
    return {
      height: 480,
      margin: { l: 56, r: 16, t: 8, b: 40 },
      font: {
        family: 'system-ui, -apple-system, "Segoe UI", sans-serif',
        color: ink.ink,
      },
      paper_bgcolor: ink.paper,
      plot_bgcolor: ink.plot,
      hovermode: "x unified",
      xaxis: {
        type: "date",
        gridcolor: ink.grid,
        zerolinecolor: ink.axis,
        linecolor: ink.axis,
        tickfont: { color: ink.secondary },
        rangeslider: { visible: true },
      },
      yaxis: {
        title: { text: cumulative ? "cumulative rows" : "rows / bucket" },
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
      showlegend: traces.length > 1,
    };
  }, [ink, cumulative, traces.length]);

  const toggleDataset = (name: string) => {
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const hasAnyRows = data?.datasets.some((d) => d.buckets.length > 0) ?? false;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-4">
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
        <label className="flex items-center gap-2 text-sm text-muted-foreground">
          <input
            type="checkbox"
            checked={cumulative}
            onChange={(e) => setCumulative(e.target.checked)}
            className="accent-foreground"
          />
          cumulative
        </label>
        <span className="text-xs text-muted-foreground ml-auto">
          rows per dataset over time · click legend or chips to toggle datasets
        </span>
      </div>

      {chips.length > 1 && (
        <div className="flex flex-wrap gap-2">
          {chips.map((chip) => (
            <button
              key={chip.dataset}
              onClick={() => toggleDataset(chip.dataset)}
              className={
                "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs transition-colors " +
                (hidden.has(chip.dataset)
                  ? "border-border text-muted-foreground opacity-60"
                  : "border-border bg-muted/40 text-foreground") +
                (chip.isSelected ? " font-semibold" : "")
              }
              title={`${chip.dataset} — ${chip.total.toLocaleString()} rows in window`}
            >
              <span
                className="inline-block h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: chip.color }}
              />
              {chip.dataset}
              <span className="text-muted-foreground">
                {chip.total.toLocaleString()}
              </span>
            </button>
          ))}
        </div>
      )}

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

      <Card>
        <CardContent className="pt-6">
          {loading && !data ? (
            <Skeleton className="h-[480px] w-full" />
          ) : data && hasAnyRows ? (
            <PlotlyChart
              data={traces}
              layout={layout}
              onLegendClick={(evt) => {
                // A legend click already toggles the trace in plotly; sync
                // the chip state so both controls stay in agreement. The
                // "other" aggregate has no chip, so leave it to plotly.
                const name = evt.data[0]?.name;
                if (name && !name.startsWith("other (")) {
                  toggleDataset(name);
                }
              }}
            />
          ) : data && !hasAnyRows ? (
            <div className="py-24 text-center text-sm text-muted-foreground">
              No rows with a <code>_created_at</code> timestamp in this window.
              <br />
              Rows ingested before the timestamp field was added are not
              charted.
            </div>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}

type Chips = Array<{
  dataset: string;
  total: number;
  color: string;
  isSelected: boolean;
}>;

export default ActivityTab;
