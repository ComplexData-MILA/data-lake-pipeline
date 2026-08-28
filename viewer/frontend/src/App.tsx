import { useState, useEffect, lazy, Suspense } from "react";
import { DatasetSelector } from "@/components/DatasetSelector";
import { DatasetSelectionDialog } from "@/components/DatasetSelectionDialog";
import { ColumnConfigurator } from "@/components/ColumnConfigurator";
import { FilterPanel } from "@/components/FilterPanel";
import { DataTable } from "@/components/DataTable";
import { RowDetailModal } from "@/components/RowDetailModal";
import { LiveStatusBadge } from "@/components/LiveStatusBadge";
import { NewRowsBanner } from "@/components/NewRowsBanner";
import { ConversionBanner } from "@/components/ConversionBanner";
import { useViewerStore } from "@/hooks/useUrlState";
import { useLiveEvents } from "@/hooks/useLiveEvents";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import { RefreshCw, Sun, Moon, Monitor } from "lucide-react";
import type { ThemeMode, TabId } from "@/types";

// Chart tabs are lazy-loaded so plotly.js stays out of the Data tab's bundle.
const ActivityTab = lazy(() => import("@/components/ActivityTab"));
const ChartsTab = lazy(() => import("@/components/ChartsTab"));

const THEME_ICON: Record<ThemeMode, typeof Sun> = {
  light: Sun,
  dark: Moon,
  system: Monitor,
};

const NEXT_THEME: Record<ThemeMode, ThemeMode> = {
  light: "dark",
  dark: "system",
  system: "light",
};

function TabFallback() {
  return (
    <div className="py-12 text-center text-muted-foreground">
      <Skeleton className="h-64 w-full max-w-4xl mx-auto" />
    </div>
  );
}

export function App() {
  const { dataset, tab, theme, setTab, setTheme, reset } = useViewerStore();
  const [datasetDialogOpen, setDatasetDialogOpen] = useState(false);

  // Per-dataset stream (feeds the rows banner + Data tab refetch) and a
  // global stream (activity/charts liveness; no row counting to avoid
  // double-counting the pending-rows banner).
  useLiveEvents(dataset || null);
  useLiveEvents("", { countRows: false });

  useEffect(() => {
    if (!dataset && tab !== "activity") {
      setDatasetDialogOpen(true);
    }
  }, [dataset, tab]);

  // Apply the theme class (light/dark/system) to <html>.
  useEffect(() => {
    const root = document.documentElement;
    const apply = () => {
      const dark =
        theme === "dark" ||
        (theme === "system" &&
          window.matchMedia("(prefers-color-scheme: dark)").matches);
      root.classList.toggle("dark", dark);
    };
    apply();
    if (theme !== "system") return;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
  }, [theme]);

  const ThemeIcon = THEME_ICON[theme];

  return (
    <div className="container mx-auto py-8 px-4">
      <Tabs value={tab} onValueChange={(v) => setTab(v as TabId)}>
        <div className="flex flex-wrap items-center gap-3 mb-2">
          <h1 className="text-3xl font-bold">Data Lake Dashboard</h1>
          <TabsList className="mx-2">
            <TabsTrigger value="data">Data</TabsTrigger>
            <TabsTrigger value="activity">Activity</TabsTrigger>
            <TabsTrigger value="charts">Charts</TabsTrigger>
          </TabsList>
          {dataset && <LiveStatusBadge />}
          <Button
            variant="ghost"
            size="icon"
            className="ml-auto"
            title={`Theme: ${theme} (click to switch)`}
            onClick={() => setTheme(NEXT_THEME[theme])}
          >
            <ThemeIcon className="h-4 w-4" />
          </Button>
        </div>
        <p className="text-muted-foreground mb-6">
          Browse, monitor, and chart your S3-hosted datasets
        </p>

        {dataset && <NewRowsBanner />}
        {dataset && <ConversionBanner dataset={dataset} />}

        <TabsContent value="data" className="mt-4">
          {dataset && (
            <Card className="w-full mb-6">
              <CardContent className="pt-6">
                <div className="flex flex-wrap items-center gap-4">
                  <DatasetSelector onOpenChange={setDatasetDialogOpen} />
                  <ColumnConfigurator />
                  <FilterPanel />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={reset}
                    className="gap-1 ml-auto"
                  >
                    <RefreshCw className="h-4 w-4" />
                    <span className="hidden sm:inline">Reset</span>
                  </Button>
                </div>
              </CardContent>
            </Card>
          )}
          <main className="w-full">
            <DataTable />
          </main>
        </TabsContent>

        <TabsContent value="activity" className="mt-4">
          <Suspense fallback={<TabFallback />}>
            <ActivityTab />
          </Suspense>
        </TabsContent>

        <TabsContent value="charts" className="mt-4">
          <Suspense fallback={<TabFallback />}>
            <ChartsTab onOpenDatasetDialog={() => setDatasetDialogOpen(true)} />
          </Suspense>
        </TabsContent>
      </Tabs>

      <RowDetailModal />

      <DatasetSelectionDialog
        open={datasetDialogOpen}
        onOpenChange={setDatasetDialogOpen}
      />
    </div>
  );
}
