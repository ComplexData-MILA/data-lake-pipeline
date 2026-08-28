import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { fetchData, fetchCount } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import { useLiveStore } from "@/hooks/useLiveStore";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { ArrowUpDown, ArrowUp, ArrowDown, AlertTriangle, SearchX } from "lucide-react";

// URL detection regex - matches http, https, ftp URLs
const URL_REGEX = /(https?:\/\/[^\s<>"{}|\\^`[\]]+)/gi;

// Minimum column width in pixels
const MIN_COLUMN_WIDTH = 80;
// Maximum column width is 30% of viewport
const MAX_COLUMN_WIDTH_RATIO = 0.3;
const CHAR_WIDTH_ESTIMATE = 8; // approximate pixel width per character

interface ColumnWidthConfig {
  minWidth: number;
  maxWidth: number;
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function detectUrls(text: string): Array<{ type: 'text' | 'url'; content: string }> {
  const parts: Array<{ type: 'text' | 'url'; content: string }> = [];
  let lastIndex = 0;
  let match;

  // Reset regex lastIndex
  URL_REGEX.lastIndex = 0;

  while ((match = URL_REGEX.exec(text)) !== null) {
    // Add text before URL
    if (match.index > lastIndex) {
      parts.push({
        type: 'text',
        content: text.slice(lastIndex, match.index)
      });
    }
    // Add URL
    parts.push({
      type: 'url',
      content: match[0]
    });
    lastIndex = match.index + match[0].length;
  }

  // Add remaining text
  if (lastIndex < text.length) {
    parts.push({
      type: 'text',
      content: text.slice(lastIndex)
    });
  }

  return parts.length > 0 ? parts : [{ type: 'text', content: text }];
}

function renderCellContent(value: unknown): React.ReactNode {
  const text = formatCellValue(value);

  // Check if text contains URLs
  const parts = detectUrls(text);

  // If no URLs found or text is unchanged, return as-is
  if (parts.length === 1 && parts[0].type === 'text') {
    return text;
  }

  // Render mixed content with links
  return (
    <>
      {parts.map((part, idx) =>
        part.type === 'url' ? (
          <a
            key={idx}
            href={part.content}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 hover:text-blue-800 underline truncate inline-block max-w-full"
            onClick={(e) => e.stopPropagation()}
          >
            {part.content}
          </a>
        ) : (
          <span key={idx}>{part.content}</span>
        )
      )}
    </>
  );
}

function calculateColumnWidths(
  rows: Record<string, unknown>[],
  columns: string[],
  config: ColumnWidthConfig = { minWidth: MIN_COLUMN_WIDTH, maxWidth: Infinity }
): Record<string, number> {
  const widths: Record<string, number> = {};

  columns.forEach(col => {
    let maxLength = col.length; // Start with header length

    rows.forEach(row => {
      const value = row[col];
      const text = formatCellValue(value);
      maxLength = Math.max(maxLength, text.length);
    });

    // Calculate pixel width with constraints
    const pixelWidth = Math.min(
      Math.max(maxLength * CHAR_WIDTH_ESTIMATE, config.minWidth),
      config.maxWidth
    );
    widths[col] = pixelWidth;
  });

  return widths;
}

function parseAnnotatorColumn(col: string): { annotator: string; column: string } | null {
  const dotIndex = col.indexOf(".");
  if (dotIndex === -1) return null;
  return {
    annotator: col.slice(0, dotIndex),
    column: col.slice(dotIndex + 1),
  };
}

function renderColumnHeader(col: string): React.ReactNode {
  const parsed = parseAnnotatorColumn(col);
  if (!parsed) {
    return col;
  }
  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbPage className="font-medium">{parsed.annotator}</BreadcrumbPage>
        </BreadcrumbItem>
        <BreadcrumbSeparator />
        <BreadcrumbItem>
          <BreadcrumbPage>{parsed.column}</BreadcrumbPage>
        </BreadcrumbItem>
      </BreadcrumbList>
    </Breadcrumb>
  );
}

// System columns hidden from the default (all-columns) view; both remain
// selectable explicitly via the column configurator.
const SYSTEM_COLUMNS = ["_batch", "_created_at"];

export function DataTable() {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [responseColumns, setResponseColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false); // first page
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [totalCount, setTotalCount] = useState(0);
  const [retryNonce, setRetryNonce] = useState(0);
  // Infinite-scroll state: cursor chaining on the keyset/ordering paths,
  // page numbers on the scan fallback.
  const [keysetMode, setKeysetMode] = useState(false);
  const [hasMore, setHasMore] = useState(false);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [scanPage, setScanPage] = useState(1);
  const [viewportWidth, setViewportWidth] = useState(typeof window !== "undefined" ? window.innerWidth : 1200);

  const rowsRef = useRef(rows);
  rowsRef.current = rows;
  const scrollRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleResize = () => setViewportWidth(window.innerWidth);
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const refreshNonce = useLiveStore((s) => s.refreshNonce);

  const {
    dataset,
    pageSize,
    baseColumns,
    annotators,
    annotatorColumns,
    filters,
    sort,
    sortDir,
    setPageSize,
    setSort,
    setSelectedId,
    resetFilters,
  } = useViewerStore();

  const displayColumns = useMemo(() => {
    const cols: string[] = [];

    if (baseColumns.length > 0) {
      cols.push(...baseColumns);
    } else {
      cols.push(...responseColumns.filter((c) => !SYSTEM_COLUMNS.includes(c)));
    }

    annotators.forEach((ann) => {
      const annCols = annotatorColumns[ann] || [];
      annCols.forEach((col) => {
        cols.push(`${ann}.${col}`);
      });
    });

    return cols;
  }, [baseColumns, responseColumns, annotators, annotatorColumns]);

  const columnWidths = useMemo(() => {
    const maxColWidth = viewportWidth * MAX_COLUMN_WIDTH_RATIO;
    return calculateColumnWidths(rows, displayColumns, {
      minWidth: MIN_COLUMN_WIDTH,
      maxWidth: maxColWidth,
    });
  }, [rows, displayColumns, viewportWidth]);

  const totalColumnsWidth = useMemo(
    () => displayColumns.reduce((acc, col) => acc + (columnWidths[col] ?? MIN_COLUMN_WIDTH), 0),
    [displayColumns, columnWidths]
  );

  // First page / full reset fetch.
  useEffect(() => {
    if (!dataset) {
      setRows([]);
      setTotalCount(0);
      setError(null);
      return;
    }
    // [] means "all columns" — the backend resolves the full base schema.
    const columnsParam = baseColumns;

    setLoading(true);
    setError(null);
    let cancelled = false;

    fetchData(dataset, {
      page: 1,
      pageSize,
      columns: columnsParam,
      annotatorColumns,
      filters,
      sort,
      sortDir,
      cursor: null,
    })
      .then((data) => {
        if (cancelled) return;
        setRows(data.rows);
        setResponseColumns(data.columns);
        // Keyset/ordering responses always carry has_more; the scan fallback
        // doesn't, so its absence switches to page-number chaining.
        const ks = data.has_more !== undefined && data.has_more !== null;
        setKeysetMode(ks);
        setHasMore(ks ? !!data.has_more : data.rows.length === pageSize);
        setNextCursor(data.next_cursor ?? null);
        setScanPage(1);
        scrollRef.current?.scrollTo({ top: 0 });
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load data");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    fetchCount(dataset, filters)
      .then((count) => {
        if (!cancelled) setTotalCount(count);
      })
      .catch(() => undefined);

    return () => {
      cancelled = true;
    };
  }, [dataset, pageSize, baseColumns, annotators, annotatorColumns, filters, sort, sortDir, refreshNonce, retryNonce]);

  const loadMore = useCallback(() => {
    if (!dataset || loading || loadingMore || !hasMore) return;
    const page = keysetMode ? 1 : scanPage + 1;
    const cursorParam = keysetMode ? nextCursor : null;
    let cancelled = false;
    setLoadingMore(true);
    fetchData(dataset, {
      page,
      pageSize,
      columns: baseColumns,
      annotatorColumns,
      filters,
      sort,
      sortDir,
      cursor: cursorParam,
    })
      .then((data) => {
        if (cancelled) return;
        setRows((prev) => [...prev, ...data.rows]);
        setNextCursor(data.next_cursor ?? null);
        setScanPage(page);
        if (keysetMode) {
          setHasMore(!!data.has_more);
        } else {
          const loaded = rowsRef.current.length + data.rows.length;
          setHasMore(
            data.rows.length === pageSize &&
              (totalCount === 0 || loaded < totalCount)
          );
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load more rows");
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingMore(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dataset, pageSize, baseColumns, annotatorColumns, filters, sort, sortDir, keysetMode, nextCursor, scanPage, loading, loadingMore, hasMore, totalCount]);

  // Sentinel at the bottom of the scroll container triggers the next page.
  useEffect(() => {
    const el = sentinelRef.current;
    const root = scrollRef.current;
    if (!el || !root) return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting)) loadMore();
      },
      { root, rootMargin: "800px" }
    );
    io.observe(el);
    return () => io.disconnect();
  }, [loadMore]);

  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 76,
    overscan: 12,
    measureElement: (el) => el.getBoundingClientRect().height,
  });
  const virtualRows = virtualizer.getVirtualItems();

  const handleSort = (column: string) => {
    if (sort === column) {
      setSort(column, sortDir === "asc" ? "desc" : "asc");
    } else {
      setSort(column, "asc");
    }
  };

  const getSortIcon = (column: string) => {
    if (sort !== column) return <ArrowUpDown className="h-3 w-3 ml-1 inline" />;
    if (sortDir === "asc") return <ArrowUp className="h-3 w-3 ml-1 inline" />;
    return <ArrowDown className="h-3 w-3 ml-1 inline" />;
  };

  const filterActive =
    !!filters.base || Object.values(filters.annotators ?? {}).some(Boolean);

  if (!dataset) {
    return (
      <div className="text-center py-12 text-muted-foreground">
        Select a dataset to view data
      </div>
    );
  }

  if (loading && rows.length === 0) {
    return (
      <div className="space-y-4">
        <div>
          <Table>
            <TableHeader>
              <TableRow>
                {Array.from({ length: 5 }).map((_, i) => (
                  <TableHead key={i}>
                    <Skeleton className="h-4 w-24" />
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {Array.from({ length: pageSize }).map((_, rowIdx) => (
                <TableRow key={rowIdx}>
                  {Array.from({ length: 5 }).map((_, colIdx) => (
                    <TableCell key={colIdx}>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
        <div className="flex items-center justify-center py-4">
          <Skeleton className="h-4 w-48" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {error && (
        <div className="flex items-center justify-between gap-4 rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm">
          <span className="flex items-center gap-2 text-destructive">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            <span className="truncate">{error}</span>
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setRetryNonce((n) => n + 1)}
          >
            Retry
          </Button>
        </div>
      )}
      <div ref={scrollRef} className="overflow-auto max-h-[75vh] rounded-md border">
        {rows.length === 0 && !loading ? (
          <div className="py-12 text-center">
            <SearchX className="mx-auto mb-2 h-6 w-6 text-muted-foreground" />
            <p className="text-muted-foreground">
              {filterActive ? "No rows match the current filters" : "No data found"}
            </p>
            {filterActive && (
              <Button variant="outline" size="sm" className="mt-3" onClick={resetFilters}>
                Clear filters
              </Button>
            )}
          </div>
        ) : (
          // Raw table elements (not the ui/Table wrapper): the wrapper adds
          // its own overflow container, which would break the virtualizer's
          // scroll container and the sticky header.
          <table
            className="w-full caption-bottom text-sm"
            style={{ tableLayout: "fixed", minWidth: totalColumnsWidth }}
          >
            <colgroup>
              {displayColumns.map((col) => (
                <col key={col} style={{ width: columnWidths[col] }} />
              ))}
            </colgroup>
            <thead className="[&_tr]:border-b sticky top-0 z-10 bg-card">
              <tr>
                {displayColumns.map((col) => (
                  <th
                    key={col}
                    className="h-12 px-4 text-left align-middle font-medium text-muted-foreground cursor-pointer hover:bg-muted/50"
                    onClick={() => handleSort(col)}
                  >
                    <span className="inline-flex items-center gap-1">
                      <span className="truncate">{renderColumnHeader(col)}</span>
                      {getSortIcon(col)}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody
              className="[&_tr:last-child]:border-0"
              style={{ height: virtualizer.getTotalSize(), position: "relative" }}
            >
              {virtualRows.map((vr) => {
                const row = rows[vr.index];
                return (
                  <tr
                    key={`${String(row.id)}:${String(row._batch)}`}
                    data-index={vr.index}
                    ref={virtualizer.measureElement}
                    className="border-b transition-colors hover:bg-muted/50 absolute top-0 left-0 w-full cursor-pointer"
                    style={{ transform: `translateY(${vr.start}px)` }}
                    onClick={() => setSelectedId(String(row.id))}
                  >
                    {displayColumns.map((col) => (
                      <td
                        key={col}
                        className="p-4 align-top"
                        style={{ width: columnWidths[col] }}
                      >
                        <div className="line-clamp-3">{renderCellContent(row[col])}</div>
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
        <div
          ref={sentinelRef}
          className="h-10 flex items-center justify-center text-xs text-muted-foreground"
        >
          {loadingMore ? (
            <Skeleton className="h-4 w-32" />
          ) : !hasMore && rows.length > 0 ? (
            `All ${rows.length.toLocaleString()} rows loaded`
          ) : (
            "Scroll for more"
          )}
        </div>
      </div>

      <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
        <div className="text-sm text-muted-foreground">
          {loading ? (
            <Skeleton className="h-4 w-32" />
          ) : (
            <>
              Loaded {rows.length.toLocaleString()} rows
              {totalCount > 0 && (
                <span className="ml-2 text-xs opacity-70">
                  of ~{totalCount.toLocaleString()}
                </span>
              )}
              <span className="ml-2 text-xs opacity-70">
                live data — counts may shift as rows arrive
              </span>
            </>
          )}
        </div>

        <div className="flex items-center gap-2">
          <Select
            value={String(pageSize)}
            onValueChange={(value) => setPageSize(Number(value))}
            disabled={loading}
          >
            <SelectTrigger className="w-[110px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="25">25 / batch</SelectItem>
              <SelectItem value="50">50 / batch</SelectItem>
              <SelectItem value="100">100 / batch</SelectItem>
              <SelectItem value="250">250 / batch</SelectItem>
              <SelectItem value="500">500 / batch</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  );
}
