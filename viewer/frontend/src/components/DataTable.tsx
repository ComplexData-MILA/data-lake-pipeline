import { useEffect, useState, useMemo } from "react";
import { fetchData, fetchCount } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
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
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, ArrowUpDown, ArrowUp, ArrowDown } from "lucide-react";

export function DataTable() {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [totalCount, setTotalCount] = useState(0);

  const {
    dataset,
    page,
    pageSize,
    baseColumns,
    annotators,
    annotatorColumns,
    filters,
    sort,
    sortDir,
    setPage,
    setPageSize,
    setSort,
    setSelectedId,
  } = useViewerStore();

  const displayColumns = useMemo(() => {
    const cols: string[] = [];

    if (baseColumns.length > 0) {
      cols.push(...baseColumns);
    } else if (rows.length > 0) {
      const rowCols = Object.keys(rows[0]).filter(c => c !== "_batch");
      cols.push(...rowCols);
    } else {
      cols.push("id", "_batch");
    }

    annotators.forEach((ann) => {
      const annCols = annotatorColumns[ann] || [];
      annCols.forEach((col) => {
        cols.push(`${ann}.${col}`);
      });
    });

    return cols;
  }, [baseColumns, annotators, annotatorColumns, rows]);

  useEffect(() => {
    if (!dataset) {
      setRows([]);
      setTotalCount(0);
      return;
    }
    setLoading(true);
    Promise.all([
      fetchData(dataset, {
        page,
        pageSize,
        columns: baseColumns.length > 0 ? baseColumns : ["id", "_batch"],
        annotators,
        annotatorColumns,
        filters,
        sort,
        sortDir,
      }),
      fetchCount(dataset, annotators, filters),
    ])
      .then(([data, count]) => {
        setRows(data.rows);
        setTotalCount(count);
      })
      .catch((err) => console.error("Failed to load data:", err))
      .finally(() => setLoading(false));
  }, [dataset, page, pageSize, baseColumns, annotators, annotatorColumns, filters, sort, sortDir]);

  const totalPages = Math.ceil(totalCount / pageSize);
  const startIdx = (page - 1) * pageSize + 1;
  const endIdx = Math.min(page * pageSize, totalCount);

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
        <div className="rounded-md border">
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
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              {displayColumns.map((col) => (
                <TableHead
                  key={col}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() => handleSort(col)}
                >
                  {col}
                  {getSortIcon(col)}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 ? (
              <TableRow>
                <TableCell colSpan={displayColumns.length} className="text-center py-8">
                  No data found
                </TableCell>
              </TableRow>
            ) : (
              rows.map((row, idx) => (
                <TableRow
                  key={row.id as string || idx}
                  className="cursor-pointer"
                  onClick={() => setSelectedId(row.id as string)}
                >
                  {displayColumns.map((col) => (
                    <TableCell key={col} className="truncate">
                      {formatCellValue(row[col])}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
        <div className="text-sm text-muted-foreground">
          {loading ? (
            <Skeleton className="h-4 w-32" />
          ) : (
            `Showing ${startIdx}-${endIdx} of ${totalCount} rows`
          )}
        </div>

        <div className="flex items-center gap-2">
          <Select
            value={String(pageSize)}
            onValueChange={(value) => setPageSize(Number(value))}
            disabled={loading}
          >
            <SelectTrigger className="w-[80px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="25">25</SelectItem>
              <SelectItem value="50">50</SelectItem>
              <SelectItem value="100">100</SelectItem>
            </SelectContent>
          </Select>

          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon"
              onClick={() => setPage(1)}
              disabled={page <= 1 || loading}
            >
              <ChevronsLeft className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              onClick={() => setPage(page - 1)}
              disabled={page <= 1 || loading}
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <span className="text-sm px-2">
              Page {page} of {totalPages || 1}
            </span>
            <Button
              variant="outline"
              size="icon"
              onClick={() => setPage(page + 1)}
              disabled={page >= totalPages || loading}
            >
              <ChevronRight className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              onClick={() => setPage(totalPages)}
              disabled={page >= totalPages || loading}
            >
              <ChevronsRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
