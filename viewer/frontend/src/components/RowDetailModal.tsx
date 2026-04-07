import { useEffect, useMemo, useState } from "react";
import { fetchRow } from "@/lib/api";
import { useViewerStore } from "@/hooks/useUrlState";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Copy, Check, ExternalLink } from "lucide-react";
import { formatDateTime } from "@/lib/utils";

function TabButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2 text-sm font-medium rounded-t-lg border-b-2 transition-colors ${
        active
          ? "border-primary bg-background text-foreground"
          : "border-transparent text-muted-foreground hover:text-foreground hover:bg-muted/50"
      }`}
    >
      {children}
    </button>
  );
}

function FieldRow({
  label,
  value,
}: {
  label: string;
  value: React.ReactNode;
}) {
  return (
    <div className="grid grid-cols-3 gap-2 py-2 border-b last:border-b-0">
      <dt className="font-medium text-muted-foreground text-sm">{label}</dt>
      <dd className="col-span-2 text-sm break-all">{value}</dd>
    </div>
  );
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  return (
    <Button variant="ghost" size="icon" className="h-6 w-6" onClick={handleCopy}>
      {copied ? <Check className="h-3 w-3 text-green-600" /> : <Copy className="h-3 w-3" />}
    </Button>
  );
}

function renderValue(value: unknown): React.ReactNode {
  if (value === null || value === undefined) {
    return <span className="text-muted-foreground">—</span>;
  }
  if (typeof value === "object") {
    return <code className="text-xs bg-muted px-1 rounded">{JSON.stringify(value)}</code>;
  }
  return String(value);
}

export function RowDetailModal() {
  const [row, setRow] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [activeTab, setActiveTab] = useState<"fields" | "json">("fields");
  const {
    dataset,
    selectedId,
    setSelectedId,
    baseColumns,
    annotators,
    annotatorColumns,
  } = useViewerStore();

  useEffect(() => {
    if (!dataset || !selectedId) {
      setRow(null);
      return;
    }
    setLoading(true);
    setError(null);
    fetchRow(dataset, selectedId, {
      columns: baseColumns.length > 0 ? baseColumns : ["id", "_batch"],
      annotators,
      annotatorColumns,
    })
      .then((res) => setRow(res.rows[0]))
      .catch((err) => {
        console.error("Failed to load row:", err);
        setError(err as Error);
        setRow(null);
      })
      .finally(() => setLoading(false));
  }, [dataset, selectedId, baseColumns, annotators, annotatorColumns]);

  const displayColumns = useMemo(() => {
    const cols: string[] = [];

    if (baseColumns.length > 0) {
      cols.push(...baseColumns);
    } else if (row) {
      const rowCols = Object.keys(row).filter(c => !c.includes(".") && c !== "_batch");
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
  }, [baseColumns, annotators, annotatorColumns, row]);

  const handleClose = () => {
    setSelectedId(null);
    setRow(null);
    setError(null);
  };

  const isUrl = (value: unknown): value is string => {
    return typeof value === "string" && (value.startsWith("http://") || value.startsWith("https://"));
  };

  return (
    <Dialog open={!!selectedId} onOpenChange={(open) => !open && handleClose()}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>
            Row: <code className="text-sm font-normal text-muted-foreground">{selectedId}</code>
          </DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-auto">
          <div className="flex gap-1 mb-4 border-b">
            <TabButton active={activeTab === "fields"} onClick={() => setActiveTab("fields")}>
              Fields
            </TabButton>
            <TabButton active={activeTab === "json"} onClick={() => setActiveTab("json")}>
              JSON
            </TabButton>
          </div>

          {loading && (
            <div className="text-center py-8 text-muted-foreground">Loading...</div>
          )}

          {error && (
            <div className="text-center py-8 text-destructive">
              Error: {error.message}
            </div>
          )}

          {row && activeTab === "fields" && (
            <dl className="divide-y">
              {displayColumns.map((col) => {
                const value = row[col];
                return (
                  <FieldRow
                    key={col}
                    label={col}
                    value={
                      isUrl(value) ? (
                        <a
                          href={value}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:underline flex items-center gap-1"
                        >
                          {value}
                          <ExternalLink className="h-3 w-3" />
                        </a>
                      ) : col.toLowerCase().includes("datetime") ||
                         col.toLowerCase().includes("date") ||
                         col.toLowerCase().includes("time")
                        ? formatDateTime(value as string)
                        : renderValue(value)
                    }
                  />
                );
              })}
              {displayColumns.length === 0 && (
                <div className="py-4 text-center text-muted-foreground">No fields available</div>
              )}
            </dl>
          )}

          {row && activeTab === "json" && (
            <JsonView row={row} />
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

function JsonView({ row }: { row: Record<string, unknown> }) {
  const jsonString = JSON.stringify(row, null, 2);

  return (
    <div className="relative">
      <div className="absolute top-2 right-2 z-10">
        <CopyButton text={jsonString} />
      </div>
      <pre className="text-xs bg-muted p-4 rounded-md overflow-auto max-h-96 font-mono">
        {jsonString}
      </pre>
    </div>
  );
}