import { useLiveStore } from "@/hooks/useLiveStore";
import type { LiveStatus } from "@/hooks/useLiveStore";

const STATUS_LABEL: Record<LiveStatus, string> = {
  connecting: "Connecting…",
  connected: "Live",
  reconnecting: "Reconnecting…",
  offline: "Offline",
};

const STATUS_DOT: Record<LiveStatus, string> = {
  connecting: "bg-amber-400",
  connected: "bg-green-500 live-pulse",
  reconnecting: "bg-amber-400 animate-pulse",
  offline: "bg-muted-foreground/40",
};

export function LiveStatusBadge() {
  const status = useLiveStore((s) => s.status);
  const lastEventAt = useLiveStore((s) => s.lastEventAt);

  const tooltip =
    lastEventAt != null
      ? `Last event: ${new Date(lastEventAt).toLocaleTimeString()}`
      : "Live updates from the ingestion pipeline";

  return (
    <span
      title={tooltip}
      className="inline-flex items-center gap-1.5 rounded-full border bg-background px-2.5 py-1 text-xs font-medium text-muted-foreground"
    >
      <span
        className={`h-2 w-2 rounded-full ${STATUS_DOT[status]}`}
        aria-hidden
      />
      {STATUS_LABEL[status]}
    </span>
  );
}
