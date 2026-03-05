/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/** Status color and label utilities for Monarch actor statuses. */

const STATUS_COLORS: Record<string, string> = {
  idle: "var(--status-healthy)",
  processing: "var(--status-processing)",
  created: "var(--status-transitional)",
  initializing: "var(--status-transitional)",
  saving: "var(--status-transitional)",
  loading: "var(--status-transitional)",
  stopping: "var(--status-transitional)",
  failed: "var(--status-failed)",
  stopped: "var(--status-stopped)",
  unknown: "var(--status-unknown)",
  client: "var(--status-unknown)",
};

/** Map a status string to its CSS color variable. */
export function statusColor(status: string | null | undefined): string {
  if (!status) return "var(--text-muted)";
  return STATUS_COLORS[status.toLowerCase()] ?? "var(--text-muted)";
}

/** Status priority for "worst status" aggregation. Higher = worse. */
const STATUS_PRIORITY: Record<string, number> = {
  idle: 0,
  processing: 1,
  client: 2,
  unknown: 3,
  created: 4,
  initializing: 4,
  saving: 4,
  loading: 4,
  stopping: 5,
  stopped: 6,
  failed: 7,
};

/** Return the worst (highest priority) status from a list. */
export function worstStatus(statuses: (string | null | undefined)[]): string {
  let worst = "idle";
  let worstPri = -1;
  for (const s of statuses) {
    if (!s) continue;
    const pri = STATUS_PRIORITY[s.toLowerCase()] ?? 3;
    if (pri > worstPri) {
      worstPri = pri;
      worst = s.toLowerCase();
    }
  }
  return worst;
}

/** Format a microsecond timestamp to a readable string. */
export function formatTimestamp(us: number): string {
  const ms = us / 1000;
  const d = new Date(ms);
  return d.toISOString().replace("T", " ").replace("Z", "").slice(0, 23);
}

/** Parse shape_json into a readable dims string like "[2, 4]". */
export function formatShape(shapeJson: string): string {
  try {
    const parsed = JSON.parse(shapeJson);
    if (parsed.dims) return `[${parsed.dims.join(", ")}]`;
    return shapeJson;
  } catch {
    return shapeJson;
  }
}

/** Message status color. */
export function messageStatusColor(status: string): string {
  switch (status.toLowerCase()) {
    case "delivered": return "var(--status-healthy)";
    case "sent": return "var(--status-processing)";
    case "queued": return "var(--status-transitional)";
    case "failed": return "var(--status-failed)";
    default: return "var(--text-muted)";
  }
}
