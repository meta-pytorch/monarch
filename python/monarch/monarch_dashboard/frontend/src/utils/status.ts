/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/** Status color and label utilities for Monarch actor statuses. */

export const STATUS_COLORS: Record<string, string> = {
  idle: "var(--status-idle)",
  client: "var(--status-client)",
  processing: "var(--status-processing)",
  saving: "var(--status-saving)",
  loading: "var(--status-loading)",
  created: "var(--status-created)",
  initializing: "var(--status-initializing)",
  stopping: "var(--status-stopping)",
  failed: "var(--status-failed)",
  stopped: "var(--status-stopped)",
  unknown: "var(--status-unknown)",
};

/** Map a status string to its CSS color variable. */
export function statusColor(status: string | null | undefined): string {
  if (!status) return "var(--text-muted)";
  return STATUS_COLORS[status.toLowerCase()] ?? "var(--text-muted)";
}

/** Format a microsecond timestamp to a readable string. */
export function formatTimestamp(us: number | null | undefined): string {
  if (us == null || isNaN(us)) return "—";
  const ms = us / 1000;
  const d = new Date(ms);
  if (isNaN(d.getTime())) return "—";
  return d.toISOString().replace("T", " ").replace("Z", "").slice(0, 23);
}

/** Parse shape_json into a readable dims string like "[2, 4]".
 *  Handles both the real ndslice Extent format
 *  ``{"inner": {"labels": ["workers"], "sizes": [2]}}`` and the legacy
 *  ``{"dims": [2]}`` format.
 */
export function formatShape(shapeJson: string): string {
  try {
    const parsed = JSON.parse(shapeJson);
    if (parsed.inner?.sizes) return `[${parsed.inner.sizes.join(", ")}]`;
    if (parsed.dims) return `[${parsed.dims.join(", ")}]`;
    return shapeJson;
  } catch {
    return shapeJson;
  }
}

/** Extract the last segment from a hierarchical name.
 *  Handles both ``/`` (fake data) and ``,`` (real data) separators.
 *  e.g. "host_mesh_0/proc_mesh_0/Trainer" → "Trainer"
 */
export function leafName(name: string | null | undefined): string {
  if (!name) return "—";
  return name.split("/").pop()!.split(",").pop()!;
}

/** Format a host/proc agent name as "Host Unit {rank}" or "Proc Unit {rank}".
 *  Returns null if the name doesn't match a host/proc agent pattern,
 *  so the caller can fall back to the default display.
 */
export function agentDisplayName(fullName: string | null | undefined, rank?: number | string | null): string | null {
  if (!fullName) return null;
  const low = fullName.toLowerCase();
  const isHost = low.includes("hostagent") || low.includes("host_agent");
  const isProcAgent = low.includes("procagent") || low.includes("proc_agent");
  if (!isHost && !isProcAgent) return null;
  const label = isHost ? "Host Unit" : "Proc Unit";
  if (rank != null) return `${label} ${rank}`;
  // Try to extract rank from trailing [N] in the name.
  const m = fullName.match(/\[(\d+)\]$/);
  return m ? `${label} ${m[1]}` : label;
}

/** Split messages into incoming and outgoing for a given actor. */
export function splitMessages<T extends { from_actor_id: any; to_actor_id: any }>(
  messages: T[],
  actorId: number | string,
): { incoming: T[]; outgoing: T[] } {
  const id = String(actorId);
  return {
    incoming: messages.filter((m) => String(m.to_actor_id) === id),
    outgoing: messages.filter((m) => String(m.from_actor_id) === id),
  };
}

/** Message status color (queued/active/complete lifecycle). */
export function messageStatusColor(status: string): string {
  switch (status.toLowerCase()) {
    case "queued": return "var(--msg-status-queued)";
    case "active": return "var(--msg-status-active)";
    case "complete": return "var(--msg-status-complete)";
    default: return "var(--text-muted)";
  }
}
