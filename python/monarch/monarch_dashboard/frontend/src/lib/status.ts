/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/** Actor status semantics — one entry per ActorStatus enum variant. */
export interface StatusMeta {
  color: string;
  /** Coarse bucket for health/summarizing. */
  kind: "healthy" | "active" | "transitional" | "warn" | "error" | "neutral";
  /** Non-color glyph so status is distinguishable without color. */
  glyph: string;
}

export const STATUS_META: Record<string, StatusMeta> = {
  idle: { color: "#3fb950", kind: "healthy", glyph: "●" },
  client: { color: "#2ea043", kind: "healthy", glyph: "◆" },
  processing: { color: "#3b82f6", kind: "active", glyph: "▶" },
  saving: { color: "#2563eb", kind: "active", glyph: "▼" },
  loading: { color: "#06b6d4", kind: "active", glyph: "▲" },
  created: { color: "#eab308", kind: "transitional", glyph: "○" },
  initializing: { color: "#f59e0b", kind: "transitional", glyph: "◐" },
  stopping: { color: "#f97316", kind: "warn", glyph: "◑" },
  failed: { color: "#f85149", kind: "error", glyph: "✕" },
  stopped: { color: "#a371f7", kind: "error", glyph: "■" },
  unknown: { color: "#8b949e", kind: "neutral", glyph: "?" },
  system: { color: "#6e7681", kind: "neutral", glyph: "⚙" },
  "n/a": { color: "#6e7681", kind: "neutral", glyph: "·" },
};

const NEUTRAL: StatusMeta = { color: "#6e7681", kind: "neutral", glyph: "·" };

export function statusMeta(status: string | null | undefined): StatusMeta {
  if (!status) return NEUTRAL;
  return STATUS_META[status.toLowerCase()] ?? NEUTRAL;
}

export function statusColor(status: string | null | undefined): string {
  return statusMeta(status).color;
}

/** Ordered status list for legends / stacked bars (lifecycle order). */
export const STATUS_ORDER = [
  "created",
  "initializing",
  "loading",
  "client",
  "idle",
  "processing",
  "saving",
  "stopping",
  "stopped",
  "failed",
  "unknown",
];

/** Message-lifecycle status color. */
export function messageStatusColor(status: string): string {
  switch (status.toLowerCase()) {
    case "queued":
      return "#f59e0b";
    case "active":
      return "#3b82f6";
    case "complete":
      return "#3fb950";
    default:
      return "#8b949e";
  }
}

/** Health-score band → color + label. */
export function healthBand(score: number): { color: string; label: string } {
  if (score >= 85) return { color: "#3fb950", label: "Healthy" };
  if (score >= 60) return { color: "#eab308", label: "Degraded" };
  if (score >= 35) return { color: "#f97316", label: "At Risk" };
  return { color: "#f85149", label: "Critical" };
}
