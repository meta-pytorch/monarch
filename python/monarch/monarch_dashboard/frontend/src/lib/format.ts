/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

const p2 = (n: number) => String(n).padStart(2, "0");

/** Microsecond epoch → "HH:MM:SS.mmm" in the viewer's local timezone.
 *  Uses local wall-clock components (not toISOString, which is always UTC) so
 *  displayed times match the clock of whoever is viewing the dashboard. */
export function formatTime(us: number | null | undefined): string {
  if (us == null || isNaN(us)) return "—";
  const d = new Date(us / 1000);
  if (isNaN(d.getTime())) return "—";
  return `${p2(d.getHours())}:${p2(d.getMinutes())}:${p2(d.getSeconds())}.${String(
    d.getMilliseconds()
  ).padStart(3, "0")}`;
}

/** Microsecond epoch → "YYYY-MM-DD HH:MM:SS" in the viewer's local timezone. */
export function formatDateTime(us: number | null | undefined): string {
  if (us == null || isNaN(us)) return "—";
  const d = new Date(us / 1000);
  if (isNaN(d.getTime())) return "—";
  return `${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())} ${p2(
    d.getHours()
  )}:${p2(d.getMinutes())}:${p2(d.getSeconds())}`;
}

/** Short local timezone abbreviation, e.g. "EDT" / "PST" (falls back to GMT±N).
 *  Shown next to displayed times so the zone is never ambiguous. */
export function localTzAbbrev(): string {
  try {
    const parts = new Intl.DateTimeFormat("en-US", { timeZoneName: "short" }).formatToParts(
      new Date()
    );
    const tz = parts.find((x) => x.type === "timeZoneName")?.value;
    if (tz) return tz;
  } catch {
    // ignore
  }
  const off = -new Date().getTimezoneOffset() / 60;
  return `GMT${off >= 0 ? "+" : ""}${off}`;
}

/** Relative time from a wall-clock ms timestamp. */
export function relativeTime(ms: number | null | undefined): string {
  if (ms == null) return "—";
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 2) return "just now";
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  return `${Math.floor(m / 60)}h ago`;
}

/** Compact number: 1234 → "1.2k". */
export function compact(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}k`;
  return `${(n / 1_000_000).toFixed(1)}M`;
}

const METRIC_LABELS: Record<string, string> = {
  "actor.message_handler_duration.ns": "Handler time",
  "actor.message_queue_size": "Queue depth",
  "actor.messages_received": "Messages received",
  "mailbox.posts": "Messages sent",
  "message.e2e_latency.us": "Delivery latency (sampled)",
};

/**
 * Readable label for a metric: "Current …" for up-down counters, "Total …" for
 * counters, and the plain label for histograms. Null for unknown histograms.
 */
export function metricLabel(
  name: string,
  kind: "histogram" | "counter" | "current",
): string | null {
  const label = METRIC_LABELS[name];
  const prefix = kind === "current" ? "Current" : kind === "counter" ? "Total" : null;
  if (!prefix) return label ?? null;
  return label ? `${prefix} ${label[0].toLowerCase()}${label.slice(1)}` : `${prefix} value`;
}

const NANOS_PER_UNIT: Record<string, number> = { ns: 1, us: 1e3, ms: 1e6, s: 1e9 };

/** A duration in `unit` as "45.3 µs" or "13.9 ms"; null for non-time units. */
export function formatDuration(value: number, unit: string): string | null {
  const scale = NANOS_PER_UNIT[unit];
  if (scale === undefined) return null;
  const ns = value * scale;
  const [divisor, suffix] =
    ns >= 1e9 ? [1e9, "s"] : ns >= 1e6 ? [1e6, "ms"] : ns >= 1e3 ? [1e3, "µs"] : [1, "ns"];
  return `${Number((ns / divisor).toPrecision(3))} ${suffix}`;
}

/** An actor metric's value: samples, average and total for histograms, else the total. */
export function formatMetricValue(m: {
  name: string;
  unit: string;
  kind: "histogram" | "counter" | "current";
  count: number | null;
  total: number | null;
}): string {
  const total = m.total ?? 0;
  const unit = m.unit || (m.name.split(".").pop() ?? "");
  const value = (v: number) =>
    formatDuration(v, unit) ?? v.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (m.kind !== "histogram") return value(total);
  const n = m.count ?? 0;
  if (n === 0) return "0 samples";
  return `${n.toLocaleString("en-US")} samples · avg ${value(total / n)} · total ${value(total)}`;
}

/** Parse shape_json → "[2, 4]" for both ndslice and legacy formats. */
export function formatShape(shapeJson: string | null | undefined): string {
  if (!shapeJson) return "—";
  try {
    const p = JSON.parse(shapeJson);
    if (p.inner?.sizes) return `[${p.inner.sizes.join(", ")}]`;
    if (p.sizes) return `[${p.sizes.join(", ")}]`;
    if (p.dims) return `[${p.dims.join(", ")}]`;
    return shapeJson;
  } catch {
    return shapeJson;
  }
}

/** Last segment of a hierarchical name (handles both "/" and "," separators).
 *  Drops the transport suffix so ipc paths like
 *  "trainer<..>.anon-0<..>@anon-0<..>.ipc:///tmp/…/hosts_0" don't collapse
 *  to the meaningless path tail "hosts_0". */
export function leafName(name: string | null | undefined): string {
  if (!name) return "—";
  const head = name.split("@")[0].split(/:\/\//)[0];
  return head.split("/").pop()!.split(",").pop()!;
}

/**
 * Actor role/type — the leading token of a Monarch actor full_name.
 *   "trainer<o7B>.anon-0<dqZ>@…ipc://…/hosts_0" -> "trainer"
 *   "host_agent.service<2>@ipc://…"                    -> "host_agent"
 *   "proc_agent.anon-3<iBh>@…"                       -> "proc_agent"
 */
export function actorRole(fullName: string | null | undefined): string {
  if (!fullName) return "actor";
  const head = fullName.split("@")[0];
  const role = head.split(/[<.]/)[0].trim();
  return role || "actor";
}

/**
 * Instance token that differentiates same-role actors (the proc segment).
 *   "trainer<o7B>.anon-0<dqZ>@…" -> "anon-0"
 *   "host_agent.service<2>@…"            -> "service"
 */
export function actorInstance(fullName: string | null | undefined): string | null {
  if (!fullName) return null;
  const head = fullName.split("@")[0];
  const parts = head.split(".");
  if (parts.length < 2) return null;
  return parts[1].split("<")[0].trim() || null;
}

/** Title-case a role slug: "host_agent" -> "Host Agent". */
export function prettyRole(role: string): string {
  return role
    .split(/[_-]/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

/**
 * Clean a DAG node label for display: strip "<uuid>", "[uuid]", "(controller)"
 * and transport, and give host nodes a friendly name.
 *   actor "trainer[o7B7whEtk1W]" -> "trainer"
 *   proc  "worker-0[fa4wpgPjMyD]"    -> "worker-0"
 *   host  "/tmp/…/hosts_0"           -> "hosts_0"
 *   host  "@91ytVCObcks…"            -> "@91ytVCObcks…"
 */
export function cleanNodeLabel(label: string, tier: string): string {
  let s = label.replace(/\(controller\)/gi, "").trim();
  s = s.replace(/<[^>]*>/g, "");
  s = s.replace(/\[[A-Za-z0-9]{6,}\]/g, "");
  s = s.split("@")[0].trim();
  if (tier === "host") {
    if (s.includes("/")) return s.split(/[:/]/).filter(Boolean).pop() ?? s;
    return s || label.trim();
  }
  return s || label;
}

/**
 * Friendly display name for host/proc system agents.
 * "…HostAgent[0]" → "Host Unit 0"; returns null for non-agents.
 */
export function agentDisplayName(
  fullName: string | null | undefined,
  rank?: number | string | null
): string | null {
  if (!fullName) return null;
  const low = fullName.toLowerCase();
  const isHost = low.includes("hostagent") || low.includes("host_agent");
  const isProc = low.includes("procagent") || low.includes("proc_agent");
  if (!isHost && !isProc) return null;
  const label = isHost ? "Host Unit" : "Proc Unit";
  if (rank != null) return `${label} ${rank}`;
  const m = fullName.match(/\[(\d+)\]$/);
  return m ? `${label} ${m[1]}` : label;
}

/** Split messages relative to an actor into incoming / outgoing. */
export function splitMessages<
  T extends { from_actor_id: unknown; to_actor_id: unknown }
>(messages: T[], actorId: number | string): { incoming: T[]; outgoing: T[] } {
  const id = String(actorId);
  return {
    incoming: messages.filter((m) => String(m.to_actor_id) === id),
    outgoing: messages.filter((m) => String(m.from_actor_id) === id),
  };
}
