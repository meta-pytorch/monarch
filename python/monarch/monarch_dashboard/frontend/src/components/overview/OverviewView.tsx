/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useMemo, useState } from "react";
import { useApi } from "../../lib/useApi";
import { ActorStats, useActorStats } from "../../lib/actorStats";
import { MessageStats, useMessageStats } from "../../lib/useMessageStats";
import { Actor, EntityId, ErrorActor, MessageActivity, Summary } from "../../types";
import { healthBand, statusMeta } from "../../lib/status";
import { actorInstance, actorRole, compact, formatTime, isSystemActor, leafName, localTzAbbrev, prettyRole } from "../../lib/format";
import { EChart } from "../common/echart";
import { Drawer, EmptyState, ErrorState, Loading } from "../common/ui";
import { ActorDetail } from "../hierarchy/ActorDetail";
import {
  IconActor,
  IconAlert,
  IconDelivery,
  IconEye,
  IconHealth,
  IconHost,
  IconMessage,
  IconProc,
} from "../common/icons";

/** Rolling telemetry retention window (minutes) from the message stream span.
 *  Message-derived counts are windowed, not all-time, so panels cite this to
 *  stay honest. Null until enough data has arrived. */
function windowMinFrom(start: number, end: number): number | null {
  if (end <= start) return null;
  return Math.max(1, Math.round((end - start) / 60_000_000));
}

export function OverviewView() {
  const { data, loading, error } = useApi<Summary>("/summary");
  const { data: activity } = useApi<MessageActivity>("/message-activity", 8000);
  const stats = useActorStats();
  const { data: msgStats } = useMessageStats();
  const [drawerActor, setDrawerActor] = useState<EntityId | null>(null);
  const windowMin = activity ? windowMinFrom(activity.start_us, activity.end_us) : null;

  if (loading && !data) return <Loading label="Loading metrics…" />;
  if (error) return <ErrorState message={error} />;
  if (!data) return <EmptyState label="No summary data" />;

  return (
    <div className="stack">
      <KpiRow s={data} stats={stats} ms={msgStats} windowMin={windowMin} />

      <div className="grid hero">
        <HealthPanel stats={stats} />
        <ActivityPanel s={data} activity={activity} />
        <StatusDonutPanel stats={stats} />
      </div>

      <div className="grid c2">
        <ErrorsPanel errors={data.errors} stats={stats} />
        <MessageTrafficPanel counts={data.message_counts} ms={msgStats} windowMin={windowMin} />
      </div>

      <FleetPanel actors={stats.actors} onSelect={setDrawerActor} />

      <HierarchyPanel s={data} stats={stats} />

      {drawerActor && (
        <Drawer title="Actor Detail" subtitle={`#${drawerActor}`} onClose={() => setDrawerActor(null)}>
          <ActorDetail actorId={drawerActor} compact />
        </Drawer>
      )}
    </div>
  );
}

/* ----------------------------- KPI row ----------------------------- */
function KpiRow({ s, stats, ms, windowMin }: { s: Summary; stats: ActorStats; ms: MessageStats | null; windowMin: number | null }) {
  const band = healthBand(stats.health);
  const actorsSub =
    stats.down > 0
      ? `${stats.down} down`
      : stats.system > 0
        ? `${stats.workload} workload · ${stats.system} system`
        : stats.unknown > 0
          ? `${stats.unknown} unknown`
          : "all healthy";
  // Handler-lifecycle metrics. Telemetry has no request/response distinction, so
  // "Messages" is total send throughput and "Success" is the handler success rate
  // = completed / (completed + failed) over messages whose handlers finished;
  // queued/active are still in progress, not failures.
  const msgVal = ms ? compact(ms.total) : compact(s.message_counts.total);
  // Telemetry tables are a rolling window, so this is throughput over the last
  // ~N min, not an all-time total — say so.
  const win = windowMin ? `last ${windowMin}m` : "rolling window";
  const msgSub = ms ? win : `${win} · events`;
  const finished = ms ? ms.completed + ms.failed : 0;
  const okVal = ms
    ? finished > 0
      ? `${(ms.successRate * 100).toFixed(0)}%`
      : "—"
    : `${(s.message_counts.delivery_rate * 100).toFixed(0)}%`;
  const okSub = !ms
    ? "delivered"
    : ms.failed > 0
      ? `${compact(ms.failed)} failed · ${compact(ms.inProgress)} in progress`
      : `${compact(ms.inProgress)} in progress`;
  const okColor = ms && ms.failed > 0 ? "var(--st-failed)" : "var(--st-idle)";
  const tiles = [
    { label: "Hosts", val: s.hierarchy_counts.host_meshes, sub: "host meshes", Icon: IconHost, color: "var(--chart-2)" },
    { label: "Procs", val: s.hierarchy_counts.proc_meshes, sub: "proc meshes", Icon: IconProc, color: "var(--chart-4)" },
    { label: "Actors", val: stats.total, sub: actorsSub, Icon: IconActor, color: stats.down > 0 ? "var(--st-failed)" : "var(--st-idle)" },
    { label: "Messages", val: msgVal, sub: msgSub, Icon: IconMessage, color: "var(--chart-3)" },
    { label: "Success", val: okVal, sub: okSub, Icon: IconDelivery, color: okColor },
    { label: "Health", val: stats.health, sub: band.label, Icon: IconHealth, color: band.color },
  ];
  return (
    <div className="grid kpis">
      {tiles.map((t) => (
        <div key={t.label} className="kpi" style={{ ["--kpi-accent" as string]: t.color }}>
          <div className="kpi-top">
            <span className="kpi-label">{t.label}</span>
            <t.Icon className="kpi-ico" size={16} />
          </div>
          <div className="kpi-val">{t.val}</div>
          <div className="kpi-sub">{t.sub}</div>
        </div>
      ))}
    </div>
  );
}

/* --------------------------- Health gauge --------------------------- */
function HealthPanel({ stats }: { stats: ActorStats }) {
  const band = healthBand(stats.health);
  const option = useMemo(
    () => ({
      series: [
        {
          type: "gauge",
          startAngle: 210,
          endAngle: -30,
          radius: "92%",
          center: ["50%", "58%"],
          progress: { show: true, width: 14, roundCap: true, itemStyle: { color: band.color } },
          axisLine: { lineStyle: { width: 14, color: [[1, "rgba(255,255,255,0.06)"]] } },
          pointer: { show: false },
          axisTick: { show: false },
          splitLine: { show: false },
          axisLabel: { show: false },
          anchor: { show: false },
          data: [{ value: stats.health }],
          detail: {
            valueAnimation: true,
            offsetCenter: [0, "-2%"],
            fontSize: 40,
            fontWeight: 700,
            fontFamily: "SF Mono, monospace",
            color: "#e9edf3",
            formatter: "{value}",
          },
          title: { show: false },
        },
      ],
    }),
    [stats.health, band.color]
  );
  const parts = [
    `${stats.healthy} healthy`,
    stats.unknown > 0 ? `${stats.unknown} unknown` : null,
    stats.down > 0 ? `${stats.down} down` : null,
  ].filter(Boolean);
  const desc =
    `${parts.join(" · ")} of ${stats.workload} workload` +
    (stats.system > 0 ? ` · ${stats.system} system excluded` : "");
  return (
    <div className="panel">
      <div className="panel-head"><h3>System Health</h3><div className="spacer" /><span className="count-tag">workload</span></div>
      <div className="panel-body gauge-wrap">
        <EChart option={option} height={180} />
        <div className="gauge-caption">
          <div className="band" style={{ color: band.color }}>{band.label}</div>
          <div className="desc">{desc}</div>
        </div>
      </div>
    </div>
  );
}

/* ------------------- Activity: throughput + timeline ------------------- */
function ActivityPanel({ s, activity }: { s: Summary; activity: MessageActivity | null }) {
  // Window + histogram are computed server-side (a small bounded result), so the
  // panel never polls the full message list — streaming that back would re-record
  // each batch as telemetry. Fall back to summary.timeline (from actor status
  // events) until the first activity payload arrives.
  const hasSpan = activity != null && activity.end_us > activity.start_us;
  const start = hasSpan ? activity!.start_us : s.timeline.start_us;
  const end = hasSpan ? activity!.end_us : s.timeline.end_us;
  const total = activity?.total ?? 0;
  const buckets = useMemo(() => activity?.buckets ?? [], [activity]);

  const labels = useMemo(() => {
    const N = buckets.length || 44;
    const span = Math.max(1, end - start);
    return Array.from({ length: N }, (_, i) => formatTime(start + (span * i) / N));
  }, [start, end, buckets.length]);

  const fo = s.timeline.failure_onset_us;
  const failPct = fo != null && fo >= start && fo <= end ? ((fo - start) / Math.max(1, end - start)) * 100 : null;
  const durMin = ((end - start) / 1_000_000 / 60).toFixed(1);

  const option = useMemo(
    () => ({
      grid: { left: 6, right: 10, top: 12, bottom: 6, containLabel: false },
      tooltip: {
        trigger: "axis",
        backgroundColor: "#1c212c",
        borderColor: "#2f3646",
        textStyle: { color: "#e9edf3", fontSize: 11 },
        formatter: (p: any) => `${labels[p[0].dataIndex]}<br/><b>${p[0].value}</b> messages`,
      },
      xAxis: { type: "category", show: false, data: labels, boundaryGap: false },
      yAxis: { type: "value", show: false },
      series: [
        {
          type: "line",
          data: buckets,
          smooth: true,
          symbol: "none",
          lineStyle: { width: 2, color: "#f0842f" },
          areaStyle: {
            color: {
              type: "linear", x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: "rgba(240,132,47,0.35)" },
                { offset: 1, color: "rgba(240,132,47,0.02)" },
              ],
            },
          },
          markLine:
            failPct != null
              ? {
                  symbol: "none",
                  data: [{ xAxis: Math.round((failPct / 100) * (labels.length - 1)) }],
                  lineStyle: { color: "#f85149", type: "dashed", width: 1.5 },
                  label: { show: true, formatter: "failure", color: "#f85149", fontSize: 10, position: "insideEndTop" },
                }
              : undefined,
        },
      ],
    }),
    [buckets, labels, failPct]
  );

  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Message Activity</h3>
        <span className="sub">throughput · last {durMin} min</span>
        <div className="spacer" />
        <span className="count-tag">{compact(total)} messages</span>
      </div>
      <div className="panel-body">
        <EChart option={option} height={130} />
        <SessionTimeline start={start} end={end} errors={s.errors} failureOnset={fo} />
        <div className="chart-note">
          <IconEye size={12} />
          <span>Live polling adds a small amount of <code>scan</code> telemetry traffic.</span>
        </div>
      </div>
    </div>
  );
}

function SessionTimeline({
  start,
  end,
  errors,
  failureOnset,
}: {
  start: number;
  end: number;
  errors: Summary["errors"];
  failureOnset: number | null;
}) {
  const span = Math.max(1, end - start);
  const durMin = (span / 1_000_000 / 60).toFixed(1);
  const inWin = (us: number) => us >= start && us <= end;
  const pos = (us: number) => `${Math.min(99.6, Math.max(0.4, ((us - start) / span) * 100))}%`;
  const failPct = failureOnset != null && inWin(failureOnset) ? pos(failureOnset) : null;
  const notches = [
    ...errors.failed_actors.filter((a) => inWin(a.timestamp_us)).map((a) => ({ a, kind: "failed", color: "var(--st-failed)" })),
    ...errors.stopped_actors.filter((a) => inWin(a.timestamp_us)).map((a) => ({ a, kind: "stopped", color: "var(--st-stopped)" })),
  ];
  return (
    <div style={{ marginTop: "var(--s4)" }}>
      <div className="timeline">
        <div className="timeline-fill" />
        {failPct && <div className="timeline-danger" style={{ left: failPct, right: 0 }} />}
        <div className="timeline-grid" />
        {notches.map((n, i) => (
          <div key={i} className="timeline-event"
            style={{ left: pos(n.a.timestamp_us), background: n.color, boxShadow: `0 0 6px ${n.color}` }}
            title={`${leafName(n.a.full_name)} ${n.kind}${n.a.reason ? " — " + n.a.reason : ""}`} />
        ))}
        {failPct && (
          <div className="timeline-marker" style={{ left: failPct, background: "var(--st-failed)" }}>
            <span className="flag" style={{ background: "var(--st-failed)" }}>failure</span>
          </div>
        )}
      </div>
      <div className="timeline-axis">
        <span>{formatTime(start)}</span>
        <span>{durMin} min · {localTzAbbrev()}</span>
        <span>{formatTime(end)}</span>
      </div>
    </div>
  );
}

/* --------------------------- Status donut --------------------------- */
function StatusDonutPanel({ stats }: { stats: ActorStats }) {
  const entries = Object.entries(stats.byStatus).sort((a, b) => b[1] - a[1]);
  const option = useMemo(
    () => ({
      tooltip: {
        trigger: "item",
        backgroundColor: "#1c212c",
        borderColor: "#2f3646",
        textStyle: { color: "#e9edf3", fontSize: 11 },
        formatter: "{b}: <b>{c}</b> ({d}%)",
      },
      series: [
        {
          type: "pie",
          radius: ["58%", "82%"],
          center: ["50%", "50%"],
          avoidLabelOverlap: false,
          itemStyle: { borderColor: "#12151c", borderWidth: 2 },
          label: { show: false },
          labelLine: { show: false },
          data: entries.map(([k, v]) => ({ name: k, value: v, itemStyle: { color: statusMeta(k).color } })),
        },
      ],
      graphic: {
        type: "text",
        left: "center",
        top: "center",
        style: { text: `${stats.total}\nactors`, textAlign: "center", fill: "#e9edf3", fontSize: 22, fontWeight: 700, lineHeight: 22 },
      },
    }),
    [entries, stats.total]
  );
  return (
    <div className="panel">
      <div className="panel-head"><h3>Actor Status</h3><div className="spacer" /><span className="count-tag">current state</span></div>
      <div className="panel-body">
        <EChart option={option} height={170} />
        <div className="legend">
          {entries.map(([k, v]) => (
            <span key={k} className="legend-item">
              <span className="dot-only" style={{ background: statusMeta(k).color }} />
              {k} <b>{v}</b>
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}

/* ---------------------------- Fleet map ---------------------------- */
function FleetPanel({ actors, onSelect }: { actors: Actor[]; onSelect: (id: EntityId) => void }) {
  const [hideSystem, setHideSystem] = useState(true);
  const sorted = useMemo(() => {
    return [...actors].sort((a, b) => {
      // Workload first, then system; then by role, then instance.
      const sa = isSystemActor(a.full_name) ? 1 : 0;
      const sb = isSystemActor(b.full_name) ? 1 : 0;
      if (sa !== sb) return sa - sb;
      const ra = actorRole(a.full_name);
      const rb = actorRole(b.full_name);
      if (ra !== rb) return ra < rb ? -1 : 1;
      return (actorInstance(a.full_name) ?? "").localeCompare(actorInstance(b.full_name) ?? "");
    });
  }, [actors]);
  const systemCount = useMemo(() => actors.filter((a) => isSystemActor(a.full_name)).length, [actors]);
  const shown = hideSystem ? sorted.filter((a) => !isSystemActor(a.full_name)) : sorted;
  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Actor Fleet</h3>
        <span className="sub">
          {hideSystem ? "workload actors" : "every actor"} by role &amp; live status — click to inspect
        </span>
        <div className="spacer" />
        {systemCount > 0 && (
          <button
            className={`btn sm${hideSystem ? "" : " active"}`}
            onClick={() => setHideSystem((h) => !h)}
            title={hideSystem ? `Show ${systemCount} system actors` : "Hide system actors"}
          >
            <IconEye size={13} /> {hideSystem ? "Show System" : "Hide System"}
          </button>
        )}
        <span className="count-tag">{shown.length}</span>
      </div>
      <div className="panel-body">
        {shown.length === 0 ? (
          <EmptyState label="No actors" />
        ) : (
          <div className="fleet-tiles">
            {shown.map((a) => {
              const sys = isSystemActor(a.full_name);
              const st = sys ? "system" : a.latest_status ?? "unknown";
              const m = statusMeta(st);
              const err = m.kind === "error";
              const role = prettyRole(actorRole(a.full_name));
              const inst = actorInstance(a.full_name);
              return (
                <button
                  key={a.id}
                  className={`fleet-tile${err ? " err" : ""}${sys ? " sys" : ""}`}
                  style={{ ["--t-color" as string]: m.color }}
                  onClick={() => onSelect(a.id)}
                  title={a.full_name}
                >
                  <span className="ft-dot" style={{ background: m.color }} />
                  <span className="ft-txt">
                    <span className="ft-name">{role}</span>
                    <span className="ft-status">
                      {inst && <span className="ft-inst">{inst}</span>}
                      <span style={{ color: m.color }}>{st}</span>
                    </span>
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

/* ------------------------- Errors & failures ------------------------- */
function ErrorsPanel({ errors, stats }: { errors: Summary["errors"]; stats: ActorStats }) {
  // Prefer the backend error lists (they carry failure reasons); fall back to
  // deriving down actors from current state so the panel is never inconsistent
  // with the "N down" shown elsewhere.
  const derivedDown = stats.actors.filter(
    (a) => (a.latest_status ?? "").toLowerCase() === "failed" || (a.latest_status ?? "").toLowerCase() === "stopped"
  );
  const failed = errors.failed_actors;
  const stopped = errors.stopped_actors;
  const hasBackend = failed.length > 0 || stopped.length > 0;
  const has = hasBackend || derivedDown.length > 0 || errors.failed_messages > 0;

  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Errors &amp; Failures</h3>
        <div className="spacer" />
        {has && (
          <span className="count-tag" style={{ color: "#fff", background: "var(--st-failed)", borderColor: "var(--st-failed)" }}>
            {hasBackend ? failed.length + stopped.length : derivedDown.length}
          </span>
        )}
      </div>
      <div className="panel-body">
        {!has ? (
          <div className="ok-banner"><IconDelivery size={18} /> No errors detected</div>
        ) : hasBackend ? (
          <>
            <ErrGroup title="Failed Actors" actors={failed} color="var(--st-failed)" />
            <ErrGroup title="Stopped Actors" actors={stopped} color="var(--st-stopped)" />
          </>
        ) : (
          <div className="err-group">
            <div className="err-head"><IconAlert size={13} /> Down Actors <span className="count-tag">{derivedDown.length}</span></div>
            {derivedDown.map((a) => (
              <div key={a.id} className="err-item" style={{ ["--err-color" as string]: statusMeta(a.latest_status).color }}>
                <div className="name">{prettyRole(actorRole(a.full_name))} · {actorInstance(a.full_name) ?? leafName(a.full_name)}</div>
                <div className="meta"><span className="reason">{a.latest_status}</span></div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function ErrGroup({ title, actors, color }: { title: string; actors: ErrorActor[]; color: string }) {
  if (actors.length === 0) return null;
  return (
    <div className="err-group">
      <div className="err-head" style={{ color }}>
        <IconAlert size={13} /> {title} <span className="count-tag">{actors.length}</span>
      </div>
      {actors.map((a) => (
        <div key={a.actor_id} className="err-item" style={{ ["--err-color" as string]: color }}>
          <div className="name">{prettyRole(actorRole(a.full_name))} · {actorInstance(a.full_name) ?? leafName(a.full_name)}</div>
          <div className="meta">
            <span className="reason">{a.reason ?? title.toLowerCase()}</span>
            <span className="time">{formatTime(a.timestamp_us)}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

/* ------------------------- Message traffic ------------------------- */
function MessageTrafficPanel({ counts, ms, windowMin }: { counts: Summary["message_counts"]; ms: MessageStats | null; windowMin: number | null }) {
  // The messages / message_status_events tables retain only a rolling window
  // (default 10 min), so every count here is "recent", not job-cumulative.
  const winLabel = windowMin ? `last ${windowMin}m` : "rolling window";
  if (ms) {
    const endpoints = ms.endpoints;
    const maxEp = Math.max(1, ...endpoints.map((e) => e.total));
    const finished = ms.completed + ms.failed;
    const pct = (n: number) => (ms.total > 0 ? (n / ms.total) * 100 : 0);
    // Handler-lifecycle segments, most-resolved first. These are handler states,
    // not request/response kinds — telemetry carries no req/resp bit.
    const segs = [
      { key: "completed", label: "completed", n: ms.completed, color: "var(--st-idle)" },
      { key: "failed", label: "failed", n: ms.failed, color: "var(--st-failed)" },
      { key: "active", label: "active", n: ms.active, color: "var(--st-created)" },
      { key: "queued", label: "queued", n: ms.queued, color: "var(--text-3)" },
    ];
    return (
      <div className="panel">
        <div className="panel-head">
          <h3>Message Traffic</h3>
          <span className="sub">handler lifecycle · {winLabel}</span>
          <div className="spacer" />
          <span className="count-tag" style={{ color: ms.failed > 0 ? "var(--st-failed)" : "var(--st-idle)" }}>
            {finished > 0 ? `${(ms.successRate * 100).toFixed(0)}% ok` : "none finished"}
          </span>
        </div>
        <div className="panel-body">
          {/* Handler lifecycle: queued -> active -> complete | failed. Telemetry
              has no request/response distinction, so "not completed" means still
              in progress (queued/active) — which is separate from failed. */}
          <div className="msg-class">
            <div className="msg-class-head">
              <span className="eyebrow">Handler lifecycle</span>
              <span className="msg-class-note">queued → active → completed / failed</span>
              <div className="spacer" />
              <span className="mono" style={{ color: "var(--text-2)", fontWeight: 700 }}>{compact(ms.total)} total</span>
            </div>
            <div className="stacked-bar" style={{ height: 12 }}>
              {ms.total > 0 ? (
                segs.map((seg) => (
                  <span key={seg.key} style={{ width: `${pct(seg.n)}%`, background: seg.color }} title={`${seg.label}: ${seg.n}`} />
                ))
              ) : (
                <span style={{ width: "100%", background: "var(--surface-2)" }} />
              )}
            </div>
            <div className="chips" style={{ marginTop: "var(--s3)" }}>
              {segs.map((seg) => (
                <span key={seg.key} className="chip">
                  <span className="dot-only" style={{ background: seg.color }} /> {seg.label} <b>{compact(seg.n)}</b>
                </span>
              ))}
            </div>
            <div style={{ marginTop: "var(--s3)", fontSize: 11.5, color: "var(--text-3)", lineHeight: 1.5 }}>
              These are handler states, not delivery kinds — Monarch telemetry has no request/response bit.
              <b style={{ color: "var(--text-2)" }}> completed</b> = handler returned Ok,
              <b style={{ color: "var(--st-failed)" }}> failed</b> = handler errored; queued/active are still in
              progress, not failures.
            </div>
          </div>

          <div className="eyebrow" style={{ margin: "var(--s5) 0 var(--s3)" }}>By Endpoint · message volume · {winLabel}</div>
          {endpoints.map((e) => (
            <div key={e.endpoint} className="endpoint-row">
              <span className="name">{e.endpoint === "(none)" || e.endpoint === "null" ? "—" : e.endpoint}</span>
              <span className="bar-track"><span className="bar-fill" style={{ width: `${(e.total / maxEp) * 100}%` }} /></span>
              <span className="num">{compact(e.total)}</span>
            </div>
          ))}
        </div>
      </div>
    );
  }

  // Fallback: raw summary counts until the lifecycle query loads.
  const total = counts.total || 0;
  const endpoints = Object.entries(counts.by_endpoint).sort((a, b) => b[1] - a[1]);
  const maxEp = Math.max(1, ...endpoints.map(([, c]) => c));
  return (
    <div className="panel">
      <div className="panel-head">
        <h3>Message Traffic</h3>
        <span className="sub">{winLabel}</span>
        <div className="spacer" />
        <span className="count-tag">{compact(total)} messages</span>
      </div>
      <div className="panel-body">
        <div className="eyebrow" style={{ margin: "0 0 var(--s3)" }}>By Endpoint · message volume · {winLabel}</div>
        {endpoints.map(([ep, c]) => (
          <div key={ep} className="endpoint-row">
            <span className="name">{ep === "(none)" ? "—" : ep}</span>
            <span className="bar-track"><span className="bar-fill" style={{ width: `${(c / maxEp) * 100}%` }} /></span>
            <span className="num">{compact(c)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ------------------------ Hierarchy breakdown ------------------------ */
function HierarchyPanel({ s, stats }: { s: Summary; stats: ActorStats }) {
  const c = s.hierarchy_counts;
  const entries: Array<[string, number]> = [
    ["Host Meshes", c.host_meshes],
    ["Proc Meshes", c.proc_meshes],
    ["Actor Meshes", c.actor_meshes],
  ];
  const total = entries.reduce((a, [, n]) => a + n, 0) || 1;
  return (
    <div className="panel">
      <div className="panel-head"><h3>Topology Breakdown</h3></div>
      <div className="panel-body">
        <div className="row" style={{ marginBottom: "var(--s4)" }}>
          {entries.map(([lab, n]) => (
            <div key={lab} className="mesh-chip">
              <span className="big">{n}</span>
              <span className="lab">{lab}</span>
              <span className="pct">{((n / total) * 100).toFixed(0)}%</span>
            </div>
          ))}
        </div>
        <div className="breakdown-row" style={{ gridTemplateColumns: "1fr" }}>
          <div className="stacked-bar" style={{ height: 12 }}>
            <span style={{ width: `${(c.host_meshes / total) * 100}%`, background: "var(--chart-2)" }} />
            <span style={{ width: `${(c.proc_meshes / total) * 100}%`, background: "var(--chart-4)" }} />
            <span style={{ width: `${(c.actor_meshes / total) * 100}%`, background: "var(--chart-3)" }} />
          </div>
        </div>
        <div className="row" style={{ marginTop: "var(--s3)", justifyContent: "space-between", color: "var(--text-3)", fontSize: 11 }}>
          <span>{s.mesh_counts.total} total meshes · {stats.total} actors</span>
          <span>{compact(s.timeline.total_status_events)} status events</span>
        </div>
      </div>
    </div>
  );
}
