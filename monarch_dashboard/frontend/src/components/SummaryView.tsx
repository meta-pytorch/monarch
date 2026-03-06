/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { useApi } from "../hooks/useApi";
import { Summary } from "../types";
import { statusColor, formatTimestamp, messageStatusColor } from "../utils/status";
import { StatusBadge } from "./StatusBadge";

/* ------------------------------------------------------------------ */
/* Sub-components                                                      */
/* ------------------------------------------------------------------ */

function OverviewCards({ data }: { data: Summary }) {
  const cards = [
    { label: "Hosts", value: data.hierarchy_counts.host_meshes, sub: "host meshes" },
    { label: "Procs", value: data.hierarchy_counts.proc_meshes, sub: "proc meshes" },
    { label: "Actors", value: data.actor_counts.total, sub: `${Object.keys(data.actor_counts.by_status).length} statuses` },
    { label: "Messages", value: data.message_counts.total, sub: `${(data.message_counts.delivery_rate * 100).toFixed(1)}% delivered` },
  ];

  return (
    <div className="summary-overview-cards" data-testid="overview-cards">
      {cards.map((c) => (
        <div key={c.label} className="summary-card">
          <div className="summary-card-value">{c.value}</div>
          <div className="summary-card-label">{c.label}</div>
          <div className="summary-card-sub">{c.sub}</div>
        </div>
      ))}
    </div>
  );
}

function StatusBreakdown({ byStatus }: { byStatus: Record<string, number> }) {
  const total = Object.values(byStatus).reduce((a, b) => a + b, 0);
  const entries = Object.entries(byStatus).sort(
    (a, b) => b[1] - a[1]
  );

  return (
    <div className="summary-section" data-testid="status-breakdown">
      <h3 className="summary-section-title">Actor Status Breakdown</h3>
      {/* Status bar */}
      <div className="summary-status-bar">
        {entries.map(([status, count]) => (
          <div
            key={status}
            className="summary-status-segment"
            style={{
              width: `${(count / total) * 100}%`,
              background: statusColor(status),
            }}
            title={`${status}: ${count}`}
          />
        ))}
      </div>
      {/* Legend rows */}
      <div className="summary-status-rows">
        {entries.map(([status, count]) => (
          <div key={status} className="summary-status-row">
            <StatusBadge status={status} />
            <span className="summary-status-count">{count}</span>
            <span className="summary-status-pct">
              {((count / total) * 100).toFixed(0)}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ActorErrorGroup({
  actors,
  title,
}: {
  actors: Array<{
    actor_id: number;
    full_name: string;
    reason: string | null;
    timestamp_us: number;
  }>;
  title: string;
}) {
  if (actors.length === 0) return null;
  return (
    <div className="summary-error-group">
      <h4 className="summary-error-heading">
        {title}
        <span className="count-badge">{actors.length}</span>
      </h4>
      {actors.map((a) => (
        <div key={a.actor_id} className="summary-error-item">
          <div className="summary-error-name">
            {a.full_name.split("/").pop()}
          </div>
          <div className="summary-error-detail">
            <span className="summary-error-reason">
              {a.reason ?? title.toLowerCase().replace(" actors", "")}
            </span>
            <span className="summary-error-time">
              {formatTimestamp(a.timestamp_us)}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}

function ErrorPanel({ errors }: { errors: Summary["errors"] }) {
  const hasErrors =
    errors.failed_actors.length > 0 ||
    errors.stopped_actors.length > 0 ||
    errors.failed_messages > 0;

  return (
    <div
      className={`summary-section ${hasErrors ? "summary-section-alert" : ""}`}
      data-testid="error-panel"
    >
      <h3 className="summary-section-title">
        Errors & Failures
        {hasErrors && (
          <span className="summary-alert-badge">
            {errors.failed_actors.length + errors.stopped_actors.length}
          </span>
        )}
      </h3>

      {!hasErrors && (
        <div className="summary-empty">No errors detected</div>
      )}

      <ActorErrorGroup actors={errors.failed_actors} title="Failed Actors" />
      <ActorErrorGroup actors={errors.stopped_actors} title="Stopped Actors" />

      {errors.failed_messages > 0 && (
        <div className="summary-error-group">
          <h4 className="summary-error-heading">Failed Messages</h4>
          <div className="summary-error-item">
            <div className="summary-error-name">
              {errors.failed_messages} message{errors.failed_messages !== 1 ? "s" : ""} failed delivery
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function MessageTraffic({ counts }: { counts: Summary["message_counts"] }) {
  const endpoints = Object.entries(counts.by_endpoint).sort(
    (a, b) => b[1] - a[1]
  );
  const maxCount = Math.max(...endpoints.map(([, c]) => c));

  return (
    <div className="summary-section" data-testid="message-traffic">
      <h3 className="summary-section-title">Message Traffic</h3>

      {/* Delivery rate gauge */}
      <div className="summary-delivery-rate">
        <div className="summary-delivery-bar-bg">
          <div
            className="summary-delivery-bar-fill"
            style={{ width: `${counts.delivery_rate * 100}%` }}
          />
        </div>
        <span className="summary-delivery-label">
          {(counts.delivery_rate * 100).toFixed(1)}% delivery rate
        </span>
      </div>

      {/* Status breakdown */}
      <div className="summary-msg-statuses">
        {Object.entries(counts.by_status).map(([status, count]) => (
          <div key={status} className="summary-msg-status-chip">
            <span
              className="status-dot"
              style={{ background: messageStatusColor(status) }}
            />
            <span>{status}</span>
            <span className="summary-msg-status-count">{count}</span>
          </div>
        ))}
      </div>

      {/* Endpoint bars */}
      <h4 className="summary-subsection-title">By Endpoint</h4>
      <div className="summary-endpoint-bars">
        {endpoints.map(([ep, count]) => (
          <div key={ep} className="summary-endpoint-row">
            <span className="summary-endpoint-name">{ep}</span>
            <div className="summary-endpoint-bar-bg">
              <div
                className="summary-endpoint-bar-fill"
                style={{ width: `${(count / maxCount) * 100}%` }}
              />
            </div>
            <span className="summary-endpoint-count">{count}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function TimelineBar({
  timeline,
  errors,
}: {
  timeline: Summary["timeline"];
  errors: Summary["errors"];
}) {
  const duration = timeline.end_us - timeline.start_us;

  // Collect error events with their position on the timeline.
  const notches: Array<{ pct: number; status: string; name: string; timestamp_us: number }> = [];
  if (duration > 0) {
    for (const a of errors.failed_actors) {
      notches.push({
        pct: ((a.timestamp_us - timeline.start_us) / duration) * 100,
        status: "failed",
        name: a.full_name.split("/").pop() ?? "actor",
        timestamp_us: a.timestamp_us,
      });
    }
    for (const a of errors.stopped_actors) {
      notches.push({
        pct: ((a.timestamp_us - timeline.start_us) / duration) * 100,
        status: "stopped",
        name: a.full_name.split("/").pop() ?? "actor",
        timestamp_us: a.timestamp_us,
      });
    }
  }

  return (
    <div className="summary-section" data-testid="timeline-bar">
      <h3 className="summary-section-title">Session Timeline</h3>

      <div className="summary-timeline-info">
        <span>Start: {formatTimestamp(timeline.start_us)}</span>
        <span>End: {formatTimestamp(timeline.end_us)}</span>
        <span>Duration: {((duration / 1_000_000) / 60).toFixed(1)} min</span>
      </div>

      <div className="summary-timeline-track">
        {/* Full healthy bar */}
        <div className="summary-timeline-healthy" style={{ width: "100%" }} />

        {/* Error notches overlaid on the bar */}
        {notches.map((n, i) => (
          <div
            key={`${n.status}-${i}`}
            className={`summary-timeline-notch summary-timeline-notch-${n.status}`}
            style={{ left: `${Math.min(Math.max(n.pct, 0.5), 99.5)}%` }}
            title={`${n.name} ${n.status} at ${formatTimestamp(n.timestamp_us)}`}
          />
        ))}
      </div>

      <div className="summary-timeline-stats">
        <span>{timeline.total_status_events} status events</span>
        <span>{timeline.total_message_events} message events</span>
        {notches.length > 0 && (
          <span>{notches.length} error{notches.length !== 1 ? "s" : ""}</span>
        )}
      </div>
    </div>
  );
}

function HierarchyBreakdown({ counts }: { counts: Summary["hierarchy_counts"] }) {
  const entries: Array<[string, number]> = [
    ["Host Meshes", counts.host_meshes],
    ["Proc Meshes", counts.proc_meshes],
    ["Actor Meshes", counts.actor_meshes],
  ];
  const total = entries.reduce((s, [, c]) => s + c, 0);

  return (
    <div className="summary-section" data-testid="mesh-breakdown">
      <h3 className="summary-section-title">Hierarchy Breakdown</h3>
      <div className="summary-mesh-chips">
        {entries.map(([label, count]) => (
          <div key={label} className="summary-mesh-chip">
            <span className="summary-mesh-chip-count">{count}</span>
            <span className="summary-mesh-chip-label">{label}</span>
            <span className="summary-mesh-chip-pct">
              {total > 0 ? ((count / total) * 100).toFixed(0) : 0}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Main component                                                      */
/* ------------------------------------------------------------------ */

export function SummaryView() {
  const { data, loading, error } = useApi<Summary>("/summary");

  if (loading) {
    return <div className="loading-state">Loading summary metrics...</div>;
  }

  if (error) {
    return <div className="error-state">Failed to load summary: {error}</div>;
  }

  if (!data) {
    return <div className="empty-state">No summary data available</div>;
  }

  return (
    <div className="summary-dashboard" data-testid="summary-dashboard">
      {/* Top row: overview cards */}
      <div className="summary-top-row">
        <OverviewCards data={data} />
      </div>

      {/* Timeline */}
      <TimelineBar timeline={data.timeline} errors={data.errors} />

      {/* Main grid: status + errors */}
      <div className="summary-grid-2col">
        <StatusBreakdown byStatus={data.actor_counts.by_status} />
        <ErrorPanel errors={data.errors} />
      </div>

      {/* Bottom grid: messages + mesh breakdown */}
      <div className="summary-grid-2col">
        <MessageTraffic counts={data.message_counts} />
        <HierarchyBreakdown counts={data.hierarchy_counts} />
      </div>
    </div>
  );
}
