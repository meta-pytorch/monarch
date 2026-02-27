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
import { statusColor, formatTimestamp } from "../utils/status";
import { StatusBadge } from "./StatusBadge";

/* ------------------------------------------------------------------ */
/* Health score helpers                                                */
/* ------------------------------------------------------------------ */

function healthLabel(score: number): string {
  if (score >= 90) return "Excellent";
  if (score >= 70) return "Good";
  if (score >= 50) return "Degraded";
  if (score >= 30) return "Poor";
  return "Critical";
}

function healthColor(score: number): string {
  if (score >= 90) return "var(--status-healthy)";
  if (score >= 70) return "var(--status-processing)";
  if (score >= 50) return "var(--status-transitional)";
  return "var(--status-failed)";
}

/* ------------------------------------------------------------------ */
/* Sub-components                                                      */
/* ------------------------------------------------------------------ */

function HealthGauge({ score }: { score: number }) {
  const color = healthColor(score);
  const circumference = 2 * Math.PI * 54;
  const offset = circumference * (1 - score / 100);

  return (
    <div className="summary-health-gauge" data-testid="health-gauge">
      <svg viewBox="0 0 120 120" width="120" height="120">
        {/* Background ring */}
        <circle
          cx="60" cy="60" r="54"
          fill="none"
          stroke="var(--bg-tertiary)"
          strokeWidth="8"
        />
        {/* Score arc */}
        <circle
          cx="60" cy="60" r="54"
          fill="none"
          stroke={color}
          strokeWidth="8"
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          transform="rotate(-90 60 60)"
          style={{ transition: "stroke-dashoffset 0.8s ease" }}
        />
        {/* Score text */}
        <text
          x="60" y="54" textAnchor="middle"
          fill={color} fontSize="28" fontWeight="700"
          fontFamily="var(--font-display)"
        >
          {score}
        </text>
        <text
          x="60" y="74" textAnchor="middle"
          fill="var(--text-muted)" fontSize="10"
          fontFamily="var(--font-body)"
        >
          {healthLabel(score)}
        </text>
      </svg>
    </div>
  );
}

function OverviewCards({ data }: { data: Summary }) {
  const cards = [
    { label: "Meshes", value: data.mesh_counts.total, sub: `${Object.keys(data.mesh_counts.by_class).length} types` },
    { label: "Actors", value: data.actor_counts.total, sub: `${Object.keys(data.actor_counts.by_status).length} statuses` },
    { label: "Messages", value: data.message_counts.total, sub: `${(data.message_counts.delivery_rate * 100).toFixed(1)}% delivered` },
    { label: "Events", value: data.timeline.total_status_events + data.timeline.total_message_events, sub: "status + message" },
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

      {errors.failed_actors.length > 0 && (
        <div className="summary-error-group">
          <h4 className="summary-error-heading">Failed Actors</h4>
          {errors.failed_actors.map((a) => (
            <div key={a.actor_id} className="summary-error-item">
              <div className="summary-error-name">
                {a.full_name.split("/").pop()}
              </div>
              <div className="summary-error-detail">
                <span className="summary-error-reason">
                  {a.reason ?? "unknown reason"}
                </span>
                <span className="summary-error-time">
                  {formatTimestamp(a.timestamp_us)}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}

      {errors.stopped_actors.length > 0 && (
        <div className="summary-error-group">
          <h4 className="summary-error-heading">
            Stopped Actors
            <span className="count-badge">{errors.stopped_actors.length}</span>
          </h4>
          {errors.stopped_actors.map((a) => (
            <div key={a.actor_id} className="summary-error-item">
              <div className="summary-error-name">
                {a.full_name.split("/").pop()}
              </div>
              <div className="summary-error-detail">
                <span className="summary-error-reason">
                  {a.reason ?? "stopped"}
                </span>
              </div>
            </div>
          ))}
        </div>
      )}

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
              style={{ background: statusColor(status === "delivered" ? "idle" : status === "sent" ? "processing" : status) }}
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

function TimelineBar({ timeline }: { timeline: Summary["timeline"] }) {
  const duration = timeline.end_us - timeline.start_us;
  const failurePct =
    timeline.failure_onset_us != null
      ? ((timeline.failure_onset_us - timeline.start_us) / duration) * 100
      : null;

  return (
    <div className="summary-section" data-testid="timeline-bar">
      <h3 className="summary-section-title">Session Timeline</h3>

      <div className="summary-timeline-info">
        <span>Start: {formatTimestamp(timeline.start_us)}</span>
        <span>End: {formatTimestamp(timeline.end_us)}</span>
        <span>Duration: {((duration / 1_000_000) / 60).toFixed(1)} min</span>
      </div>

      <div className="summary-timeline-track">
        {/* Healthy region */}
        <div
          className="summary-timeline-healthy"
          style={{ width: failurePct != null ? `${failurePct}%` : "100%" }}
        />
        {/* Failure region */}
        {failurePct != null && (
          <div
            className="summary-timeline-failed"
            style={{ width: `${100 - failurePct}%` }}
          />
        )}
        {/* Failure marker */}
        {failurePct != null && (
          <div
            className="summary-timeline-marker"
            style={{ left: `${failurePct}%` }}
            title={`Failure onset: ${formatTimestamp(timeline.failure_onset_us!)}`}
          >
            <div className="summary-timeline-marker-line" />
            <div className="summary-timeline-marker-label">FAILURE</div>
          </div>
        )}
      </div>

      <div className="summary-timeline-stats">
        <span>{timeline.total_status_events} status events</span>
        <span>{timeline.total_message_events} message events</span>
      </div>
    </div>
  );
}

function MeshBreakdown({ byClass }: { byClass: Record<string, number> }) {
  const entries = Object.entries(byClass).sort((a, b) => b[1] - a[1]);
  const total = entries.reduce((s, [, c]) => s + c, 0);

  return (
    <div className="summary-section" data-testid="mesh-breakdown">
      <h3 className="summary-section-title">Mesh Hierarchy</h3>
      <div className="summary-mesh-chips">
        {entries.map(([cls, count]) => (
          <div key={cls} className="summary-mesh-chip">
            <span className="summary-mesh-chip-count">{count}</span>
            <span className="summary-mesh-chip-label">{cls}</span>
            <span className="summary-mesh-chip-pct">
              {((count / total) * 100).toFixed(0)}%
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
      {/* Top row: Health gauge + overview cards */}
      <div className="summary-top-row">
        <div className="summary-health-card">
          <h3 className="summary-section-title">System Health</h3>
          <HealthGauge score={data.health_score} />
        </div>
        <OverviewCards data={data} />
      </div>

      {/* Timeline */}
      <TimelineBar timeline={data.timeline} />

      {/* Main grid: status + errors */}
      <div className="summary-grid-2col">
        <StatusBreakdown byStatus={data.actor_counts.by_status} />
        <ErrorPanel errors={data.errors} />
      </div>

      {/* Bottom grid: messages + mesh breakdown */}
      <div className="summary-grid-2col">
        <MessageTraffic counts={data.message_counts} />
        <MeshBreakdown byClass={data.mesh_counts.by_class} />
      </div>
    </div>
  );
}
