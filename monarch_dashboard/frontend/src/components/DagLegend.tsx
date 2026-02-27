/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";

const LEGEND_ITEMS = [
  { color: "var(--status-healthy)", label: "Idle" },
  { color: "var(--status-processing)", label: "Processing" },
  { color: "var(--status-transitional)", label: "Transitional" },
  { color: "var(--status-failed)", label: "Failed" },
  { color: "var(--status-stopped)", label: "Stopped" },
];

const TIER_ITEMS = [
  { radius: 14, label: "Host Mesh" },
  { radius: 11, label: "Proc Mesh" },
  { radius: 9, label: "Actor Mesh" },
  { radius: 7, label: "Actor" },
];

/** Top-right legend overlay for the DAG view. */
export function DagLegend() {
  return (
    <div className="dag-legend" data-testid="dag-legend">
      <div className="dag-legend-title">Legend</div>

      <div className="dag-legend-section">
        <div className="dag-legend-heading">Status</div>
        {LEGEND_ITEMS.map((item) => (
          <div key={item.label} className="dag-legend-item">
            <span
              className="dag-legend-swatch"
              style={{ background: item.color }}
            />
            <span className="dag-legend-label">{item.label}</span>
          </div>
        ))}
      </div>

      <div className="dag-legend-section">
        <div className="dag-legend-heading">Node Type</div>
        {TIER_ITEMS.map((item) => (
          <div key={item.label} className="dag-legend-item">
            <svg width="28" height="28" viewBox="0 0 28 28">
              <circle
                cx="14"
                cy="14"
                r={item.radius}
                fill="none"
                stroke="var(--text-muted)"
                strokeWidth="1.5"
              />
            </svg>
            <span className="dag-legend-label">{item.label}</span>
          </div>
        ))}
      </div>

      <div className="dag-legend-section">
        <div className="dag-legend-heading">Edges</div>
        <div className="dag-legend-item">
          <svg width="28" height="12">
            <line
              x1="2"
              y1="6"
              x2="26"
              y2="6"
              stroke="var(--border-subtle)"
              strokeWidth="1.5"
            />
          </svg>
          <span className="dag-legend-label">Hierarchy</span>
        </div>
        <div className="dag-legend-item">
          <svg width="28" height="12">
            <line
              x1="2"
              y1="6"
              x2="26"
              y2="6"
              stroke="var(--accent-secondary)"
              strokeWidth="1"
              strokeDasharray="4 3"
              opacity="0.6"
            />
          </svg>
          <span className="dag-legend-label">Message</span>
        </div>
      </div>
    </div>
  );
}
