/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { DagNode as DagNodeData } from "../utils/dagLayout";
import { statusColor } from "../utils/status";

interface DagNodeProps {
  node: DagNodeData;
  selected: boolean;
  onSelect: (node: DagNodeData) => void;
  onHover: (node: DagNodeData | null) => void;
  /** If set, shows a badge indicating this node has hidden children. */
  hiddenChildCount?: number;
}

/** Border style by status category. */
function borderStyle(status: string): string {
  switch (status) {
    case "failed":
      return "3";
    case "stopped":
      return "2.5";
    default:
      return "2";
  }
}

function dashArray(status: string): string | undefined {
  const transitional = [
    "created",
    "initializing",
    "saving",
    "loading",
    "stopping",
  ];
  if (transitional.includes(status)) return "4 3";
  return undefined;
}

/** Single node rendered as an SVG group.
 *
 * All nodes render as rounded rectangles with two lines of text:
 * a tier title (e.g. "Host", "Proc", "Actor") and a short name
 * derived from the label. The full entity_id is shown on hover.
 */
export function DagNodeComponent({
  node,
  selected,
  onSelect,
  onHover,
  hiddenChildCount,
}: DagNodeProps) {
  const color = statusColor(node.status);
  const r = node.radius;

  // Strip "-UUID" suffix and "[rank]" to get a short name.
  // e.g. "anon_0-13su5vJ2cg5v" -> "anon_0"
  //      "greeter-1yVhEBwKxr3i[0]" -> "greeter"
  //      "host_agent[0]" -> "host_agent"
  const shortName = node.label
    .replace(/-[A-Za-z0-9]{8,}(\[\d+\])?$/, "")
    .replace(/\[\d+\]$/, "");
  const shortNameMax = 14;
  const shortNameDisplay = shortName.length > shortNameMax
    ? shortName.slice(0, shortNameMax - 1) + "\u2026"
    : shortName;

  // All nodes show tier title + short name (2 lines).
  const w = Math.max(r * 2.2, shortNameDisplay.length * 6 + 20);
  const h = r * 1.8;
  const rx = 6;

  const displayText = node.subtitle;
  const maxChars = 16;
  const truncated =
    displayText.length > maxChars
      ? displayText.slice(0, maxChars - 1) + "\u2026"
      : displayText;

  return (
    <g
      className="dag-node-group"
      transform={`translate(${node.x}, ${node.y})`}
      onClick={() => onSelect(node)}
      onMouseEnter={() => onHover(node)}
      onMouseLeave={() => onHover(null)}
      style={{ cursor: "pointer" }}
      data-testid={`dag-node-${node.id}`}
    >
      {/* Glow ring for selected node */}
      {selected && (
        <rect
          x={-(w + 12) / 2}
          y={-(h + 12) / 2}
          width={w + 12}
          height={h + 12}
          rx={rx + 2}
          fill="none"
          stroke={color}
          strokeWidth="1.5"
          opacity="0.4"
          className="dag-node-glow"
        />
      )}

      {/* Outer status ring */}
      <rect
        x={-w / 2}
        y={-h / 2}
        width={w}
        height={h}
        rx={rx}
        fill="var(--bg-tertiary)"
        stroke={color}
        strokeWidth={borderStyle(node.status)}
        strokeDasharray={dashArray(node.status)}
        className="dag-node-rect"
      />

      {/* Inner fill - tinted background */}
      <rect
        x={-(w - 6) / 2}
        y={-(h - 6) / 2}
        width={w - 6}
        height={h - 6}
        rx={rx - 1}
        fill={color}
        opacity="0.15"
      />

      {/* Status dot at top-right (hidden for neutral n/a nodes) */}
      {node.status !== "n/a" && (
        <circle
          cx={w / 2 - 4}
          cy={-h / 2 + 4}
          r={4}
          fill={color}
          className={
            node.status === "processing" || node.status === "idle"
              ? "dag-status-dot-pulse"
              : ""
          }
        />
      )}

      {/* Primary text: tier title + short name */}
      <text
        textAnchor="middle"
        dy="-0.2em"
        fill="#fff"
        fontSize="10px"
        fontFamily="var(--font-display)"
        fontWeight="600"
      >
        {truncated}
      </text>
      {shortNameDisplay && (
        <text
          textAnchor="middle"
          dy="1.1em"
          fill="var(--text-secondary)"
          fontSize="8px"
          fontFamily="var(--font-display)"
          fontWeight="400"
        >
          {shortNameDisplay}
        </text>
      )}

      {/* Expand badge: shows hidden child count on collapsed nodes */}
      {hiddenChildCount != null && hiddenChildCount > 0 && (
        <g>
          <circle
            cx={w / 2 + 2}
            cy={h / 2 + 2}
            r={10}
            fill="var(--accent-primary)"
          />
          <text
            x={w / 2 + 2}
            y={h / 2 + 2}
            textAnchor="middle"
            dominantBaseline="central"
            fill="#fff"
            fontSize="8px"
            fontFamily="var(--font-display)"
            fontWeight="700"
          >
            +{hiddenChildCount}
          </text>
        </g>
      )}
    </g>
  );
}
