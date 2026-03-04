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

/** Mesh tiers render as rounded rectangles; others as circles. */
const RECT_TIERS = new Set(["mesh", "proc_mesh", "actor_mesh"]);

/** Single node rendered as an SVG group. */
export function DagNodeComponent({
  node,
  selected,
  onSelect,
  onHover,
}: DagNodeProps) {
  const color = statusColor(node.status);
  const r = node.radius;
  const isRect = RECT_TIERS.has(node.tier);
  const isSmallNode = node.tier === "actor" || node.tier === "actor_mesh";

  // Truncate label for small nodes.
  const maxChars = isSmallNode ? 12 : 14;
  const displayLabel =
    node.label.length > maxChars
      ? node.label.slice(0, maxChars - 1) + "\u2026"
      : node.label;

  // Rectangle dimensions for mesh tiers.
  const w = r * 2.2;
  const h = r * 1.4;
  const rx = 6;

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
      {selected &&
        (isRect ? (
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
        ) : (
          <circle
            r={r + 6}
            fill="none"
            stroke={color}
            strokeWidth="1.5"
            opacity="0.4"
            className="dag-node-glow"
          />
        ))}

      {/* Outer status ring */}
      {isRect ? (
        <rect
          x={-w / 2}
          y={-h / 2}
          width={w}
          height={h}
          rx={rx}
          fill="var(--bg-secondary)"
          stroke={color}
          strokeWidth={borderStyle(node.status)}
          strokeDasharray={dashArray(node.status)}
          className="dag-node-rect"
        />
      ) : (
        <circle
          r={r}
          fill="var(--bg-secondary)"
          stroke={color}
          strokeWidth={borderStyle(node.status)}
          strokeDasharray={dashArray(node.status)}
          className="dag-node-circle"
        />
      )}

      {/* Inner fill - subtle tinted background */}
      {isRect ? (
        <rect
          x={-(w - 6) / 2}
          y={-(h - 6) / 2}
          width={w - 6}
          height={h - 6}
          rx={rx - 1}
          fill={color}
          opacity="0.08"
        />
      ) : (
        <circle r={r - 3} fill={color} opacity="0.08" />
      )}

      {/* Status dot at top-right (hidden for neutral n/a nodes) */}
      {node.status !== "n/a" && (
        <circle
          cx={isRect ? w / 2 - 4 : r * 0.65}
          cy={isRect ? -h / 2 + 4 : -r * 0.65}
          r={4}
          fill={color}
          className={
            node.status === "processing" || node.status === "idle"
              ? "dag-status-dot-pulse"
              : ""
          }
        />
      )}

      {/* Label */}
      <text
        textAnchor="middle"
        dy={isSmallNode ? "0.35em" : "-0.15em"}
        fill="var(--text-primary)"
        fontSize={isSmallNode ? "9px" : "10px"}
        fontFamily="var(--font-display)"
        fontWeight="500"
      >
        {displayLabel}
      </text>

      {/* Subtitle (tier label) - only for non-small nodes */}
      {!isSmallNode && (
        <text
          textAnchor="middle"
          dy="1.3em"
          fill="var(--text-muted)"
          fontSize="8px"
          fontFamily="var(--font-body)"
        >
          {node.subtitle}
        </text>
      )}
    </g>
  );
}
