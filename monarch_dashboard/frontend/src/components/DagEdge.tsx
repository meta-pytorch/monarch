/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useMemo } from "react";
import { DagEdge as DagEdgeData, DagNode } from "../utils/dagLayout";

interface DagEdgeProps {
  edge: DagEdgeData;
  nodes: Map<string, DagNode>;
}

/** Mesh tiers render as rects (half-height = r * 0.7). */
const RECT_TIERS = new Set(["mesh", "proc_mesh", "actor_mesh"]);

function bottomY(node: DagNode): number {
  return node.y + (RECT_TIERS.has(node.tier) ? node.radius * 0.7 : node.radius);
}

function topY(node: DagNode): number {
  return node.y - (RECT_TIERS.has(node.tier) ? node.radius * 0.7 : node.radius);
}

/**
 * Renders an SVG path between two nodes.
 * Hierarchy edges: solid gray curves exiting downward.
 * Message edges: dashed, colored arcs below the actor row.
 */
export function DagEdgeComponent({ edge, nodes }: DagEdgeProps) {
  const source = nodes.get(edge.sourceId);
  const target = nodes.get(edge.targetId);
  if (!source || !target) return null;

  const isMessage = edge.type === "message";

  const path = useMemo(() => {
    if (!isMessage) {
      // Hierarchy: exit bottom of source, enter top of target.
      const sx = source.x;
      const sy = bottomY(source);
      const tx = target.x;
      const ty = topY(target);
      const dy = ty - sy;
      return `M ${sx} ${sy} C ${sx} ${sy + dy * 0.4}, ${tx} ${ty - dy * 0.4}, ${tx} ${ty}`;
    }

    // Message: both nodes typically at the same Y row — arc below.
    const dx = target.x - source.x;
    const dy = target.y - source.y;
    const angle = Math.atan2(dy, dx);
    const sx = source.x + Math.cos(angle) * source.radius;
    const sy = source.y + Math.sin(angle) * source.radius;
    const tx = target.x - Math.cos(angle) * target.radius;
    const ty = target.y - Math.sin(angle) * target.radius;
    const sag = Math.max(30, Math.abs(dx) * 0.25);
    const belowY = Math.max(sy, ty) + sag;
    return `M ${sx} ${sy} C ${sx} ${belowY}, ${tx} ${belowY}, ${tx} ${ty}`;
  }, [source, target, isMessage]);

  if (isMessage) {
    return (
      <path
        d={path}
        fill="none"
        stroke="var(--accent-secondary)"
        strokeWidth="1"
        strokeDasharray="5 4"
        opacity="0.35"
        className="dag-message-edge"
        data-testid={`dag-edge-${edge.id}`}
      />
    );
  }

  return (
    <path
      d={path}
      fill="none"
      stroke="var(--border-subtle)"
      strokeWidth="1.5"
      opacity="0.7"
      data-testid={`dag-edge-${edge.id}`}
    />
  );
}
