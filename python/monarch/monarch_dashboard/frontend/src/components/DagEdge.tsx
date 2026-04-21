/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useMemo } from "react";
import { DagEdge as DagEdgeData, DagNode, DagDirection } from "../utils/dagLayout";

interface DagEdgeProps {
  edge: DagEdgeData;
  nodes: Map<string, DagNode>;
  direction: DagDirection;
}

/** All nodes are now rects; extent = half the rect height. */
function nodeExtent(node: DagNode): number {
  // Parent tiers use r*1.8, actors use r*1.6
  const isParent = ["host_mesh", "proc_mesh", "actor_mesh", "host", "proc"].includes(node.tier);
  return node.radius * (isParent ? 0.9 : 0.8);
}

/**
 * Renders an SVG path between two nodes.
 * Hierarchy edges: solid gray curves flowing in the layout direction.
 * Message edges: dashed, colored arcs offset from the main flow.
 */
export function DagEdgeComponent({ edge, nodes, direction }: DagEdgeProps) {
  const source = nodes.get(edge.sourceId);
  const target = nodes.get(edge.targetId);
  if (!source || !target) return null;

  const isMessage = edge.type === "message";
  const isLR = direction === "LR";

  const path = useMemo(() => {
    if (!isMessage) {
      if (isLR) {
        // Hierarchy LR: exit right side of source, enter left side of target.
        const sx = source.x + nodeExtent(source);
        const sy = source.y;
        const tx = target.x - nodeExtent(target);
        const ty = target.y;
        const dx = tx - sx;
        return `M ${sx} ${sy} C ${sx + dx * 0.4} ${sy}, ${tx - dx * 0.4} ${ty}, ${tx} ${ty}`;
      }
      // Hierarchy TB: exit bottom of source, enter top of target.
      const sx = source.x;
      const sy = source.y + nodeExtent(source);
      const tx = target.x;
      const ty = target.y - nodeExtent(target);
      const dy = ty - sy;
      return `M ${sx} ${sy} C ${sx} ${sy + dy * 0.4}, ${tx} ${ty - dy * 0.4}, ${tx} ${ty}`;
    }

    // Message: smooth rounded arc offset from the main flow.
    const dx = target.x - source.x;
    const dy = target.y - source.y;
    const angle = Math.atan2(dy, dx);
    const sx = source.x + Math.cos(angle) * source.radius;
    const sy = source.y + Math.sin(angle) * source.radius;
    const tx = target.x - Math.cos(angle) * target.radius;
    const ty = target.y - Math.sin(angle) * target.radius;

    if (isLR) {
      const sag = Math.max(40, Math.abs(dy) * 0.35);
      const rightX = Math.max(sx, tx) + sag;
      return `M ${sx} ${sy} Q ${rightX} ${(sy + ty) / 2}, ${tx} ${ty}`;
    }
    const sag = Math.max(40, Math.abs(dx) * 0.35);
    const belowY = Math.max(sy, ty) + sag;
    return `M ${sx} ${sy} Q ${(sx + tx) / 2} ${belowY}, ${tx} ${ty}`;
  }, [source, target, isMessage, isLR]);

  if (isMessage) {
    return (
      <path
        d={path}
        fill="none"
        stroke="var(--accent-secondary)"
        strokeWidth="1.5"
        strokeDasharray="5 4"
        opacity="0.5"
        className="dag-message-edge"
        data-testid={`dag-edge-${edge.id}`}
      />
    );
  }

  return (
    <path
      d={path}
      fill="none"
      stroke="var(--text-muted)"
      strokeWidth="1.5"
      opacity="0.8"
      data-testid={`dag-edge-${edge.id}`}
    />
  );
}
