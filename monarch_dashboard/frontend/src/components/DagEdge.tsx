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

/**
 * Renders an SVG path between two nodes.
 * Hierarchy edges: solid gray curves.
 * Message edges: dashed, colored, animated.
 */
export function DagEdgeComponent({ edge, nodes }: DagEdgeProps) {
  const source = nodes.get(edge.sourceId);
  const target = nodes.get(edge.targetId);
  if (!source || !target) return null;

  const isMessage = edge.type === "message";

  // Compute a smooth cubic bezier curve between source and target.
  const path = useMemo(() => {
    const dx = target.x - source.x;
    const dy = target.y - source.y;
    const dist = Math.sqrt(dx * dx + dy * dy);

    // Offset start/end to the edge of circles.
    const angle = Math.atan2(dy, dx);
    const sx = source.x + Math.cos(angle) * source.radius;
    const sy = source.y + Math.sin(angle) * source.radius;
    const tx = target.x - Math.cos(angle) * target.radius;
    const ty = target.y - Math.sin(angle) * target.radius;

    // Control points for a gentle curve.
    const cx = dist * 0.35;
    const cp1x = sx + cx;
    const cp1y = sy;
    const cp2x = tx - cx;
    const cp2y = ty;

    return `M ${sx} ${sy} C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${tx} ${ty}`;
  }, [source, target]);

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
