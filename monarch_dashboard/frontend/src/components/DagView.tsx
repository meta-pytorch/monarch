/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState, useRef, useCallback, useMemo, useEffect } from "react";
import { Mesh, Actor, Message } from "../types";
import { useApi } from "../hooks/useApi";
import { computeLayout, DagNode, DagGraph } from "../utils/dagLayout";
import { DagNodeComponent } from "./DagNode";
import { DagEdgeComponent } from "./DagEdge";
import { DagLegend } from "./DagLegend";
import { NodeDetail } from "./NodeDetail";

/** Tooltip state for hover. */
interface Tooltip {
  node: DagNode;
  x: number;
  y: number;
}

/**
 * Full DAG visualization of the Monarch mesh hierarchy.
 *
 * Renders an interactive SVG with zoom/pan, clickable nodes,
 * hover tooltips, and a slide-in detail panel.
 */
export function DagView() {
  // Fetch all data needed for the graph.
  const { data: meshes, loading: mLoading } = useApi<Mesh[]>("/meshes");
  const { data: actors, loading: aLoading } = useApi<Actor[]>("/actors");
  const { data: messages, loading: msgLoading } = useApi<Message[]>("/messages");

  // UI state.
  const [selectedNode, setSelectedNode] = useState<DagNode | null>(null);
  const [tooltip, setTooltip] = useState<Tooltip | null>(null);

  // Pan/zoom state.
  const [viewBox, setViewBox] = useState({ x: 0, y: 0, w: 1060, h: 800 });
  const svgRef = useRef<SVGSVGElement>(null);
  const isPanning = useRef(false);
  const panStart = useRef({ x: 0, y: 0, vx: 0, vy: 0 });

  // Compute graph layout.
  const graph: DagGraph | null = useMemo(() => {
    if (!meshes || !actors) return null;

    // Build actor status map from latest_status on actor objects.
    const actorStatuses: Record<number, string> = {};
    for (const a of actors) {
      actorStatuses[a.id] = a.latest_status ?? "unknown";
    }

    // Build message pairs.
    const messagePairs: Array<[number, number]> = (messages ?? []).map((m) => [
      m.from_actor_id,
      m.to_actor_id,
    ]);

    return computeLayout(meshes, actors, actorStatuses, messagePairs);
  }, [meshes, actors, messages]);

  // Fit view on initial load.
  useEffect(() => {
    if (graph) {
      setViewBox({
        x: -20,
        y: -20,
        w: graph.width + 40,
        h: graph.height + 40,
      });
    }
  }, [graph]);

  // Node lookup map for edge rendering.
  const nodeMap = useMemo(() => {
    if (!graph) return new Map<string, DagNode>();
    const m = new Map<string, DagNode>();
    for (const n of graph.nodes) m.set(n.id, n);
    return m;
  }, [graph]);

  // -- Zoom handler --
  const handleWheel = useCallback(
    (e: React.WheelEvent) => {
      e.preventDefault();
      const factor = e.deltaY > 0 ? 1.1 : 0.9;
      const svg = svgRef.current;
      if (!svg) return;

      const rect = svg.getBoundingClientRect();
      const mx = ((e.clientX - rect.left) / rect.width) * viewBox.w + viewBox.x;
      const my = ((e.clientY - rect.top) / rect.height) * viewBox.h + viewBox.y;

      setViewBox((vb) => {
        const nw = vb.w * factor;
        const nh = vb.h * factor;
        return {
          x: mx - (mx - vb.x) * factor,
          y: my - (my - vb.y) * factor,
          w: nw,
          h: nh,
        };
      });
    },
    [viewBox]
  );

  // -- Pan handlers --
  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (e.button !== 0) return;
      isPanning.current = true;
      panStart.current = {
        x: e.clientX,
        y: e.clientY,
        vx: viewBox.x,
        vy: viewBox.y,
      };
    },
    [viewBox]
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (!isPanning.current) return;
      const svg = svgRef.current;
      if (!svg) return;

      const rect = svg.getBoundingClientRect();
      const dx = ((e.clientX - panStart.current.x) / rect.width) * viewBox.w;
      const dy = ((e.clientY - panStart.current.y) / rect.height) * viewBox.h;

      setViewBox((vb) => ({
        ...vb,
        x: panStart.current.vx - dx,
        y: panStart.current.vy - dy,
      }));
    },
    [viewBox]
  );

  const handleMouseUp = useCallback(() => {
    isPanning.current = false;
  }, []);

  // -- Node interaction --
  const handleNodeSelect = useCallback((node: DagNode) => {
    setSelectedNode((prev) => (prev?.id === node.id ? null : node));
  }, []);

  const handleNodeHover = useCallback(
    (node: DagNode | null) => {
      if (!node) {
        setTooltip(null);
        return;
      }
      // Position tooltip relative to node.
      setTooltip({ node, x: node.x, y: node.y - node.radius - 14 });
    },
    []
  );

  // -- Loading / Error --
  if (mLoading || aLoading || msgLoading) {
    return <div className="loading-state">Loading DAG data...</div>;
  }

  if (!graph || graph.nodes.length === 0) {
    return <div className="empty-state">No mesh data available</div>;
  }

  // Separate hierarchy and message edges for layering.
  const hierEdges = graph.edges.filter((e) => e.type === "hierarchy");
  const msgEdges = graph.edges.filter((e) => e.type === "message");

  return (
    <div className="dag-container" data-testid="dag-container">
      <div className="dag-canvas-wrapper">
        <svg
          ref={svgRef}
          className="dag-svg"
          viewBox={`${viewBox.x} ${viewBox.y} ${viewBox.w} ${viewBox.h}`}
          preserveAspectRatio="xMidYMid meet"
          onWheel={handleWheel}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMouseMove}
          onMouseUp={handleMouseUp}
          onMouseLeave={handleMouseUp}
        >
          <defs>
            {/* Subtle grid pattern for background depth */}
            <pattern
              id="dag-grid"
              width="40"
              height="40"
              patternUnits="userSpaceOnUse"
            >
              <path
                d="M 40 0 L 0 0 0 40"
                fill="none"
                stroke="var(--border-subtle)"
                strokeWidth="0.3"
                opacity="0.4"
              />
            </pattern>

            {/* Glow filter for selected nodes */}
            <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="3" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          {/* Background grid */}
          <rect
            x={viewBox.x - 1000}
            y={viewBox.y - 1000}
            width={viewBox.w + 2000}
            height={viewBox.h + 2000}
            fill="url(#dag-grid)"
          />

          {/* Tier labels */}
          <text
            x={120}
            y={30}
            textAnchor="middle"
            fill="var(--text-muted)"
            fontSize="11"
            fontFamily="var(--font-display)"
            opacity="0.5"
          >
            HOST MESHES
          </text>
          <text
            x={380}
            y={30}
            textAnchor="middle"
            fill="var(--text-muted)"
            fontSize="11"
            fontFamily="var(--font-display)"
            opacity="0.5"
          >
            PROC MESHES
          </text>
          <text
            x={640}
            y={30}
            textAnchor="middle"
            fill="var(--text-muted)"
            fontSize="11"
            fontFamily="var(--font-display)"
            opacity="0.5"
          >
            ACTOR MESHES
          </text>
          <text
            x={900}
            y={30}
            textAnchor="middle"
            fill="var(--text-muted)"
            fontSize="11"
            fontFamily="var(--font-display)"
            opacity="0.5"
          >
            ACTORS
          </text>

          {/* Edges: hierarchy first (below), messages on top */}
          <g className="dag-edges-hierarchy">
            {hierEdges.map((e) => (
              <DagEdgeComponent key={e.id} edge={e} nodes={nodeMap} />
            ))}
          </g>
          <g className="dag-edges-messages">
            {msgEdges.map((e) => (
              <DagEdgeComponent key={e.id} edge={e} nodes={nodeMap} />
            ))}
          </g>

          {/* Nodes */}
          <g className="dag-nodes">
            {graph.nodes.map((node) => (
              <DagNodeComponent
                key={node.id}
                node={node}
                selected={selectedNode?.id === node.id}
                onSelect={handleNodeSelect}
                onHover={handleNodeHover}
              />
            ))}
          </g>
        </svg>

        {/* Hover tooltip */}
        {tooltip && !selectedNode && (
          <div
            className="dag-tooltip"
            style={{
              left: `${((tooltip.x - viewBox.x) / viewBox.w) * 100}%`,
              top: `${((tooltip.y - viewBox.y) / viewBox.h) * 100}%`,
            }}
          >
            <div className="dag-tooltip-name">{tooltip.node.label}</div>
            <div className="dag-tooltip-info">
              {tooltip.node.subtitle} &middot; {tooltip.node.status}
            </div>
          </div>
        )}

        {/* Legend */}
        <DagLegend />
      </div>

      {/* Detail panel */}
      {selectedNode && (
        <NodeDetail
          node={selectedNode}
          onClose={() => setSelectedNode(null)}
        />
      )}
    </div>
  );
}
