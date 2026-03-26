/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState, useRef, useCallback, useMemo, useEffect } from "react";
import { useApi } from "../hooks/useApi";
import { computeLayout, ApiDagData, DagNode, DagGraph, TIER_Y, TIER_LABELS, DagTier } from "../utils/dagLayout";
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
 * Full DAG visualization of the Monarch hierarchy.
 *
 * Fetches pre-classified nodes and edges from /api/dag, then uses
 * computeLayout to assign X/Y positions for SVG rendering.
 */
export function DagView() {
  const { data: dagData, loading } = useApi<ApiDagData>("/dag");

  const [selectedNode, setSelectedNode] = useState<DagNode | null>(null);
  const [tooltip, setTooltip] = useState<Tooltip | null>(null);
  const [viewBox, setViewBox] = useState({ x: 0, y: 0, w: 1300, h: 800 });
  const svgRef = useRef<SVGSVGElement>(null);
  const isPanning = useRef(false);
  const panStart = useRef({ x: 0, y: 0, vx: 0, vy: 0 });

  const graph: DagGraph | null = useMemo(() => {
    if (!dagData) return null;
    return computeLayout(dagData);
  }, [dagData]);

  const viewInitialized = useRef(false);
  useEffect(() => {
    if (graph && !viewInitialized.current) {
      viewInitialized.current = true;
      setViewBox({ x: -20, y: -20, w: graph.width + 40, h: Math.min(graph.height + 40, 800) });
    }
  }, [graph]);

  useEffect(() => {
    if (selectedNode && graph) {
      const updated = graph.nodes.find((n) => n.id === selectedNode.id);
      if (updated && updated !== selectedNode) setSelectedNode(updated);
      else if (!updated) setSelectedNode(null);
    }
  }, [graph, selectedNode]);

  const nodeMap = useMemo(() => {
    if (!graph) return new Map<string, DagNode>();
    const m = new Map<string, DagNode>();
    for (const n of graph.nodes) m.set(n.id, n);
    return m;
  }, [graph]);

  const handleWheel = useCallback(
    (e: React.WheelEvent) => {
      e.preventDefault();
      const factor = e.deltaY > 0 ? 1.1 : 0.9;
      const svg = svgRef.current;
      if (!svg) return;
      const rect = svg.getBoundingClientRect();
      const mx = ((e.clientX - rect.left) / rect.width) * viewBox.w + viewBox.x;
      const my = ((e.clientY - rect.top) / rect.height) * viewBox.h + viewBox.y;
      setViewBox((vb) => ({
        x: mx - (mx - vb.x) * factor,
        y: my - (my - vb.y) * factor,
        w: vb.w * factor,
        h: vb.h * factor,
      }));
    },
    [viewBox]
  );

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (e.button !== 0) return;
      isPanning.current = true;
      panStart.current = { x: e.clientX, y: e.clientY, vx: viewBox.x, vy: viewBox.y };
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
      setViewBox((vb) => ({ ...vb, x: panStart.current.vx - dx, y: panStart.current.vy - dy }));
    },
    [viewBox]
  );

  const handleMouseUp = useCallback(() => { isPanning.current = false; }, []);

  const handleNodeSelect = useCallback((node: DagNode) => {
    setSelectedNode((prev) => (prev?.id === node.id ? null : node));
  }, []);

  const handleNodeHover = useCallback((node: DagNode | null) => {
    if (!node) { setTooltip(null); return; }
    setTooltip({ node, x: node.x, y: node.y + node.radius + 10 });
  }, []);

  if (loading) return <div className="loading-state">Loading DAG data...</div>;
  if (!graph || graph.nodes.length === 0) return <div className="empty-state">No data available</div>;

  const hierEdges = graph.edges.filter((e) => e.type === "hierarchy");
  const msgEdges = graph.edges.filter((e) => e.type === "message");
  const tierEntries = Object.entries(TIER_LABELS) as Array<[DagTier, string]>;

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
            <pattern id="dag-grid" width="40" height="40" patternUnits="userSpaceOnUse">
              <path d="M 40 0 L 0 0 0 40" fill="none" stroke="var(--border-subtle)" strokeWidth="0.3" opacity="0.4" />
            </pattern>
            <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="3" result="blur" />
              <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
            </filter>
          </defs>
          <rect x={viewBox.x - 1000} y={viewBox.y - 1000} width={viewBox.w + 2000} height={viewBox.h + 2000} fill="url(#dag-grid)" />
          {tierEntries.map(([tier, label]) => (
            <text key={tier} x={15} y={TIER_Y[tier]} textAnchor="start" dominantBaseline="middle" fill="var(--text-muted)" fontSize="10" fontFamily="var(--font-display)" opacity="0.5">{label}</text>
          ))}
          <g className="dag-edges-hierarchy">{hierEdges.map((e) => <DagEdgeComponent key={e.id} edge={e} nodes={nodeMap} />)}</g>
          <g className="dag-edges-messages">{msgEdges.map((e) => <DagEdgeComponent key={e.id} edge={e} nodes={nodeMap} />)}</g>
          <g className="dag-nodes">
            {graph.nodes.map((node) => (
              <DagNodeComponent key={node.id} node={node} selected={selectedNode?.id === node.id} onSelect={handleNodeSelect} onHover={handleNodeHover} />
            ))}
          </g>
        </svg>
        <DagLegend />
      </div>
      {selectedNode && <NodeDetail node={selectedNode} onClose={() => setSelectedNode(null)} />}
    </div>
  );
}
