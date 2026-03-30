/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState, useRef, useCallback, useMemo, useEffect } from "react";
import { useApi } from "../hooks/useApi";
import {
  computeLayout, ApiDagData, ApiDagEdge, DagNode, DagGraph,
  DagDirection, TIER_POSITION, TIER_LABELS, DagTier,
} from "../utils/dagLayout";
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
 * Build a parent→children map and a set of root IDs from hierarchy edges.
 */
function buildTree(data: ApiDagData) {
  const children: Record<string, string[]> = {};
  const hasParent = new Set<string>();
  for (const e of data.edges) {
    if (e.type !== "hierarchy") continue;
    (children[e.source_id] ??= []).push(e.target_id);
    hasParent.add(e.target_id);
  }
  const roots = data.nodes
    .filter((n) => !hasParent.has(n.id))
    .map((n) => n.id);
  return { children, roots };
}

/**
 * Compute the set of visible node IDs given the expanded set.
 */
function visibleNodes(
  roots: string[],
  children: Record<string, string[]>,
  expanded: Set<string>,
): Set<string> {
  const visible = new Set<string>();
  const queue = [...roots];
  while (queue.length > 0) {
    const id = queue.shift()!;
    visible.add(id);
    if (expanded.has(id)) {
      for (const kid of children[id] ?? []) {
        queue.push(kid);
      }
    }
  }
  return visible;
}

/**
 * Count all descendants (recursively) of a node.
 */
function countDescendants(
  id: string,
  children: Record<string, string[]>,
): number {
  const kids = children[id] ?? [];
  let count = kids.length;
  for (const kid of kids) {
    count += countDescendants(kid, children);
  }
  return count;
}

/**
 * Compute a viewBox that fits nodes and message arcs into the container.
 *
 * Message arcs sag below (TB) or right (LR) of the bottom-most nodes.
 * We estimate the sag to ensure arcs aren't clipped.
 */
function computeFitViewBox(
  nodes: DagNode[],
  containerRect: DOMRect,
  graph?: DagGraph | null,
): { x: number; y: number; w: number; h: number } | null {
  if (nodes.length === 0 || containerRect.width === 0 || containerRect.height === 0) return null;
  const containerAspect = containerRect.width / containerRect.height;
  const isLR = graph?.direction === "LR";

  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (const n of nodes) {
    const r = n.radius + 10;
    minX = Math.min(minX, n.x - r);
    maxX = Math.max(maxX, n.x + r);
    minY = Math.min(minY, n.y - r);
    maxY = Math.max(maxY, n.y + r);
  }

  // Estimate message arc sag. Message arcs use:
  //   TB: belowY = max(sy, ty) + max(40, |dx| * 0.35)
  //   LR: rightX = max(sx, tx) + max(40, |dy| * 0.35)
  if (graph) {
    const nodeMap = new Map<string, DagNode>();
    for (const n of nodes) nodeMap.set(n.id, n);
    for (const e of graph.edges) {
      if (e.type !== "message") continue;
      const src = nodeMap.get(e.sourceId);
      const tgt = nodeMap.get(e.targetId);
      if (!src || !tgt) continue;
      if (isLR) {
        const sag = Math.max(40, Math.abs(tgt.y - src.y) * 0.35);
        maxX = Math.max(maxX, Math.max(src.x, tgt.x) + src.radius + sag);
      } else {
        const sag = Math.max(40, Math.abs(tgt.x - src.x) * 0.35);
        maxY = Math.max(maxY, Math.max(src.y, tgt.y) + src.radius + sag);
      }
    }
  }

  const contentW = maxX - minX;
  const contentH = maxY - minY;
  const contentAspect = contentW / contentH;
  const margin = 40;

  let vw: number, vh: number;
  if (contentAspect > containerAspect) {
    vw = contentW + margin * 2;
    vh = vw / containerAspect;
  } else {
    vh = contentH + margin * 2;
    vw = vh * containerAspect;
  }

  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  return { x: cx - vw / 2, y: cy - vh / 2, w: vw, h: vh };
}

/**
 * Full DAG visualization of the Monarch hierarchy.
 *
 * Supports collapse/expand: initially only roots + 1 level of children
 * are shown. Click a node with children to expand/collapse.
 */
export function DagView() {
  const [hideSystem, setHideSystem] = useState(true);
  const dagPath = hideSystem ? "/dag?hide_system=true" : "/dag?hide_system=false";
  const { data: dagData, loading } = useApi<ApiDagData>(dagPath);

  const [direction, setDirection] = useState<DagDirection>("TB");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selectedNode, setSelectedNode] = useState<DagNode | null>(null);
  const [tooltip, setTooltip] = useState<Tooltip | null>(null);
  const [viewBox, setViewBox] = useState({ x: 0, y: 0, w: 1300, h: 800 });
  const svgRef = useRef<SVGSVGElement>(null);
  const isPanning = useRef(false);
  const panStart = useRef({ x: 0, y: 0, vx: 0, vy: 0 });
  const needsFit = useRef(false);

  // Build the full tree structure from API data.
  const tree = useMemo(() => {
    if (!dagData) return null;
    return buildTree(dagData);
  }, [dagData]);

  // Initialize expanded set: roots are expanded so we see 1 level of children.
  const expandedInitialized = useRef(false);
  useEffect(() => {
    if (tree && !expandedInitialized.current) {
      expandedInitialized.current = true;
      setExpanded(new Set(tree.roots));
    }
  }, [tree]);

  // Fit whenever the expanded set changes. This runs before the graph fit
  // effect, so needsFit will be true when the NEW graph (with the updated
  // expanded nodes) triggers the fit effect on the next render.
  const prevExpanded = useRef(expanded);
  useEffect(() => {
    if (expanded !== prevExpanded.current) {
      prevExpanded.current = expanded;
      needsFit.current = true;
    }
  }, [expanded]);

  // Hidden children count for each node (for the badge).
  const hiddenChildCounts = useMemo(() => {
    if (!dagData || !tree) return new Map<string, number>();
    const counts = new Map<string, number>();
    for (const n of dagData.nodes) {
      const kids = tree.children[n.id] ?? [];
      if (kids.length > 0 && !expanded.has(n.id)) {
        counts.set(n.id, countDescendants(n.id, tree.children));
      }
    }
    return counts;
  }, [dagData, tree, expanded]);

  // Filter the API data to only visible nodes, then compute layout.
  const graph: DagGraph | null = useMemo(() => {
    if (!dagData || !tree) return null;
    const visible = visibleNodes(tree.roots, tree.children, expanded);
    const filteredNodes = dagData.nodes.filter((n) => visible.has(n.id));
    const filteredEdges = dagData.edges.filter(
      (e) => visible.has(e.source_id) && visible.has(e.target_id)
    );
    return computeLayout({ nodes: filteredNodes, edges: filteredEdges }, direction);
  }, [dagData, tree, expanded, direction]);

  // Auto-fit after user actions (expand/collapse, direction toggle).
  // Retries until the container has real dimensions (handles initial mount race).
  useEffect(() => {
    if (graph && graph.nodes.length > 0 && needsFit.current) {
      needsFit.current = false;
      const nodes = graph.nodes;
      let attempts = 0;
      const tryFit = () => {
        const container = svgRef.current?.parentElement;
        if (!container) return;
        const rect = container.getBoundingClientRect();
        if ((rect.width === 0 || rect.height === 0) && attempts < 10) {
          // Container not laid out yet — retry.
          attempts++;
          setTimeout(tryFit, 50);
          return;
        }
        const fit = computeFitViewBox(nodes, rect, graph);
        if (fit) setViewBox(fit);
      };
      // Kick off after a frame to let React commit.
      requestAnimationFrame(tryFit);
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

  const fitViewToGraph = useCallback(() => {
    if (!graph || graph.nodes.length === 0) return;
    const container = svgRef.current?.parentElement;
    if (!container) return;
    const fit = computeFitViewBox(graph.nodes, container.getBoundingClientRect(), graph);
    if (fit) setViewBox(fit);
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

  const handleNodeClick = useCallback((node: DagNode) => {
    if (!tree) return;
    const kids = tree.children[node.id] ?? [];
    if (kids.length > 0) {
      setExpanded((prev) => {
        const next = new Set(prev);
        if (next.has(node.id)) {
          next.delete(node.id);
        } else {
          next.add(node.id);
        }
        return next;
      });
    } else {
      setSelectedNode((prev) => (prev?.id === node.id ? null : node));
    }
  }, [tree]);

  const handleNodeHover = useCallback((node: DagNode | null) => {
    if (!node) { setTooltip(null); return; }
    setTooltip({ node, x: node.x, y: node.y - node.radius - 14 });
  }, []);

  const toggleDirection = useCallback(() => {
    needsFit.current = true;
    setDirection((d) => (d === "TB" ? "LR" : "TB"));
  }, []);

  const toggleSystem = useCallback(() => {
    setHideSystem((h) => !h);
    // Reset expand state since the node set changes.
    expandedInitialized.current = false;
    setExpanded(new Set());
  }, []);

  const expandAll = useCallback(() => {
    if (!dagData || !tree) return;
    const all = new Set<string>();
    for (const n of dagData.nodes) {
      if ((tree.children[n.id] ?? []).length > 0) {
        all.add(n.id);
      }
    }
    setExpanded(all);
  }, [dagData, tree]);

  const collapseAll = useCallback(() => {
    if (!tree) return;
    setExpanded(new Set(tree.roots));
  }, [tree]);

  if (loading) return <div className="loading-state">Loading DAG data...</div>;
  if (!graph || graph.nodes.length === 0) return <div className="empty-state">No data available</div>;

  const isLR = direction === "LR";
  const hierEdges = graph.edges.filter((e) => e.type === "hierarchy");
  const msgEdges = graph.edges.filter((e) => e.type === "message");
  const tierEntries = Object.entries(TIER_LABELS) as Array<[DagTier, string]>;

  return (
    <div className="dag-container" data-testid="dag-container">
      <div className="dag-canvas-wrapper">
        <div className="dag-toolbar">
          <button
            className="dag-direction-toggle"
            onClick={toggleDirection}
            title={isLR ? "Switch to top-down layout" : "Switch to left-right layout"}
          >
            {isLR ? "↓ Top-Down" : "→ Left-Right"}
          </button>
          <button
            className="dag-direction-toggle"
            onClick={fitViewToGraph}
            title="Fit graph in view"
          >
            ⊡ Fit
          </button>
          <button
            className="dag-direction-toggle"
            onClick={expandAll}
            title="Expand all nodes"
          >
            + Expand All
          </button>
          <button
            className="dag-direction-toggle"
            onClick={collapseAll}
            title="Collapse to 1 level"
          >
            − Collapse
          </button>
          <button
            className={`dag-direction-toggle ${hideSystem ? "active" : ""}`}
            onClick={toggleSystem}
            title={hideSystem ? "Show system actors" : "Hide system actors"}
          >
            {hideSystem ? "◉ System Hidden" : "○ Show All"}
          </button>
        </div>
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
{/* Tier labels removed — node subtitles already show the tier */}
          <g className="dag-edges-hierarchy">{hierEdges.map((e) => <DagEdgeComponent key={e.id} edge={e} nodes={nodeMap} direction={direction} />)}</g>
          <g className="dag-edges-messages">{msgEdges.map((e) => <DagEdgeComponent key={e.id} edge={e} nodes={nodeMap} direction={direction} />)}</g>
          <g className="dag-nodes">
            {graph.nodes.map((node) => {
              const hiddenCount = hiddenChildCounts.get(node.id);
              return (
                <DagNodeComponent
                  key={node.id}
                  node={node}
                  selected={selectedNode?.id === node.id}
                  onSelect={handleNodeClick}
                  onHover={handleNodeHover}
                  hiddenChildCount={hiddenCount}
                />
              );
            })}
          </g>
        </svg>
        {tooltip && !selectedNode && (() => {
          const n = tooltip.node;
          const idParts = String(n.entityId).split(",");
          return (
            <div className="dag-tooltip" style={{ left: `${((tooltip.x - viewBox.x) / viewBox.w) * 100}%`, top: `${((tooltip.y - viewBox.y) / viewBox.h) * 100}%` }}>
              <div className="dag-tooltip-name">
                {idParts.map((part, i) => (
                  <div key={i}>{part}{i < idParts.length - 1 ? "," : ""}</div>
                ))}
              </div>
              <div className="dag-tooltip-info">
                {n.status}
                {hiddenChildCounts.has(n.id) && (
                  <> &middot; click to expand ({hiddenChildCounts.get(n.id)} hidden)</>
                )}
              </div>
            </div>
          );
        })()}
      </div>
      <DagLegend />
      {selectedNode && <NodeDetail node={selectedNode} onClose={() => setSelectedNode(null)} />}
    </div>
  );
}
