/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Pure layout engine for the DAG visualization.
 *
 * Accepts pre-classified nodes and edges from the /api/dag endpoint
 * and computes X/Y positions using a deterministic hierarchical layout
 * with 6 tiers:
 *
 *   Host Mesh -> Host Unit -> Proc Mesh -> Proc Unit -> Actor Mesh -> Actor
 *
 * Supports two directions:
 *   - "TB" (top-to-bottom): tiers flow downward, leaves spread horizontally
 *   - "LR" (left-to-right): tiers flow rightward, leaves spread vertically
 *
 * All classification, status propagation, and edge construction is
 * done server-side. This module only adds positions and radii.
 */

/** Layout direction. */
export type DagDirection = "TB" | "LR";

/** Tiers in the Monarch hierarchy.
 *  6-tier (telemetry): host_mesh, host_unit, proc_mesh, proc_unit, actor_mesh, actor
 *  3-tier (admin API): host, proc, actor
 */
export type DagTier = "host_mesh" | "host_unit" | "proc_mesh" | "proc_unit" | "actor_mesh" | "actor" | "host" | "proc";

/** A node from the /api/dag response (before positioning). */
export interface ApiDagNode {
  id: string;
  entity_id: number | string;
  tier: DagTier;
  label: string;
  subtitle: string;
  status: string;
  rank?: number;
  /** Telemetry actor ID for querying messages/status (admin DAG only). */
  telemetry_actor_id?: number | string;
  /** Mesh name this node belongs to (proc/actor tiers). */
  mesh_name?: string;
}

/** An edge from the /api/dag response. */
export interface ApiDagEdge {
  id: string;
  source_id: string;
  target_id: string;
  type: "hierarchy" | "message";
}

/** Full /api/dag response shape. */
export interface ApiDagData {
  nodes: ApiDagNode[];
  edges: ApiDagEdge[];
}

/** A positioned node in the DAG (after layout). */
export interface DagNode {
  id: string;
  label: string;
  subtitle: string;
  x: number;
  y: number;
  radius: number;
  tier: DagTier;
  status: string;
  entityId: number | string;
  /** Telemetry actor ID for querying messages/status (admin DAG only). */
  telemetryActorId?: number | string;
  /** Mesh name this node belongs to. */
  meshName?: string;
}

/** An edge connecting two nodes (camelCase for frontend use). */
export interface DagEdge {
  id: string;
  sourceId: string;
  targetId: string;
  type: "hierarchy" | "message";
}

/** Full graph layout result. */
export interface DagGraph {
  nodes: DagNode[];
  edges: DagEdge[];
  width: number;
  height: number;
  direction: DagDirection;
}

/** Fixed position along the primary axis for each tier. */
const TIER_POSITION: Record<DagTier, number> = {
  // 6-tier (telemetry SQL)
  host_mesh: 60,
  host_unit: 180,
  proc_mesh: 300,
  proc_unit: 420,
  actor_mesh: 540,
  actor: 660,
  // 3-tier (admin API) — spaced for 3 levels, not 6
  host: 80,
  proc: 260,
};

const NODE_RADIUS: Record<DagTier, number> = {
  host_mesh: 44,
  host_unit: 36,
  proc_mesh: 36,
  proc_unit: 28,
  actor_mesh: 28,
  actor: 28,
  host: 40,
  proc: 32,
};

/** Parent tiers that show tier name instead of label. */
const PARENT_TIERS = new Set<string>(["host_mesh", "proc_mesh", "actor_mesh", "host", "proc"]);

/** Gap between adjacent nodes (in addition to their half-widths). */
const NODE_GAP = 16;

/** Compute the visual width of a node (matches DagNode.tsx rendering). */
function nodeWidth(node: ApiDagNode): number {
  const r = NODE_RADIUS[node.tier] ?? 18;
  const isParent = PARENT_TIERS.has(node.tier);
  if (isParent) {
    return r * 2.2;
  }
  // Actor nodes: sized to fit label text.
  return Math.max(r * 2.2, node.label.length * 6 + 24);
}

const TIER_LABELS: Record<DagTier, string> = {
  host_mesh: "HOST MESH",
  host_unit: "HOST",
  proc_mesh: "PROC MESH",
  proc_unit: "PROC",
  actor_mesh: "ACTOR MESH",
  actor: "ACTOR",
  host: "HOST",
  proc: "PROC",
};

const PADDING = 80;

// Keep backward-compatible exports (TIER_Y is used by DagView for tier labels).
const TIER_Y = TIER_POSITION;
export { TIER_Y, TIER_LABELS, TIER_POSITION };

/**
 * Compute X/Y positions for pre-classified DAG nodes.
 *
 * In TB mode: tiers get fixed Y, leaves spread along X.
 * In LR mode: tiers get fixed X, leaves spread along Y.
 *
 * Each tier uses its own spacing so that small leaf nodes (actors)
 * pack tightly while larger parent nodes (host meshes) stay roomy.
 */
export function computeLayout(data: ApiDagData, direction: DagDirection = "TB"): DagGraph {
  if (data.nodes.length === 0) {
    return { nodes: [], edges: [], width: 0, height: 0, direction };
  }

  const isLR = direction === "LR";

  // Determine unique tiers present and assign evenly-spaced positions
  // if the data uses fewer than the full 6-tier layout.
  const uniqueTiers = [...new Set(data.nodes.map((n) => n.tier))];
  const tierOrder: DagTier[] = ["host_mesh", "host_unit", "host", "proc_mesh", "proc_unit", "proc", "actor_mesh", "actor"];
  uniqueTiers.sort((a, b) => tierOrder.indexOf(a) - tierOrder.indexOf(b));

  const dynamicTierPos: Record<string, number> = {};
  const tierSpacing = uniqueTiers.length > 1 ? 500 / (uniqueTiers.length - 1) : 0;
  uniqueTiers.forEach((tier, i) => {
    dynamicTierPos[tier] = 80 + i * tierSpacing;
  });

  // Index nodes by id.
  const nodeMap: Record<string, ApiDagNode> = {};
  for (const n of data.nodes) nodeMap[n.id] = n;

  // Build parent -> children map from hierarchy edges.
  const children: Record<string, string[]> = {};
  const hasParent = new Set<string>();
  for (const e of data.edges) {
    if (e.type !== "hierarchy") continue;
    (children[e.source_id] ??= []).push(e.target_id);
    hasParent.add(e.target_id);
  }

  // Roots = nodes with no incoming hierarchy edge, sorted by mesh name
  // so nodes in the same mesh are placed adjacent in the layout.
  const roots = data.nodes
    .filter((n) => !hasParent.has(n.id))
    .sort((a, b) => {
      const ma = a.mesh_name ?? "\uffff";
      const mb = b.mesh_name ?? "\uffff";
      if (ma !== mb) return ma < mb ? -1 : 1;
      return (a.rank ?? 0) - (b.rank ?? 0);
    })
    .map((n) => n.id);

  // --- Two-pass layout ---
  // Pass 1: Position leaves using tier-specific spacing, then
  //         center each parent over its children.
  // Pass 2: (implicit) Parents are already centered from the recursion.

  let nextLeafPos = PADDING;
  // Track the spread positions assigned to each node (along the secondary axis).
  const spreadPos: Record<string, number> = {};

  function positionSubtree(id: string): { min: number; max: number } | null {
    const node = nodeMap[id];
    if (!node) return null;

    const tierPos = dynamicTierPos[node.tier] ?? TIER_POSITION[node.tier];
    const kids = (children[id] ?? []).slice().sort((a, b) => {
      // Sort by mesh name so siblings in the same mesh are adjacent.
      const ma = nodeMap[a]?.mesh_name ?? "\uffff";
      const mb = nodeMap[b]?.mesh_name ?? "\uffff";
      if (ma !== mb) return ma < mb ? -1 : 1;
      const ra = nodeMap[a]?.rank ?? 0;
      const rb = nodeMap[b]?.rank ?? 0;
      return ra - rb;
    });
    if (kids.length === 0) {
      // Leaf: position based on actual node width so nodes don't overlap.
      const halfW = nodeWidth(node) / 2;
      const pos = nextLeafPos + halfW;
      nextLeafPos = pos + halfW + NODE_GAP;
      spreadPos[id] = pos;
      return { min: pos, max: pos };
    }

    // Recurse into children.
    let groupMin = Infinity;
    let groupMax = -Infinity;
    for (const kid of kids) {
      const range = positionSubtree(kid);
      if (range) {
        groupMin = Math.min(groupMin, range.min);
        groupMax = Math.max(groupMax, range.max);
      }
    }

    // Center this parent over its children's span.
    const center = (groupMin + groupMax) / 2;
    spreadPos[id] = center;
    return { min: groupMin, max: groupMax };
  }

  for (const root of roots) {
    positionSubtree(root);
    // Small gap between independent subtrees.
    nextLeafPos += NODE_GAP * 2;
  }

  // --- Overlap resolution pass ---
  // For each tier, sort nodes by spread position and push any that
  // overlap with their predecessor to the right.  When a parent is
  // pushed, shift its entire subtree by the same delta.
  const tierNodes: Record<string, string[]> = {};
  for (const [id] of Object.entries(spreadPos)) {
    const node = nodeMap[id];
    if (!node) continue;
    (tierNodes[node.tier] ??= []).push(id);
  }

  function shiftSubtree(id: string, delta: number) {
    spreadPos[id] += delta;
    for (const kid of children[id] ?? []) {
      if (spreadPos[kid] !== undefined) {
        shiftSubtree(kid, delta);
      }
    }
  }

  for (const ids of Object.values(tierNodes)) {
    ids.sort((a, b) => spreadPos[a] - spreadPos[b]);
    for (let i = 1; i < ids.length; i++) {
      const prevNode = nodeMap[ids[i - 1]];
      const currNode = nodeMap[ids[i]];
      if (!prevNode || !currNode) continue;
      const prevRight = spreadPos[ids[i - 1]] + nodeWidth(prevNode) / 2;
      const currLeft = spreadPos[ids[i]] - nodeWidth(currNode) / 2;
      const overlap = prevRight + NODE_GAP - currLeft;
      if (overlap > 0) {
        shiftSubtree(ids[i], overlap);
      }
    }
  }

  // Re-center parents over their (possibly shifted) children.
  function recenter(id: string): void {
    const kids = children[id] ?? [];
    if (kids.length === 0) return;
    for (const kid of kids) recenter(kid);
    const childPositions = kids
      .filter((k) => spreadPos[k] !== undefined)
      .map((k) => spreadPos[k]);
    if (childPositions.length > 0) {
      spreadPos[id] = (Math.min(...childPositions) + Math.max(...childPositions)) / 2;
    }
  }
  for (const root of roots) recenter(root);

  // Convert spread positions to x/y based on direction.
  const positions: Record<string, { x: number; y: number }> = {};
  for (const [id, spread] of Object.entries(spreadPos)) {
    const node = nodeMap[id];
    if (!node) continue;
    const tierPos = dynamicTierPos[node.tier] ?? TIER_POSITION[node.tier];
    if (isLR) {
      positions[id] = { x: tierPos, y: spread };
    } else {
      positions[id] = { x: spread, y: tierPos };
    }
  }

  // Build positioned nodes.
  const nodes: DagNode[] = data.nodes
    .filter((n) => positions[n.id])
    .map((n) => ({
      id: n.id,
      label: n.label,
      subtitle: n.subtitle,
      x: positions[n.id].x,
      y: positions[n.id].y,
      radius: NODE_RADIUS[n.tier],
      tier: n.tier,
      status: n.status,
      entityId: n.entity_id,
      telemetryActorId: n.telemetry_actor_id,
      meshName: n.mesh_name,
    }));

  // Map edges from snake_case to camelCase.
  const edges: DagEdge[] = data.edges.map((e) => ({
    id: e.id,
    sourceId: e.source_id,
    targetId: e.target_id,
    type: e.type,
  }));

  // Compute the extent along the primary (tier) axis from actual positions.
  const maxTierPos = data.nodes.reduce((max, n) => Math.max(max, dynamicTierPos[n.tier] ?? TIER_POSITION[n.tier] ?? 0), 0);
  const lastTier = maxTierPos + 120;
  const spreadExtent = nextLeafPos + PADDING;

  return {
    nodes,
    edges,
    width: isLR ? lastTier : spreadExtent,
    height: isLR ? spreadExtent : lastTier,
    direction,
  };
}
