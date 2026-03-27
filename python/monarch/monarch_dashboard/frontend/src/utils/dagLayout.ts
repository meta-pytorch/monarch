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
 * and computes X/Y positions using a deterministic top-to-bottom
 * hierarchical layout with 6 tiers:
 *
 *   Host Mesh -> Host Unit -> Proc Mesh -> Proc Unit -> Actor Mesh -> Actor
 *
 * All classification, status propagation, and edge construction is
 * done server-side. This module only adds positions and radii.
 */

/** The 6 tiers in the Monarch hierarchy. */
export type DagTier = "host_mesh" | "host_unit" | "proc_mesh" | "proc_unit" | "actor_mesh" | "actor";

/** A node from the /api/dag response (before positioning). */
export interface ApiDagNode {
  id: string;
  entity_id: number | string;
  tier: DagTier;
  label: string;
  subtitle: string;
  status: string;
  rank?: number;
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
}

// Layout constants — 6 tiers spread top to bottom.
const TIER_Y: Record<DagTier, number> = {
  host_mesh: 60,
  host_unit: 180,
  proc_mesh: 300,
  proc_unit: 420,
  actor_mesh: 540,
  actor: 660,
};

const NODE_RADIUS: Record<DagTier, number> = {
  host_mesh: 44,
  host_unit: 36,
  proc_mesh: 36,
  proc_unit: 28,
  actor_mesh: 28,
  actor: 18,
};

const TIER_LABELS: Record<DagTier, string> = {
  host_mesh: "HOST MESH",
  host_unit: "HOST",
  proc_mesh: "PROC MESH",
  proc_unit: "PROC",
  actor_mesh: "ACTOR MESH",
  actor: "ACTOR",
};

const HORIZONTAL_SPACING = 100;
const PADDING_X = 160;

export { TIER_Y, TIER_LABELS };

/**
 * Compute X/Y positions for pre-classified DAG nodes.
 *
 * Tiers are arranged top-to-bottom at fixed Y positions.
 * Leaf nodes are spread horizontally, parents centered above children.
 */
export function computeLayout(data: ApiDagData): DagGraph {
  if (data.nodes.length === 0) {
    return { nodes: [], edges: [], width: 0, height: 0 };
  }

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

  // Roots = nodes with no incoming hierarchy edge.
  const roots = data.nodes
    .filter((n) => !hasParent.has(n.id))
    .map((n) => n.id);

  // Recursively position: leaves get horizontal X positions,
  // parents center horizontally above children.
  let nextX = PADDING_X;
  const positions: Record<string, { x: number; y: number }> = {};

  function positionSubtree(id: string): number[] {
    const node = nodeMap[id];
    if (!node) return [];

    const kids = (children[id] ?? []).slice().sort((a, b) => {
      const ra = nodeMap[a]?.rank ?? 0;
      const rb = nodeMap[b]?.rank ?? 0;
      return ra - rb;
    });
    if (kids.length === 0) {
      const x = nextX;
      nextX += HORIZONTAL_SPACING;
      positions[id] = { x, y: TIER_Y[node.tier] };
      return [x];
    }

    const childXs: number[] = [];
    for (const kid of kids) {
      childXs.push(...positionSubtree(kid));
    }

    const centerX =
      childXs.length > 0
        ? (childXs[0] + childXs[childXs.length - 1]) / 2
        : nextX;
    positions[id] = { x: centerX, y: TIER_Y[node.tier] };
    return childXs;
  }

  for (const root of roots) {
    positionSubtree(root);
    nextX += HORIZONTAL_SPACING * 0.5;
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
    }));

  // Map edges from snake_case to camelCase.
  const edges: DagEdge[] = data.edges.map((e) => ({
    id: e.id,
    sourceId: e.source_id,
    targetId: e.target_id,
    type: e.type,
  }));

  return {
    nodes,
    edges,
    width: nextX + PADDING_X,
    height: TIER_Y.actor + 120,
  };
}
