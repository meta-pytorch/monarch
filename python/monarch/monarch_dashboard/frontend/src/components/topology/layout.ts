/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import ELK from "elkjs/lib/elk.bundled.js";
import type { Edge, Node } from "@xyflow/react";
import type { ApiDagData, ApiDagEdge, ApiDagNode, DagTier } from "../../types";

const elk = new ELK();

export type Direction = "TB" | "LR";

/** Uniform-ish node box sizes per tier (px). */
// Tiers that carry a mesh tag are taller to fit the footer.
export const NODE_SIZE: Record<string, { w: number; h: number }> = {
  host_mesh: { w: 196, h: 58 },
  proc_mesh: { w: 196, h: 58 },
  actor_mesh: { w: 196, h: 58 },
  host_unit: { w: 184, h: 72 },
  proc_unit: { w: 184, h: 72 },
  actor: { w: 190, h: 72 },
  host: { w: 190, h: 72 },
  proc: { w: 184, h: 72 },
};

function sizeOf(tier: DagTier) {
  return NODE_SIZE[tier] ?? { w: 168, h: 52 };
}

/** parent→children map + roots, from hierarchy edges only. */
export function buildTree(data: ApiDagData) {
  const children: Record<string, string[]> = {};
  const hasParent = new Set<string>();
  for (const e of data.edges) {
    if (e.type !== "hierarchy") continue;
    (children[e.source_id] ??= []).push(e.target_id);
    hasParent.add(e.target_id);
  }
  const roots = data.nodes.filter((n) => !hasParent.has(n.id)).map((n) => n.id);
  return { children, roots };
}

/** BFS the visible set given the expanded set (roots always visible). */
export function visibleSet(
  roots: string[],
  children: Record<string, string[]>,
  expanded: Set<string>
): Set<string> {
  const vis = new Set<string>();
  const q = [...roots];
  while (q.length) {
    const id = q.shift()!;
    vis.add(id);
    if (expanded.has(id)) for (const k of children[id] ?? []) q.push(k);
  }
  return vis;
}

export function countDescendants(id: string, children: Record<string, string[]>): number {
  const kids = children[id] ?? [];
  let n = kids.length;
  for (const k of kids) n += countDescendants(k, children);
  return n;
}

/**
 * Run ELK "layered" layout over the visible sub-graph (hierarchy edges drive
 * layout; message edges are overlaid afterward). Returns positioned RF nodes
 * plus the RF edges.
 */
export async function layoutGraph(
  apiNodes: ApiDagNode[],
  apiEdges: ApiDagEdge[],
  direction: Direction,
  makeData: (n: ApiDagNode) => Record<string, unknown>
): Promise<{ nodes: Node[]; edges: Edge[] }> {
  const hierEdges = apiEdges.filter((e) => e.type === "hierarchy");

  const graph = {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": direction === "TB" ? "DOWN" : "RIGHT",
      // Keep all roots on the same top layer instead of stacking each
      // disconnected mesh subtree, which caused vertical overlap.
      "elk.separateConnectedComponents": "false",
      "elk.layered.spacing.nodeNodeBetweenLayers": "68",
      "elk.spacing.nodeNode": "28",
      "elk.spacing.componentComponent": "48",
      "elk.layered.nodePlacement.strategy": "BRANDES_KOEPF",
      "elk.layered.considerModelOrder.strategy": "NODES_AND_EDGES",
    },
    children: apiNodes.map((n) => {
      const sz = sizeOf(n.tier);
      return { id: n.id, width: sz.w, height: sz.h };
    }),
    edges: hierEdges.map((e) => ({
      id: e.id,
      sources: [e.source_id],
      targets: [e.target_id],
    })),
  };

  const res = await elk.layout(graph as any);
  const pos: Record<string, { x: number; y: number }> = {};
  for (const c of res.children ?? []) pos[c.id] = { x: c.x ?? 0, y: c.y ?? 0 };

  const nodes: Node[] = apiNodes
    .filter((n) => pos[n.id])
    .map((n) => {
      const sz = sizeOf(n.tier);
      return {
        id: n.id,
        type: "monarch",
        position: pos[n.id],
        data: makeData(n),
        style: { width: sz.w, height: sz.h },
        width: sz.w,
        height: sz.h,
      };
    });

  const targetPos = direction === "TB" ? "top" : "left";
  const sourcePos = direction === "TB" ? "bottom" : "right";

  // Message edges connect nodes in the same tier; route them as arcs that bow
  // away from the node row (down in TB, right in LR) so they never overlap the
  // boxes or each other. Both ends anchor on the same side for a clean chord.
  const msgSide = direction === "TB" ? "bottom" : "right";

  const edges: Edge[] = apiEdges.map((e) => {
    const isMsg = e.type === "message";
    return {
      id: e.id,
      source: e.source_id,
      target: e.target_id,
      type: isMsg ? "message" : "smoothstep",
      sourceHandle: isMsg ? msgSide : sourcePos,
      targetHandle: isMsg ? `${msgSide}-t` : targetPos,
      zIndex: isMsg ? 0 : 1,
      style: isMsg
        ? { stroke: "var(--accent-2)", strokeWidth: 1.6, opacity: 0.7 }
        : { stroke: "#3a4152", strokeWidth: 1.5 },
      data: { kind: e.type, dir: direction },
    };
  });

  return { nodes, edges };
}
