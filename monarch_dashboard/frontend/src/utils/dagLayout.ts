/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Converts API data into positioned graph nodes and edges
 * for the DAG visualization. Uses a deterministic left-to-right
 * hierarchical layout with 4 tiers:
 * Host Mesh -> Proc Mesh -> Actor Mesh -> Actor
 */

import { Mesh, Actor } from "../types";

export type DagTier = "host_mesh" | "proc_mesh" | "actor_mesh" | "actor";

/** A positioned node in the DAG. */
export interface DagNode {
  id: string;
  label: string;
  subtitle: string;
  x: number;
  y: number;
  radius: number;
  tier: DagTier;
  status: string;
  entityId: number;
}

/** An edge connecting two nodes. */
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

// Layout constants — 4 tiers spread left to right.
const TIER_X: Record<DagTier, number> = {
  host_mesh: 80,
  proc_mesh: 380,
  actor_mesh: 680,
  actor: 980,
};

const NODE_RADIUS: Record<DagTier, number> = {
  host_mesh: 44,
  proc_mesh: 36,
  actor_mesh: 28,
  actor: 18,
};

const TIER_LABELS: Record<DagTier, string> = {
  host_mesh: "HOST MESHES",
  proc_mesh: "PROC MESHES",
  actor_mesh: "ACTOR MESHES",
  actor: "ACTORS",
};

const VERTICAL_SPACING = 90;
const PADDING_Y = 80;

/** Extract a short display name. */
function shortName(name: string): string {
  const parts = name.split("/");
  return parts[parts.length - 1];
}

export { TIER_X, TIER_LABELS };

/**
 * Compute a hierarchical DAG layout from meshes and actors.
 */
export function computeLayout(
  meshes: Mesh[],
  actors: Actor[],
  actorStatuses: Record<number, string>,
  messagePairs: Array<[number, number]>
): DagGraph {
  const nodes: DagNode[] = [];
  const edges: DagEdge[] = [];

  // Separate meshes by class.
  const hostMeshes = meshes.filter((m) => m.class === "Host");
  const procMeshes = meshes.filter((m) => m.class === "Proc");
  const actorMeshes = meshes.filter(
    (m) => m.class !== "Host" && m.class !== "Proc"
  );

  // Build parent -> children maps using parent_mesh_id.
  const meshChildren: Record<number, Mesh[]> = {};
  for (const m of meshes) {
    if (m.parent_mesh_id != null) {
      if (!meshChildren[m.parent_mesh_id]) meshChildren[m.parent_mesh_id] = [];
      meshChildren[m.parent_mesh_id].push(m);
    }
  }

  // Build mesh -> actors map.
  const meshActors: Record<number, Actor[]> = {};
  for (const a of actors) {
    if (!meshActors[a.mesh_id]) meshActors[a.mesh_id] = [];
    meshActors[a.mesh_id].push(a);
  }

  // Assign Y positions bottom-up from actors.
  let nextY = PADDING_Y;
  const nodePositions: Record<string, { x: number; y: number }> = {};

  for (const hostMesh of hostMeshes) {
    const pms = meshChildren[hostMesh.id] ?? [];
    const hostChildYs: number[] = [];

    for (const pm of pms) {
      const ams = meshChildren[pm.id] ?? [];
      const pmChildYs: number[] = [];

      for (const am of ams) {
        const acts = meshActors[am.id] ?? [];
        const amChildYs: number[] = [];

        for (const act of acts) {
          const y = nextY;
          nextY += VERTICAL_SPACING;
          nodePositions[`actor-${act.id}`] = { x: TIER_X.actor, y };
          amChildYs.push(y);
        }

        const amY =
          amChildYs.length > 0
            ? (amChildYs[0] + amChildYs[amChildYs.length - 1]) / 2
            : nextY;
        if (amChildYs.length === 0) nextY += VERTICAL_SPACING;
        nodePositions[`actor_mesh-${am.id}`] = {
          x: TIER_X.actor_mesh,
          y: amY,
        };
        pmChildYs.push(amY);
      }

      const pmY =
        pmChildYs.length > 0
          ? (pmChildYs[0] + pmChildYs[pmChildYs.length - 1]) / 2
          : nextY;
      if (pmChildYs.length === 0) nextY += VERTICAL_SPACING;
      nodePositions[`proc_mesh-${pm.id}`] = { x: TIER_X.proc_mesh, y: pmY };
      hostChildYs.push(pmY);
    }

    const hostY =
      hostChildYs.length > 0
        ? (hostChildYs[0] + hostChildYs[hostChildYs.length - 1]) / 2
        : nextY;
    if (hostChildYs.length === 0) nextY += VERTICAL_SPACING;
    nodePositions[`host_mesh-${hostMesh.id}`] = {
      x: TIER_X.host_mesh,
      y: hostY,
    };

    nextY += VERTICAL_SPACING * 0.5;
  }

  // Create DagNode objects.
  for (const m of hostMeshes) {
    const pos = nodePositions[`host_mesh-${m.id}`];
    if (!pos) continue;
    nodes.push({
      id: `host_mesh-${m.id}`,
      label: shortName(m.given_name),
      subtitle: "Host Mesh",
      x: pos.x,
      y: pos.y,
      radius: NODE_RADIUS.host_mesh,
      tier: "host_mesh",
      status: "unknown",
      entityId: m.id,
    });
  }

  for (const m of procMeshes) {
    const pos = nodePositions[`proc_mesh-${m.id}`];
    if (!pos) continue;
    nodes.push({
      id: `proc_mesh-${m.id}`,
      label: shortName(m.given_name),
      subtitle: "Proc Mesh",
      x: pos.x,
      y: pos.y,
      radius: NODE_RADIUS.proc_mesh,
      tier: "proc_mesh",
      status: "unknown",
      entityId: m.id,
    });
  }

  for (const m of actorMeshes) {
    const pos = nodePositions[`actor_mesh-${m.id}`];
    if (!pos) continue;
    nodes.push({
      id: `actor_mesh-${m.id}`,
      label: shortName(m.given_name),
      subtitle: "Actor Mesh",
      x: pos.x,
      y: pos.y,
      radius: NODE_RADIUS.actor_mesh,
      tier: "actor_mesh",
      status: "unknown",
      entityId: m.id,
    });
  }

  for (const a of actors) {
    const pos = nodePositions[`actor-${a.id}`];
    if (!pos) continue;
    nodes.push({
      id: `actor-${a.id}`,
      label: shortName(a.full_name),
      subtitle: `rank ${a.rank}`,
      x: pos.x,
      y: pos.y,
      radius: NODE_RADIUS.actor,
      tier: "actor",
      status: actorStatuses[a.id] ?? "unknown",
      entityId: a.id,
    });
  }

  // Create hierarchy edges: mesh parent -> child.
  for (const m of meshes) {
    if (m.parent_mesh_id != null) {
      const parentTier =
        meshes.find((p) => p.id === m.parent_mesh_id)?.class === "Host"
          ? "host_mesh"
          : meshes.find((p) => p.id === m.parent_mesh_id)?.class === "Proc"
            ? "proc_mesh"
            : "actor_mesh";
      const childTier =
        m.class === "Host"
          ? "host_mesh"
          : m.class === "Proc"
            ? "proc_mesh"
            : "actor_mesh";
      edges.push({
        id: `hier-${parentTier}-${m.parent_mesh_id}-${childTier}-${m.id}`,
        sourceId: `${parentTier}-${m.parent_mesh_id}`,
        targetId: `${childTier}-${m.id}`,
        type: "hierarchy",
      });
    }
  }

  // Create actor -> mesh edges.
  for (const a of actors) {
    const mesh = meshes.find((m) => m.id === a.mesh_id);
    if (mesh) {
      const meshTier =
        mesh.class === "Host"
          ? "host_mesh"
          : mesh.class === "Proc"
            ? "proc_mesh"
            : "actor_mesh";
      edges.push({
        id: `hier-${meshTier}-${a.mesh_id}-actor-${a.id}`,
        sourceId: `${meshTier}-${a.mesh_id}`,
        targetId: `actor-${a.id}`,
        type: "hierarchy",
      });
    }
  }

  // Message flow edges (deduplicated by actor pair).
  const seenPairs = new Set<string>();
  for (const [fromId, toId] of messagePairs) {
    const key = `${fromId}-${toId}`;
    if (seenPairs.has(key)) continue;
    seenPairs.add(key);
    edges.push({
      id: `msg-${fromId}-${toId}`,
      sourceId: `actor-${fromId}`,
      targetId: `actor-${toId}`,
      type: "message",
    });
  }

  const totalHeight = nextY + PADDING_Y;
  const totalWidth = TIER_X.actor + 160;

  return { nodes, edges, width: totalWidth, height: totalHeight };
}
