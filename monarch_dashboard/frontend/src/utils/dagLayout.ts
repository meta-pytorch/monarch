/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Converts API mesh/actor data into positioned graph nodes and edges
 * for the DAG visualization. Uses a deterministic left-to-right
 * hierarchical layout: Host Meshes -> Proc Meshes -> Actor Meshes -> Actors.
 */

import { Mesh, Actor } from "../types";

/** A positioned node in the DAG. */
export interface DagNode {
  id: string;
  label: string;
  subtitle: string;
  x: number;
  y: number;
  radius: number;
  tier: "host" | "proc" | "actor_mesh" | "actor";
  status: string;
  entityId: number;
  meshClass?: string;
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

// Layout constants.
const TIER_X: Record<string, number> = {
  host: 120,
  proc: 380,
  actor_mesh: 640,
  actor: 900,
};

const NODE_RADIUS: Record<string, number> = {
  host: 40,
  proc: 32,
  actor_mesh: 26,
  actor: 20,
};

const VERTICAL_SPACING = 100;
const PADDING_Y = 80;

/** Determine the tier of a mesh by its class. */
function meshTier(meshClass: string): "host" | "proc" | "actor_mesh" {
  if (meshClass === "Host") return "host";
  if (meshClass === "Proc") return "proc";
  return "actor_mesh";
}

/** Extract a short display name from a full_name or given_name. */
function shortName(name: string): string {
  const parts = name.split("/");
  return parts[parts.length - 1];
}

/**
 * Compute a hierarchical DAG layout from meshes and actors.
 *
 * Nodes are placed in four vertical columns (tiers) from left to right.
 * Within each tier, nodes are distributed vertically to align under
 * their parents.
 */
export function computeLayout(
  meshes: Mesh[],
  actors: Actor[],
  actorStatuses: Record<number, string>,
  messagePairs: Array<[number, number]>
): DagGraph {
  const nodes: DagNode[] = [];
  const edges: DagEdge[] = [];

  // Group meshes by tier.
  const hostMeshes = meshes.filter((m) => m.class === "Host");
  const procMeshes = meshes.filter((m) => m.class === "Proc");
  const actorMeshes = meshes.filter(
    (m) => m.class !== "Host" && m.class !== "Proc"
  );

  // Assign vertical positions per tier.
  // Strategy: lay out leaf-level (actors) first, then position parents
  // at the vertical center of their children.

  // 1. Build parent -> children maps.
  const meshChildren: Record<number, Mesh[]> = {};
  for (const m of meshes) {
    if (m.parent_mesh_id != null) {
      if (!meshChildren[m.parent_mesh_id]) meshChildren[m.parent_mesh_id] = [];
      meshChildren[m.parent_mesh_id].push(m);
    }
  }

  const meshActors: Record<number, Actor[]> = {};
  for (const a of actors) {
    if (!meshActors[a.mesh_id]) meshActors[a.mesh_id] = [];
    meshActors[a.mesh_id].push(a);
  }

  // 2. Assign Y positions bottom-up.
  // Start with actors (rightmost column), then bubble up.
  let nextY = PADDING_Y;
  const nodePositions: Record<string, { x: number; y: number }> = {};

  // Process each host mesh tree top-down to maintain ordering,
  // but assign Y from a running counter.
  for (const host of hostMeshes) {
    const procs = meshChildren[host.id] ?? [];
    const hostChildYs: number[] = [];

    for (const proc of procs) {
      const aMeshes = meshChildren[proc.id] ?? [];
      const procChildYs: number[] = [];

      for (const am of aMeshes) {
        const actsInMesh = meshActors[am.id] ?? [];
        const amChildYs: number[] = [];

        for (const act of actsInMesh) {
          const y = nextY;
          nextY += VERTICAL_SPACING;
          nodePositions[`actor-${act.id}`] = {
            x: TIER_X.actor,
            y,
          };
          amChildYs.push(y);
        }

        // Actor mesh: center over its actors, or get its own slot.
        const amY =
          amChildYs.length > 0
            ? (amChildYs[0] + amChildYs[amChildYs.length - 1]) / 2
            : nextY;
        if (amChildYs.length === 0) nextY += VERTICAL_SPACING;
        nodePositions[`mesh-${am.id}`] = {
          x: TIER_X.actor_mesh,
          y: amY,
        };
        procChildYs.push(amY);
      }

      // Also handle actors directly in the proc mesh (system actors).
      const directActors = meshActors[proc.id] ?? [];
      for (const act of directActors) {
        const y = nextY;
        nextY += VERTICAL_SPACING;
        nodePositions[`actor-${act.id}`] = {
          x: TIER_X.actor,
          y,
        };
        procChildYs.push(y);
      }

      // Proc mesh: center over children.
      const procY =
        procChildYs.length > 0
          ? (procChildYs[0] + procChildYs[procChildYs.length - 1]) / 2
          : nextY;
      if (procChildYs.length === 0) nextY += VERTICAL_SPACING;
      nodePositions[`mesh-${proc.id}`] = {
        x: TIER_X.proc,
        y: procY,
      };
      hostChildYs.push(procY);
    }

    // Host mesh: center over procs.
    const hostY =
      hostChildYs.length > 0
        ? (hostChildYs[0] + hostChildYs[hostChildYs.length - 1]) / 2
        : nextY;
    if (hostChildYs.length === 0) nextY += VERTICAL_SPACING;
    nodePositions[`mesh-${host.id}`] = {
      x: TIER_X.host,
      y: hostY,
    };

    // Add spacing between host mesh groups.
    nextY += VERTICAL_SPACING * 0.5;
  }

  // 3. Create DagNode objects.
  for (const m of meshes) {
    const tier = meshTier(m.class);
    const pos = nodePositions[`mesh-${m.id}`];
    if (!pos) continue;

    // Aggregate status: worst of all actors in this subtree.
    const subtreeActors = findSubtreeActors(m.id, meshChildren, meshActors);
    const statuses = subtreeActors.map(
      (a) => actorStatuses[a.id] ?? "unknown"
    );
    const worst = aggregateWorst(statuses);

    nodes.push({
      id: `mesh-${m.id}`,
      label: shortName(m.given_name),
      subtitle: m.class,
      x: pos.x,
      y: pos.y,
      radius: NODE_RADIUS[tier],
      tier,
      status: worst,
      entityId: m.id,
      meshClass: m.class,
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

  // 4. Create hierarchy edges.
  for (const m of meshes) {
    if (m.parent_mesh_id != null) {
      edges.push({
        id: `hier-${m.parent_mesh_id}-${m.id}`,
        sourceId: `mesh-${m.parent_mesh_id}`,
        targetId: `mesh-${m.id}`,
        type: "hierarchy",
      });
    }
  }

  // Actor mesh -> actor edges.
  for (const a of actors) {
    edges.push({
      id: `hier-mesh-${a.mesh_id}-actor-${a.id}`,
      sourceId: `mesh-${a.mesh_id}`,
      targetId: `actor-${a.id}`,
      type: "hierarchy",
    });
  }

  // 5. Create message flow edges (deduplicated by actor pair).
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

/** Recursively collect all actors under a mesh. */
function findSubtreeActors(
  meshId: number,
  meshChildren: Record<number, Mesh[]>,
  meshActors: Record<number, Actor[]>
): Actor[] {
  const result: Actor[] = [];
  const directActors = meshActors[meshId] ?? [];
  result.push(...directActors);
  const children = meshChildren[meshId] ?? [];
  for (const child of children) {
    result.push(...findSubtreeActors(child.id, meshChildren, meshActors));
  }
  return result;
}

/** Aggregate to worst status. */
function aggregateWorst(statuses: string[]): string {
  if (statuses.length === 0) return "unknown";
  const priority: Record<string, number> = {
    idle: 0,
    processing: 1,
    client: 2,
    unknown: 3,
    created: 4,
    initializing: 4,
    saving: 4,
    loading: 4,
    stopping: 5,
    stopped: 6,
    failed: 7,
  };
  let worst = "unknown";
  let worstPri = -1;
  for (const s of statuses) {
    const p = priority[s.toLowerCase()] ?? 3;
    if (p > worstPri) {
      worstPri = p;
      worst = s.toLowerCase();
    }
  }
  return worst;
}
