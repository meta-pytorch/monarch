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
 * hierarchical layout with 6 tiers:
 * Host Mesh -> Host Unit -> Proc Mesh -> Proc Unit -> Actor Mesh -> Actor
 *
 * Host Meshes and Proc Meshes are structural (status "n/a").
 * HostAgent actors render as "host_unit" nodes under their host mesh.
 * ProcAgent actors render as "proc_unit" nodes under their proc mesh.
 * Regular actors render in the "actor" tier under actor meshes.
 */

import { Mesh, Actor } from "../types";

export type DagTier = "host_mesh" | "host_unit" | "proc_mesh" | "proc_unit" | "actor_mesh" | "actor";

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

// Layout constants — 6 tiers spread left to right.
const TIER_X: Record<DagTier, number> = {
  host_mesh: 80,
  host_unit: 230,
  proc_mesh: 380,
  proc_unit: 530,
  actor_mesh: 680,
  actor: 980,
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
  host_mesh: "HOST MESHES",
  host_unit: "HOSTS",
  proc_mesh: "PROC MESHES",
  proc_unit: "PROCS",
  actor_mesh: "ACTOR MESHES",
  actor: "ACTORS",
};

const VERTICAL_SPACING = 90;
const PADDING_Y = 80;

const TERMINAL_STATUSES = new Set(["stopped", "failed", "stopping"]);

/** Extract a short display name. */
function shortName(name: string): string {
  const parts = name.split("/");
  return parts[parts.length - 1];
}

export { TIER_X, TIER_LABELS };

/**
 * Compute a hierarchical DAG layout from meshes and actors.
 *
 * HostAgent actors render as host_unit nodes (visible, with status).
 * ProcAgent actors render as proc_unit nodes (visible, with status).
 * Meshes are structural containers with status "n/a".
 *
 * Downward propagation: terminal host_unit → proc_units and actors
 * under that host inherit the terminal status.
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

  // Separate actors into system agents and regular actors.
  const hostAgentsByMesh: Record<number, Actor[]> = {};
  const procAgentsByMesh: Record<number, Actor[]> = {};
  const regularActors: Actor[] = [];

  for (const a of actors) {
    if (a.full_name.includes("HostAgent")) {
      if (!hostAgentsByMesh[a.mesh_id]) hostAgentsByMesh[a.mesh_id] = [];
      hostAgentsByMesh[a.mesh_id].push(a);
    } else if (a.full_name.includes("ProcAgent")) {
      if (!procAgentsByMesh[a.mesh_id]) procAgentsByMesh[a.mesh_id] = [];
      procAgentsByMesh[a.mesh_id].push(a);
    } else {
      regularActors.push(a);
    }
  }

  // Build parent -> children maps.
  const meshChildren: Record<number, Mesh[]> = {};
  for (const m of meshes) {
    if (m.parent_mesh_id != null) {
      if (!meshChildren[m.parent_mesh_id]) meshChildren[m.parent_mesh_id] = [];
      meshChildren[m.parent_mesh_id].push(m);
    }
  }

  // Build mesh -> regular actors map.
  const meshActors: Record<number, Actor[]> = {};
  for (const a of regularActors) {
    if (!meshActors[a.mesh_id]) meshActors[a.mesh_id] = [];
    meshActors[a.mesh_id].push(a);
  }

  // Compute host unit statuses for downward propagation.
  const hostUnitStatus: Record<number, string> = {};
  for (const hm of hostMeshes) {
    for (const agent of hostAgentsByMesh[hm.id] ?? []) {
      hostUnitStatus[agent.id] = actorStatuses[agent.id] ?? "unknown";
    }
  }

  // Map proc mesh -> parent host mesh for propagation lookup.
  const procToHost: Record<number, number> = {};
  for (const pm of procMeshes) {
    if (pm.parent_mesh_id != null) procToHost[pm.id] = pm.parent_mesh_id;
  }

  // Check if ANY host agent under a host mesh is terminal.
  function isHostTerminal(hostMeshId: number): string | null {
    for (const agent of hostAgentsByMesh[hostMeshId] ?? []) {
      const s = actorStatuses[agent.id] ?? "unknown";
      if (TERMINAL_STATUSES.has(s)) return s;
    }
    return null;
  }

  // Assign Y positions from leaf actors upward.
  let nextY = PADDING_Y;
  const nodePositions: Record<string, { x: number; y: number }> = {};

  for (const hostMesh of hostMeshes) {
    const hostAgents = hostAgentsByMesh[hostMesh.id] ?? [];
    const pms = meshChildren[hostMesh.id] ?? [];
    const hostChildYs: number[] = [];

    // Position host unit nodes (HostAgents).
    for (const agent of hostAgents) {
      const y = nextY;
      nextY += VERTICAL_SPACING;
      nodePositions[`host_unit-${agent.id}`] = { x: TIER_X.host_unit, y };
      hostChildYs.push(y);
    }

    for (const pm of pms) {
      const procAgents = procAgentsByMesh[pm.id] ?? [];
      const ams = meshChildren[pm.id] ?? [];
      const pmChildYs: number[] = [];

      // Position proc unit nodes (ProcAgents).
      for (const agent of procAgents) {
        const y = nextY;
        nextY += VERTICAL_SPACING;
        nodePositions[`proc_unit-${agent.id}`] = { x: TIER_X.proc_unit, y };
        pmChildYs.push(y);
      }

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
        nodePositions[`actor_mesh-${am.id}`] = { x: TIER_X.actor_mesh, y: amY };
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
    nodePositions[`host_mesh-${hostMesh.id}`] = { x: TIER_X.host_mesh, y: hostY };

    nextY += VERTICAL_SPACING * 0.5;
  }

  // -- Create DagNode objects --

  // Host meshes — structural, always "n/a".
  for (const m of hostMeshes) {
    const pos = nodePositions[`host_mesh-${m.id}`];
    if (!pos) continue;
    nodes.push({
      id: `host_mesh-${m.id}`,
      label: shortName(m.given_name),
      subtitle: "Host Mesh",
      x: pos.x, y: pos.y,
      radius: NODE_RADIUS.host_mesh,
      tier: "host_mesh",
      status: "n/a",
      entityId: m.id,
    });
  }

  // Host units (HostAgent actors) — visible, with their own status.
  for (const hm of hostMeshes) {
    for (const agent of hostAgentsByMesh[hm.id] ?? []) {
      const pos = nodePositions[`host_unit-${agent.id}`];
      if (!pos) continue;
      nodes.push({
        id: `host_unit-${agent.id}`,
        label: shortName(hm.given_name).replace("_mesh", ""),
        subtitle: "Host",
        x: pos.x, y: pos.y,
        radius: NODE_RADIUS.host_unit,
        tier: "host_unit",
        status: actorStatuses[agent.id] ?? "unknown",
        entityId: agent.id,
      });
    }
  }

  // Proc meshes — structural, always "n/a".
  for (const m of procMeshes) {
    const pos = nodePositions[`proc_mesh-${m.id}`];
    if (!pos) continue;
    nodes.push({
      id: `proc_mesh-${m.id}`,
      label: shortName(m.given_name),
      subtitle: "Proc Mesh",
      x: pos.x, y: pos.y,
      radius: NODE_RADIUS.proc_mesh,
      tier: "proc_mesh",
      status: "n/a",
      entityId: m.id,
    });
  }

  // Proc units (ProcAgent actors) — visible, with status (or inherited from host).
  for (const pm of procMeshes) {
    const hostId = procToHost[pm.id];
    const terminalHost = hostId != null ? isHostTerminal(hostId) : null;

    for (const agent of procAgentsByMesh[pm.id] ?? []) {
      const pos = nodePositions[`proc_unit-${agent.id}`];
      if (!pos) continue;
      const ownStatus = actorStatuses[agent.id] ?? "unknown";
      nodes.push({
        id: `proc_unit-${agent.id}`,
        label: shortName(pm.given_name).replace("_mesh", ""),
        subtitle: "Proc",
        x: pos.x, y: pos.y,
        radius: NODE_RADIUS.proc_unit,
        tier: "proc_unit",
        status: terminalHost ?? ownStatus,
        entityId: agent.id,
      });
    }
  }

  // Actor meshes — structural, always "n/a".
  for (const m of actorMeshes) {
    const pos = nodePositions[`actor_mesh-${m.id}`];
    if (!pos) continue;
    nodes.push({
      id: `actor_mesh-${m.id}`,
      label: shortName(m.given_name),
      subtitle: "Actor Mesh",
      x: pos.x, y: pos.y,
      radius: NODE_RADIUS.actor_mesh,
      tier: "actor_mesh",
      status: "n/a",
      entityId: m.id,
    });
  }

  // Regular actors — with effective status (own or inherited from terminal host).
  for (const a of regularActors) {
    const pos = nodePositions[`actor-${a.id}`];
    if (!pos) continue;
    const mesh = meshes.find((m) => m.id === a.mesh_id);
    const parentProcId = mesh?.parent_mesh_id;
    const hostId = parentProcId != null ? procToHost[parentProcId] : null;
    const terminalHost = hostId != null ? isHostTerminal(hostId) : null;
    nodes.push({
      id: `actor-${a.id}`,
      label: shortName(a.full_name),
      subtitle: `rank ${a.rank}`,
      x: pos.x, y: pos.y,
      radius: NODE_RADIUS.actor,
      tier: "actor",
      status: terminalHost ?? (actorStatuses[a.id] ?? "unknown"),
      entityId: a.id,
    });
  }

  // -- Edges (sequential: mesh → unit → mesh → unit → mesh → actor) --

  // Host mesh -> host unit edges.
  for (const hm of hostMeshes) {
    for (const agent of hostAgentsByMesh[hm.id] ?? []) {
      edges.push({
        id: `hier-host_mesh-${hm.id}-host_unit-${agent.id}`,
        sourceId: `host_mesh-${hm.id}`,
        targetId: `host_unit-${agent.id}`,
        type: "hierarchy",
      });
    }
  }

  // Host unit -> proc mesh edges (units connect to child meshes).
  for (const pm of procMeshes) {
    if (pm.parent_mesh_id == null) continue;
    // Find the host unit(s) for this proc mesh's parent host mesh.
    for (const agent of hostAgentsByMesh[pm.parent_mesh_id] ?? []) {
      edges.push({
        id: `hier-host_unit-${agent.id}-proc_mesh-${pm.id}`,
        sourceId: `host_unit-${agent.id}`,
        targetId: `proc_mesh-${pm.id}`,
        type: "hierarchy",
      });
    }
  }

  // Proc mesh -> proc unit edges.
  for (const pm of procMeshes) {
    for (const agent of procAgentsByMesh[pm.id] ?? []) {
      edges.push({
        id: `hier-proc_mesh-${pm.id}-proc_unit-${agent.id}`,
        sourceId: `proc_mesh-${pm.id}`,
        targetId: `proc_unit-${agent.id}`,
        type: "hierarchy",
      });
    }
  }

  // Proc unit -> actor mesh edges (units connect to child meshes).
  for (const am of actorMeshes) {
    if (am.parent_mesh_id == null) continue;
    // Find the proc unit(s) for this actor mesh's parent proc mesh.
    for (const agent of procAgentsByMesh[am.parent_mesh_id] ?? []) {
      edges.push({
        id: `hier-proc_unit-${agent.id}-actor_mesh-${am.id}`,
        sourceId: `proc_unit-${agent.id}`,
        targetId: `actor_mesh-${am.id}`,
        type: "hierarchy",
      });
    }
  }

  // Actor mesh -> actor edges.
  for (const a of regularActors) {
    edges.push({
      id: `hier-actor_mesh-${a.mesh_id}-actor-${a.id}`,
      sourceId: `actor_mesh-${a.mesh_id}`,
      targetId: `actor-${a.id}`,
      type: "hierarchy",
    });
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
