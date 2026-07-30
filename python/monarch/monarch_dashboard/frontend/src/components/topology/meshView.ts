/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { ApiDagData, ApiDagEdge, ApiDagNode, DagTier } from "../../types";
import { meshLabel } from "../../lib/mesh";

/**
 * Mesh-view collapse: fold the per-entity DAG (hosts / procs / actors) into one
 * node per mesh, so a job with hundreds of actors renders as a handful of
 * Host / Proc / Actor mesh boxes. A mesh of five generators becomes a single
 * "generator" actor-mesh node annotated with its member count.
 *
 * Meshes are grouped by derived identity because the admin snapshot doesn't
 * carry explicit mesh nodes: hosts/actors group by `mesh_name`; procs group by
 * their given name with the trailing rank stripped (anon-0..4 -> "anon").
 * Hierarchy edges are rolled up to the mesh level and deduped.
 */

// Most-severe-first: a mesh's rolled-up status surfaces the worst member state.
const SEVERITY = ["failed", "stopping", "stopped", "processing", "idle", "unknown", "n/a"];
function moreSevere(a: string, b: string): string {
  const ra = SEVERITY.indexOf(a);
  const rb = SEVERITY.indexOf(b);
  return (ra < 0 ? SEVERITY.length : ra) <= (rb < 0 ? SEVERITY.length : rb) ? a : b;
}

/** Proc mesh label: strip the hash suffix, then the trailing "-<rank>". */
function procGroupLabel(label: string): string {
  const noHash = label
    .replace(/\[[A-Za-z0-9]{6,}\]$/, "")
    .replace(/-[A-Za-z0-9]{6,}$/, "");
  return noHash.replace(/-\d+$/, "").trim() || noHash.trim() || label;
}

interface Group {
  key: string;
  label: string;
  tier: DagTier;
}

function groupOf(n: ApiDagNode): Group | null {
  // The DAG is the admin snapshot (host / proc / actor only); each node maps to
  // its mesh. The telemetry-SQL fallback — which carried explicit *_mesh
  // container nodes — has been removed, so there are no container tiers to
  // special-case here.
  const t = n.tier;
  if (t === "host") {
    const l = meshLabel(n.mesh_name) || String(n.label).split("@")[0].trim() || "host";
    return { key: `H:${l}`, label: l, tier: "host_mesh" };
  }
  if (t === "proc") {
    const l = procGroupLabel(String(n.label));
    return { key: `P:${l}`, label: l, tier: "proc_mesh" };
  }
  if (t === "actor") {
    const mn = n.mesh_name || String(n.label);
    return { key: `A:${mn}`, label: meshLabel(mn) || mn, tier: "actor_mesh" };
  }
  return null;
}

export interface MeshCollapse {
  data: ApiDagData;
  /** original node entity_id (string) -> mesh node id, for message overlay. */
  ent2node: Map<string, string>;
}

export function collapseToMeshes(raw: ApiDagData): MeshCollapse {
  const nodeById = new Map<string, ApiDagNode>();
  for (const n of raw.nodes) nodeById.set(n.id, n);

  // original node id -> group; skip anything that doesn't map to a mesh tier.
  const groupByNode = new Map<string, Group>();
  const meshNodes = new Map<string, ApiDagNode>();
  const ent2node = new Map<string, string>();

  for (const n of raw.nodes) {
    const g = groupOf(n);
    if (!g) continue;
    groupByNode.set(n.id, g);
    ent2node.set(String(n.entity_id), g.key);

    const existing = meshNodes.get(g.key);
    if (!existing) {
      meshNodes.set(g.key, {
        id: g.key,
        entity_id: g.key,
        tier: g.tier,
        label: g.label,
        subtitle: "",
        status: n.status,
        mesh_name: g.label,
        memberCount: 1,
      });
    } else {
      existing.memberCount = (existing.memberCount ?? 1) + 1;
      existing.status = moreSevere(existing.status, n.status);
    }
  }

  // Roll hierarchy edges up to the mesh level, deduped; drop self-loops.
  const edgeSeen = new Set<string>();
  const edges: ApiDagEdge[] = [];
  for (const e of raw.edges) {
    if (e.type !== "hierarchy") continue;
    const sg = groupByNode.get(e.source_id);
    const tg = groupByNode.get(e.target_id);
    if (!sg || !tg || sg.key === tg.key) continue;
    const key = `${sg.key}->${tg.key}`;
    if (edgeSeen.has(key)) continue;
    edgeSeen.add(key);
    edges.push({ id: `h-${key}`, source_id: sg.key, target_id: tg.key, type: "hierarchy" });
  }

  return { data: { nodes: [...meshNodes.values()], edges }, ent2node };
}
