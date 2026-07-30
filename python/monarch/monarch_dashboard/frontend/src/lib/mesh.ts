/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { DagTier } from "../types";

// Categorical palette, deliberately distinct from the status colors so a mesh
// tag is never confused with a health state.
const PALETTE = [
  "#38bdf8", "#a78bfa", "#f472b6", "#34d399", "#fbbf24", "#22d3ee",
  "#818cf8", "#fb923c", "#4ade80", "#e879f9", "#2dd4bf", "#facc15",
];

// Stable first-seen assignment (collision-free until the palette is exhausted),
// so a node's tag and the legend always agree within a session.
const assigned = new Map<string, string>();
export function meshColor(key: string): string {
  let c = assigned.get(key);
  if (!c) {
    c = PALETTE[assigned.size % PALETTE.length];
    assigned.set(key, c);
  }
  return c;
}

/** Strip the "-<hash>" / "[<hash>]" suffix: "trainer-v7YhDpwWWzX" -> "trainer". */
export function meshLabel(name: string | null | undefined): string {
  if (!name) return "";
  return (
    name
      .replace(/-[A-Za-z0-9]{6,}$/, "")
      .replace(/\[[A-Za-z0-9]{6,}\]$/, "") || name
  );
}

/** Proc-mesh name derived from a proc's given name: "worker-0" -> "worker", "anon-2" -> "anon". */
export function procMeshLabel(procLabel: string): string {
  return procLabel.replace(/[-[]\d+\]?$/, "").trim() || procLabel;
}

/**
 * Resolve the mesh a node belongs to. Hosts/actors carry mesh_name directly;
 * procs don't in the admin snapshot, so derive from the proc's given name.
 * Returns null for mesh-tier nodes (they *are* the mesh) and system-less nodes.
 */
export function nodeMesh(
  tier: DagTier,
  label: string,
  meshName: string | null | undefined
): { key: string; label: string; kind: "host" | "proc" | "actor" } | null {
  if (tier === "host" || tier === "host_unit") {
    const l = meshLabel(meshName);
    return l ? { key: `h:${l}`, label: l, kind: "host" } : null;
  }
  if (tier === "actor") {
    const l = meshLabel(meshName);
    return l ? { key: `a:${l}`, label: l, kind: "actor" } : null;
  }
  if (tier === "proc" || tier === "proc_unit") {
    const l = procMeshLabel(label);
    return l ? { key: `p:${l}`, label: l, kind: "proc" } : null;
  }
  return null;
}
