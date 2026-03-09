/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { computeLayout, ApiDagData, DagGraph } from "../utils/dagLayout";

/** Pre-classified test data matching the /api/dag response shape. */
const dagData: ApiDagData = {
  nodes: [
    { id: "host_mesh-1", entity_id: 1, tier: "host_mesh", label: "host_0", subtitle: "Host Mesh", status: "n/a" },
    { id: "host_mesh-2", entity_id: 2, tier: "host_mesh", label: "host_1", subtitle: "Host Mesh", status: "n/a" },
    { id: "host_unit-1", entity_id: 1, tier: "host_unit", label: "host_0", subtitle: "Host", status: "idle" },
    { id: "host_unit-2", entity_id: 2, tier: "host_unit", label: "host_1", subtitle: "Host", status: "failed" },
    { id: "proc_mesh-3", entity_id: 3, tier: "proc_mesh", label: "proc_0", subtitle: "Proc Mesh", status: "n/a" },
    { id: "proc_mesh-4", entity_id: 4, tier: "proc_mesh", label: "proc_0", subtitle: "Proc Mesh", status: "n/a" },
    { id: "proc_unit-3", entity_id: 3, tier: "proc_unit", label: "proc_0", subtitle: "Proc", status: "idle" },
    { id: "proc_unit-4", entity_id: 4, tier: "proc_unit", label: "proc_0", subtitle: "Proc", status: "failed" },
    { id: "actor_mesh-5", entity_id: 5, tier: "actor_mesh", label: "Python<Trainer>", subtitle: "Actor Mesh", status: "n/a" },
    { id: "actor_mesh-6", entity_id: 6, tier: "actor_mesh", label: "Python<Trainer>", subtitle: "Actor Mesh", status: "n/a" },
    { id: "actor-5", entity_id: 5, tier: "actor", label: "PythonActor<Trainer>[0]", subtitle: "rank 0", status: "idle" },
    { id: "actor-6", entity_id: 6, tier: "actor", label: "PythonActor<Trainer>[1]", subtitle: "rank 1", status: "processing" },
    { id: "actor-7", entity_id: 7, tier: "actor", label: "PythonActor<Trainer>[0]", subtitle: "rank 0", status: "failed" },
    { id: "actor-8", entity_id: 8, tier: "actor", label: "PythonActor<Trainer>[1]", subtitle: "rank 1", status: "failed" },
  ],
  edges: [
    { id: "h1", source_id: "host_mesh-1", target_id: "host_unit-1", type: "hierarchy" },
    { id: "h2", source_id: "host_mesh-2", target_id: "host_unit-2", type: "hierarchy" },
    { id: "h3", source_id: "host_unit-1", target_id: "proc_mesh-3", type: "hierarchy" },
    { id: "h4", source_id: "host_unit-2", target_id: "proc_mesh-4", type: "hierarchy" },
    { id: "h5", source_id: "proc_mesh-3", target_id: "proc_unit-3", type: "hierarchy" },
    { id: "h6", source_id: "proc_mesh-4", target_id: "proc_unit-4", type: "hierarchy" },
    { id: "h7", source_id: "proc_unit-3", target_id: "actor_mesh-5", type: "hierarchy" },
    { id: "h8", source_id: "proc_unit-4", target_id: "actor_mesh-6", type: "hierarchy" },
    { id: "h9", source_id: "actor_mesh-5", target_id: "actor-5", type: "hierarchy" },
    { id: "h10", source_id: "actor_mesh-5", target_id: "actor-6", type: "hierarchy" },
    { id: "h11", source_id: "actor_mesh-6", target_id: "actor-7", type: "hierarchy" },
    { id: "h12", source_id: "actor_mesh-6", target_id: "actor-8", type: "hierarchy" },
    { id: "m1", source_id: "actor-6", target_id: "actor-8", type: "message" },
    { id: "m2", source_id: "actor-8", target_id: "actor-6", type: "message" },
  ],
};

describe("computeLayout", () => {
  let graph: DagGraph;

  beforeAll(() => {
    graph = computeLayout(dagData);
  });

  it("creates nodes for all entities", () => {
    expect(graph.nodes.length).toBe(14);
  });

  it("creates nodes with correct tiers", () => {
    expect(graph.nodes.filter((n) => n.tier === "host_mesh").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "host_unit").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "proc_mesh").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "proc_unit").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "actor_mesh").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "actor").length).toBe(4);
  });

  it("preserves status from server", () => {
    expect(graph.nodes.find((n) => n.id === "host_unit-1")!.status).toBe("idle");
    expect(graph.nodes.find((n) => n.id === "host_unit-2")!.status).toBe("failed");
  });

  it("preserves labels from server", () => {
    const hm = graph.nodes.find((n) => n.id === "host_mesh-1")!;
    expect(hm.label).toBe("host_0");
    expect(hm.subtitle).toBe("Host Mesh");
  });

  it("host mesh nodes have largest radius", () => {
    const hostMesh = graph.nodes.find((n) => n.tier === "host_mesh")!;
    const actor = graph.nodes.find((n) => n.tier === "actor")!;
    expect(hostMesh.radius).toBeGreaterThan(actor.radius);
  });

  it("creates hierarchy edges", () => {
    expect(graph.edges.filter((e) => e.type === "hierarchy").length).toBe(12);
  });

  it("creates message edges", () => {
    expect(graph.edges.filter((e) => e.type === "message").length).toBe(2);
  });

  it("maps edge keys to camelCase", () => {
    const edge = graph.edges[0];
    expect(edge.sourceId).toBeDefined();
    expect(edge.targetId).toBeDefined();
  });

  it("assigns positive coordinates to all nodes", () => {
    for (const n of graph.nodes) {
      expect(n.x).toBeGreaterThan(0);
      expect(n.y).toBeGreaterThan(0);
    }
  });

  it("positions tiers top to bottom", () => {
    const hm = graph.nodes.find((n) => n.tier === "host_mesh")!;
    const hu = graph.nodes.find((n) => n.tier === "host_unit")!;
    const pm = graph.nodes.find((n) => n.tier === "proc_mesh")!;
    const pu = graph.nodes.find((n) => n.tier === "proc_unit")!;
    const am = graph.nodes.find((n) => n.tier === "actor_mesh")!;
    const a = graph.nodes.find((n) => n.tier === "actor")!;
    expect(hm.y).toBeLessThan(hu.y);
    expect(hu.y).toBeLessThan(pm.y);
    expect(pm.y).toBeLessThan(pu.y);
    expect(pu.y).toBeLessThan(am.y);
    expect(am.y).toBeLessThan(a.y);
  });

  it("sets graph dimensions", () => {
    expect(graph.width).toBeGreaterThan(0);
    expect(graph.height).toBeGreaterThan(0);
  });

  it("handles empty input", () => {
    const empty = computeLayout({ nodes: [], edges: [] });
    expect(empty.nodes.length).toBe(0);
    expect(empty.edges.length).toBe(0);
  });
});
