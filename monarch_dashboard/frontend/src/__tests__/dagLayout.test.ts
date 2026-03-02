/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { computeLayout, DagGraph } from "../utils/dagLayout";
import { Mesh, Actor } from "../types";

// Minimal test data matching the fake_data topology.
const meshes: Mesh[] = [
  { id: 1, timestamp_us: 0, class: "Host", given_name: "host_mesh_0", full_name: "//root/host_mesh_0", shape_json: '{"dims":[2]}', parent_mesh_id: null, parent_view_json: null },
  { id: 2, timestamp_us: 0, class: "Host", given_name: "host_mesh_1", full_name: "//root/host_mesh_1", shape_json: '{"dims":[2]}', parent_mesh_id: null, parent_view_json: null },
  { id: 3, timestamp_us: 0, class: "Proc", given_name: "proc_mesh_0_0", full_name: "//root/host_mesh_0/proc_mesh_0_0", shape_json: '{"dims":[2]}', parent_mesh_id: 1, parent_view_json: null },
  { id: 4, timestamp_us: 0, class: "Proc", given_name: "proc_mesh_0_1", full_name: "//root/host_mesh_0/proc_mesh_0_1", shape_json: '{"dims":[2]}', parent_mesh_id: 1, parent_view_json: null },
  { id: 7, timestamp_us: 0, class: "Python<Trainer>", given_name: "actor_mesh_0_0", full_name: "//root/.../actor_mesh_0_0", shape_json: '{"dims":[1]}', parent_mesh_id: 3, parent_view_json: null },
  { id: 8, timestamp_us: 0, class: "Python<Trainer>", given_name: "actor_mesh_0_1", full_name: "//root/.../actor_mesh_0_1", shape_json: '{"dims":[1]}', parent_mesh_id: 4, parent_view_json: null },
];

const actors: Actor[] = [
  { id: 1, timestamp_us: 0, mesh_id: 3, rank: 0, full_name: "ProcMeshAgent[0,0]" },
  { id: 2, timestamp_us: 0, mesh_id: 7, rank: 0, full_name: "PythonActor<Trainer>[0,0]" },
  { id: 3, timestamp_us: 0, mesh_id: 4, rank: 0, full_name: "ProcMeshAgent[0,1]" },
  { id: 4, timestamp_us: 0, mesh_id: 8, rank: 0, full_name: "PythonActor<Trainer>[0,1]" },
];

const statuses: Record<number, string> = {
  1: "idle",
  2: "processing",
  3: "idle",
  4: "failed",
};

const messagePairs: Array<[number, number]> = [
  [2, 4],
  [4, 2],
  [2, 4], // duplicate - should be deduped
];

describe("computeLayout", () => {
  let graph: DagGraph;

  beforeAll(() => {
    graph = computeLayout(meshes, actors, statuses, messagePairs);
  });

  it("creates nodes for all meshes and actors", () => {
    // 6 meshes + 4 actors = 10 nodes
    expect(graph.nodes.length).toBe(10);
  });

  it("creates mesh nodes with correct tiers", () => {
    const hostNodes = graph.nodes.filter((n) => n.tier === "host");
    const procNodes = graph.nodes.filter((n) => n.tier === "proc");
    const amNodes = graph.nodes.filter((n) => n.tier === "actor_mesh");
    expect(hostNodes.length).toBe(2);
    expect(procNodes.length).toBe(2);
    expect(amNodes.length).toBe(2);
  });

  it("creates actor nodes", () => {
    const actorNodes = graph.nodes.filter((n) => n.tier === "actor");
    expect(actorNodes.length).toBe(4);
  });

  it("host nodes have largest radius", () => {
    const host = graph.nodes.find((n) => n.tier === "host")!;
    const actor = graph.nodes.find((n) => n.tier === "actor")!;
    expect(host.radius).toBeGreaterThan(actor.radius);
  });

  it("creates hierarchy edges", () => {
    const hierEdges = graph.edges.filter((e) => e.type === "hierarchy");
    // 4 mesh-to-mesh + 4 mesh-to-actor = 8
    expect(hierEdges.length).toBe(8);
  });

  it("creates deduplicated message edges", () => {
    const msgEdges = graph.edges.filter((e) => e.type === "message");
    // [2,4] and [4,2] = 2 unique directional pairs
    expect(msgEdges.length).toBe(2);
  });

  it("assigns positive coordinates to all nodes", () => {
    for (const n of graph.nodes) {
      expect(n.x).toBeGreaterThan(0);
      expect(n.y).toBeGreaterThan(0);
    }
  });

  it("positions host nodes to the left of proc nodes", () => {
    const host = graph.nodes.find((n) => n.tier === "host")!;
    const proc = graph.nodes.find((n) => n.tier === "proc")!;
    expect(host.x).toBeLessThan(proc.x);
  });

  it("positions proc nodes to the left of actor mesh nodes", () => {
    const proc = graph.nodes.find((n) => n.tier === "proc")!;
    const am = graph.nodes.find((n) => n.tier === "actor_mesh")!;
    expect(proc.x).toBeLessThan(am.x);
  });

  it("positions actor mesh nodes to the left of actor nodes", () => {
    const am = graph.nodes.find((n) => n.tier === "actor_mesh")!;
    const actor = graph.nodes.find((n) => n.tier === "actor")!;
    expect(am.x).toBeLessThan(actor.x);
  });

  it("computes aggregate status for mesh with failed child", () => {
    // actor_mesh_0_1 (id=8) contains actor 4 which is "failed"
    const amNode = graph.nodes.find((n) => n.id === "mesh-8")!;
    expect(amNode.status).toBe("failed");
  });

  it("computes aggregate status for healthy mesh", () => {
    // actor_mesh_0_0 (id=7) contains actor 2 which is "processing"
    const amNode = graph.nodes.find((n) => n.id === "mesh-7")!;
    expect(amNode.status).toBe("processing");
  });

  it("sets graph dimensions", () => {
    expect(graph.width).toBeGreaterThan(0);
    expect(graph.height).toBeGreaterThan(0);
  });

  it("handles empty input", () => {
    const empty = computeLayout([], [], {}, []);
    expect(empty.nodes.length).toBe(0);
    expect(empty.edges.length).toBe(0);
  });
});
