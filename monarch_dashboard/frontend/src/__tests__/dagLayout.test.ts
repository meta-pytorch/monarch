/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { computeLayout, DagGraph } from "../utils/dagLayout";
import { Mesh, Actor } from "../types";

// Minimal test data matching the actual API response fields.
const meshes: Mesh[] = [
  { id: 1, timestamp_us: 0, class: "Host", given_name: "host_mesh_0", full_name: "host_mesh_0", shape_json: '{"dims": [1]}', parent_mesh_id: null, parent_view_json: null },
  { id: 2, timestamp_us: 0, class: "Host", given_name: "host_mesh_1", full_name: "host_mesh_1", shape_json: '{"dims": [1]}', parent_mesh_id: null, parent_view_json: null },
  { id: 3, timestamp_us: 0, class: "Proc", given_name: "proc_mesh_0", full_name: "host_mesh_0/proc_mesh_0", shape_json: '{"dims": [1]}', parent_mesh_id: 1, parent_view_json: '{"offset": [0], "sizes": [1]}' },
  { id: 4, timestamp_us: 0, class: "Proc", given_name: "proc_mesh_0", full_name: "host_mesh_1/proc_mesh_0", shape_json: '{"dims": [1]}', parent_mesh_id: 2, parent_view_json: '{"offset": [0], "sizes": [1]}' },
  { id: 5, timestamp_us: 0, class: "Python<Trainer>", given_name: "Python<Trainer>", full_name: "host_mesh_0/proc_mesh_0/Python<Trainer>", shape_json: '{"dims": [2]}', parent_mesh_id: 3, parent_view_json: '{"offset": [0], "sizes": [1]}' },
  { id: 6, timestamp_us: 0, class: "Python<Trainer>", given_name: "Python<Trainer>", full_name: "host_mesh_1/proc_mesh_0/Python<Trainer>", shape_json: '{"dims": [2]}', parent_mesh_id: 4, parent_view_json: '{"offset": [0], "sizes": [1]}' },
];

const actors: Actor[] = [
  { id: 1, timestamp_us: 0, mesh_id: 5, rank: 0, full_name: "host_mesh_0/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[0]" },
  { id: 2, timestamp_us: 0, mesh_id: 5, rank: 1, full_name: "host_mesh_0/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[1]" },
  { id: 3, timestamp_us: 0, mesh_id: 6, rank: 0, full_name: "host_mesh_1/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[0]" },
  { id: 4, timestamp_us: 0, mesh_id: 6, rank: 1, full_name: "host_mesh_1/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[1]" },
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

  it("creates nodes for all entities", () => {
    // 2 host meshes + 2 proc meshes + 2 actor meshes + 4 actors = 10
    expect(graph.nodes.length).toBe(10);
  });

  it("creates nodes with correct tiers", () => {
    const hostNodes = graph.nodes.filter((n) => n.tier === "host_mesh");
    const procNodes = graph.nodes.filter((n) => n.tier === "proc_mesh");
    const amNodes = graph.nodes.filter((n) => n.tier === "actor_mesh");
    expect(hostNodes.length).toBe(2);
    expect(procNodes.length).toBe(2);
    expect(amNodes.length).toBe(2);
  });

  it("creates actor nodes", () => {
    const actorNodes = graph.nodes.filter((n) => n.tier === "actor");
    expect(actorNodes.length).toBe(4);
  });

  it("host mesh nodes have largest radius", () => {
    const hostMesh = graph.nodes.find((n) => n.tier === "host_mesh")!;
    const actor = graph.nodes.find((n) => n.tier === "actor")!;
    expect(hostMesh.radius).toBeGreaterThan(actor.radius);
  });

  it("creates hierarchy edges", () => {
    const hierEdges = graph.edges.filter((e) => e.type === "hierarchy");
    // 2 host->proc + 2 proc->am + 4 am->actor = 8
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

  it("positions tiers left to right", () => {
    const hostMesh = graph.nodes.find((n) => n.tier === "host_mesh")!;
    const procMesh = graph.nodes.find((n) => n.tier === "proc_mesh")!;
    const actorMesh = graph.nodes.find((n) => n.tier === "actor_mesh")!;
    const actor = graph.nodes.find((n) => n.tier === "actor")!;
    expect(hostMesh.x).toBeLessThan(procMesh.x);
    expect(procMesh.x).toBeLessThan(actorMesh.x);
    expect(actorMesh.x).toBeLessThan(actor.x);
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
