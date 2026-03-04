/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { computeLayout, DagGraph } from "../utils/dagLayout";
import { Mesh, Actor } from "../types";

const meshes: Mesh[] = [
  { id: 1, timestamp_us: 0, class: "Host", given_name: "host_mesh_0", full_name: "host_mesh_0", shape_json: '{"dims": [1]}', parent_mesh_id: null, parent_view_json: null },
  { id: 2, timestamp_us: 0, class: "Host", given_name: "host_mesh_1", full_name: "host_mesh_1", shape_json: '{"dims": [1]}', parent_mesh_id: null, parent_view_json: null },
  { id: 3, timestamp_us: 0, class: "Proc", given_name: "proc_mesh_0", full_name: "host_mesh_0/proc_mesh_0", shape_json: '{"dims": [1]}', parent_mesh_id: 1, parent_view_json: '{"offset": [0], "sizes": [1]}' },
  { id: 4, timestamp_us: 0, class: "Proc", given_name: "proc_mesh_0", full_name: "host_mesh_1/proc_mesh_0", shape_json: '{"dims": [1]}', parent_mesh_id: 2, parent_view_json: '{"offset": [0], "sizes": [1]}' },
  { id: 5, timestamp_us: 0, class: "Python<Trainer>", given_name: "Python<Trainer>", full_name: "host_mesh_0/proc_mesh_0/Python<Trainer>", shape_json: '{"dims": [2]}', parent_mesh_id: 3, parent_view_json: '{"offset": [0], "sizes": [1]}' },
  { id: 6, timestamp_us: 0, class: "Python<Trainer>", given_name: "Python<Trainer>", full_name: "host_mesh_1/proc_mesh_0/Python<Trainer>", shape_json: '{"dims": [2]}', parent_mesh_id: 4, parent_view_json: '{"offset": [0], "sizes": [1]}' },
];

const actors: Actor[] = [
  // System agents — rendered as host_unit / proc_unit nodes.
  { id: 1, timestamp_us: 0, mesh_id: 1, rank: 0, full_name: "host_mesh_0/HostAgent[0]" },
  { id: 2, timestamp_us: 0, mesh_id: 2, rank: 0, full_name: "host_mesh_1/HostAgent[0]" },
  { id: 3, timestamp_us: 0, mesh_id: 3, rank: 0, full_name: "host_mesh_0/proc_mesh_0/ProcAgent[0]" },
  { id: 4, timestamp_us: 0, mesh_id: 4, rank: 0, full_name: "host_mesh_1/proc_mesh_0/ProcAgent[0]" },
  // Regular actors.
  { id: 5, timestamp_us: 0, mesh_id: 5, rank: 0, full_name: "host_mesh_0/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[0]" },
  { id: 6, timestamp_us: 0, mesh_id: 5, rank: 1, full_name: "host_mesh_0/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[1]" },
  { id: 7, timestamp_us: 0, mesh_id: 6, rank: 0, full_name: "host_mesh_1/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[0]" },
  { id: 8, timestamp_us: 0, mesh_id: 6, rank: 1, full_name: "host_mesh_1/proc_mesh_0/Python<Trainer>/PythonActor<Trainer>[1]" },
];

const statuses: Record<number, string> = {
  1: "idle",       // HostAgent host_mesh_0
  2: "failed",     // HostAgent host_mesh_1 (terminal)
  3: "idle",       // ProcAgent under host_mesh_0
  4: "idle",       // ProcAgent under host_mesh_1
  5: "idle",
  6: "processing",
  7: "idle",
  8: "idle",
};

const messagePairs: Array<[number, number]> = [
  [6, 8],
  [8, 6],
  [6, 8], // duplicate
];

describe("computeLayout", () => {
  let graph: DagGraph;

  beforeAll(() => {
    graph = computeLayout(meshes, actors, statuses, messagePairs);
  });

  it("creates correct total node count", () => {
    // 2 host meshes + 2 host units + 2 proc meshes + 2 proc units
    // + 2 actor meshes + 4 regular actors = 14
    expect(graph.nodes.length).toBe(14);
  });

  it("creates host_unit and proc_unit nodes", () => {
    expect(graph.nodes.filter((n) => n.tier === "host_unit").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "proc_unit").length).toBe(2);
  });

  it("creates mesh nodes", () => {
    expect(graph.nodes.filter((n) => n.tier === "host_mesh").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "proc_mesh").length).toBe(2);
    expect(graph.nodes.filter((n) => n.tier === "actor_mesh").length).toBe(2);
  });

  it("creates actor nodes for regular actors only", () => {
    expect(graph.nodes.filter((n) => n.tier === "actor").length).toBe(4);
  });

  it("meshes have n/a status", () => {
    const meshNodes = graph.nodes.filter(
      (n) => n.tier === "host_mesh" || n.tier === "proc_mesh"
    );
    for (const n of meshNodes) {
      expect(n.status).toBe("n/a");
    }
  });

  it("host_unit status comes from HostAgent", () => {
    expect(graph.nodes.find((n) => n.id === "host_unit-1")!.status).toBe("idle");
    expect(graph.nodes.find((n) => n.id === "host_unit-2")!.status).toBe("failed");
  });

  it("propagates terminal host down to proc_unit", () => {
    expect(graph.nodes.find((n) => n.id === "proc_unit-4")!.status).toBe("failed");
    expect(graph.nodes.find((n) => n.id === "proc_unit-3")!.status).toBe("idle");
  });

  it("propagates terminal host down to actors", () => {
    expect(graph.nodes.find((n) => n.id === "actor-7")!.status).toBe("failed");
    expect(graph.nodes.find((n) => n.id === "actor-8")!.status).toBe("failed");
    expect(graph.nodes.find((n) => n.id === "actor-5")!.status).toBe("idle");
  });

  it("creates hierarchy edges flowing through units", () => {
    const hierEdges = graph.edges.filter((e) => e.type === "hierarchy");
    // 2 host_mesh->host_unit + 2 host_unit->proc_mesh
    // + 2 proc_mesh->proc_unit + 2 proc_unit->actor_mesh
    // + 4 actor_mesh->actor = 12
    expect(hierEdges.length).toBe(12);
  });

  it("creates deduplicated message edges", () => {
    expect(graph.edges.filter((e) => e.type === "message").length).toBe(2);
  });

  it("assigns positive coordinates", () => {
    for (const n of graph.nodes) {
      expect(n.x).toBeGreaterThan(0);
      expect(n.y).toBeGreaterThan(0);
    }
  });

  it("host_unit label shows host name without mesh", () => {
    const hu = graph.nodes.find((n) => n.id === "host_unit-1")!;
    expect(hu.label).toBe("host_0");
  });

  it("proc_unit label shows proc name without mesh", () => {
    const pu = graph.nodes.find((n) => n.id === "proc_unit-3")!;
    expect(pu.label).toBe("proc_0");
  });
    const empty = computeLayout([], [], {}, []);
    expect(empty.nodes.length).toBe(0);
    expect(empty.edges.length).toBe(0);
  });
});
