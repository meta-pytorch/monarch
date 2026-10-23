/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import assert from "node:assert/strict";
import { test } from "node:test";
import type { Actor } from "../src/types";
import { computeActorHealth, fleetActorSubtitle } from "../src/lib/actorHealth";
import { healthMeta, statusMeta } from "../src/lib/status";

function actor(id: number, status: string | null, role = "BashActor", isSystem: boolean | null = false): Actor {
  return { id: String(id), timestamp_us: id, mesh_id: "1", rank: 0,
    full_name: `${role}<actor${id}>.worker<proc>@tcp://worker:1234`, latest_status: status, is_system: isSystem };
}

test("successful iterative executions retain stopped actors without lowering health", () => {
  const actors = [actor(0, "Idle"), actor(1, "Processing")];
  for (let iteration = 0; iteration < 5; iteration++) {
    for (let rank = 0; rank < 16; rank++) actors.push(actor(actors.length, "Stopped"));
    const stats = computeActorHealth(actors);
    assert.equal(stats.health, "healthy");
    assert.equal(stats.down, 0);
    assert.equal(stats.healthy, 2);
    assert.equal(stats.stopped, (iteration + 1) * 16);
    assert.equal(stats.byStatus.stopped, stats.stopped);
    assert.equal(stats.total, actors.length);
  }
});

test("every normal live lifecycle state is healthy", () => {
  for (const state of ["Created", "Initializing", "Loading", "Client", "Idle", "Processing", "Saving", "Stopping"]) {
    const stats = computeActorHealth([actor(0, state)]);
    assert.equal(stats.health, "healthy", state);
    assert.equal(stats.healthy, 1, state);
    assert.equal(stats.down, 0, state);
    assert.equal(healthMeta(stats.health).label, "Healthy", state);
  }
  assert.equal(statusMeta("Stopping").kind, "transitional");
  assert.equal(statusMeta("Stopped").kind, "neutral");
});

test("a failure cannot be diluted by healthy actors or completed executions", () => {
  const history = Array.from({ length: 1000 }, (_, i) => actor(i, "Stopped"));
  const healthy = Array.from({ length: 1000 }, (_, i) => actor(i + 1000, "Idle"));
  const stats = computeActorHealth([...history, ...healthy, actor(2000, "Failed")]);
  assert.equal(stats.health, "unhealthy");
  assert.equal(stats.down, 1);
  assert.equal(stats.byStatus.failed, 1);
  assert.equal(healthMeta(stats.health).label, "Unhealthy");
  assert.equal(statusMeta("Failed").kind, "error");
});

test("a reported failure takes precedence over missing telemetry", () => {
  const stats = computeActorHealth([actor(0, "Failed"), actor(1, null)]);
  assert.equal(stats.health, "unhealthy");
  assert.equal(stats.unknown, 1);
});

test("system actors and telemetry gaps retain distinct counts", () => {
  const stats = computeActorHealth([
    actor(0, "Client"), actor(1, "Stopping"), actor(2, null),
    actor(3, "unknown"), actor(4, "Stopped", "logger", true),
  ]);
  assert.equal(stats.health, "unknown");
  assert.equal(stats.healthy, 2);
  assert.equal(stats.byStatus.stopping, 1);
  assert.equal(stats.unknown, 2);
  assert.equal(stats.system, 1);
  assert.equal(stats.workload, 4);
  assert.equal(stats.down, 0);
  assert.equal(Object.values(stats.byStatus).reduce((a, b) => a + b, 0), stats.total);
});

test("empty or stopped workloads are inactive", () => {
  for (const actors of [[], [actor(0, "Stopped")]]) {
    const stats = computeActorHealth(actors);
    assert.equal(stats.health, "inactive");
    assert.equal(stats.known, 0);
    assert.equal(healthMeta(stats.health).label, "Inactive");
  }
});

test("missing and unrecognized states cannot prove health", () => {
  for (const state of [null, "Unknown", "FutureStatus", ""]) {
    const stats = computeActorHealth([actor(0, "Idle"), actor(1, state)]);
    assert.equal(stats.health, "unknown");
    assert.equal(stats.known, 1);
    assert.equal(stats.unknown, 1);
    assert.equal(healthMeta(stats.health).label, "Unknown");
  }
});

test("health display remains unknown for missing or future health values", () => {
  for (const health of [null, undefined, "", "future-health"]) {
    const band = healthMeta(health);
    assert.equal(band.label, "Unknown");
    assert.equal(band.color, "#8b949e");
  }
});

test("a newer normal status recovers a reported failure", () => {
  const workload = actor(0, "Failed");
  assert.equal(computeActorHealth([workload]).health, "unhealthy");
  workload.latest_status = "Initializing";
  assert.equal(computeActorHealth([workload]).health, "healthy");
});

test("an execution-looking name without record-backed ownership stays in fleet health", () => {
  const failed = actor(0, "Failed");
  failed.full_name = `BashActor<x>.exec_${"a".repeat(32)}<proc>@tcp://worker`;
  const stats = computeActorHealth([failed, actor(1, "Idle")]);
  assert.equal(stats.health, "unhealthy");
  assert.equal(stats.down, 1);
});

test("fleet KPI describes lifecycle history and normal transitions truthfully", () => {
  const stopped = computeActorHealth(Array.from({length: 10}, (_, i) => actor(i, "Stopped")));
  assert.equal(fleetActorSubtitle(stopped), "10 stopped · no live workload");
  assert.equal(fleetActorSubtitle(computeActorHealth([])), "no live workload");
  assert.equal(fleetActorSubtitle(computeActorHealth([actor(0, null)])), "1 unknown");
  assert.equal(fleetActorSubtitle(computeActorHealth([actor(0, "Stopping")])), "all healthy");
  assert.equal(fleetActorSubtitle(computeActorHealth([actor(0, "Idle")])), "all healthy");
  assert.equal(fleetActorSubtitle(computeActorHealth([actor(0, "Failed")])), "1 failed");
  assert.equal(fleetActorSubtitle(computeActorHealth([actor(0, "Idle"), actor(1, "Stopped")])), "1 live · 1 stopped");
});

test("explicit runtime flags determine scope regardless of actor names", () => {
  for (const role of ["logger", "telemetry", "SetupActor", "controller", "HostAgent"]) {
    for (const flag of [false, null]) {
      const stats = computeActorHealth([actor(0, "Failed", role, flag)]);
      assert.equal(stats.system, 0, role);
      assert.equal(stats.health, "unhealthy", role);
    }
  }
  const stats = computeActorHealth([actor(0, "Idle"), actor(1, "Failed", "ordinary", true)]);
  assert.equal(stats.system, 1);
  assert.equal(stats.health, "healthy");
});
