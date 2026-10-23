/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import type { Actor, HealthStatus } from "../types";

const HEALTHY_STATES = new Set([
  "idle", "client", "processing", "saving", "loading", "created", "initializing", "stopping",
]);

/** Current actor counts, with system and stopped actors excluded from health. */
export interface ActorHealth {
  total: number;
  /** Status buckets sum to total, including neutral lifecycle states. */
  byStatus: Record<string, number>;
  health: HealthStatus;
  known: number;
  healthy: number;
  unknown: number;
  down: number;
  system: number;
  workload: number;
  stopped: number;
}

/** Classify reported workload states, preserving normal shutdowns as lifecycle data. */
export function computeActorHealth(actors: readonly Actor[]): ActorHealth {
  const byStatus: Record<string, number> = {};
  let known = 0;
  let healthy = 0;
  let unknown = 0;
  let down = 0;
  let system = 0;
  let workload = 0;
  let stopped = 0;
  for (const actor of actors) {
    if (actor.is_system === true) {
      byStatus.system = (byStatus.system ?? 0) + 1;
      system++;
      continue;
    }
    const status = (actor.latest_status ?? "unknown").toLowerCase();
    byStatus[status] = (byStatus[status] ?? 0) + 1;
    workload++;
    // Successful execs stop their process meshes. Retained shutdown events
    // describe completed work, so neither the numerator nor denominator may
    // treat them as failures. Failed is a distinct terminal runtime verdict.
    if (status === "stopped") {
      stopped++;
      continue;
    }
    if (status !== "failed" && !HEALTHY_STATES.has(status)) {
      unknown++;
      continue;
    }
    known++;
    if (status === "failed") down++;
    else healthy++;
  }
  return {
    total: actors.length,
    byStatus,
    health: down > 0 ? "unhealthy" : unknown > 0 ? "unknown" : known > 0 ? "healthy" : "inactive",
    known,
    healthy,
    unknown,
    down,
    system,
    workload,
    stopped,
  };
}

/** The fleet KPI describes reported coverage and retained lifecycle history. */
export function fleetActorSubtitle(stats: ActorHealth): string {
  if (stats.down > 0) return `${stats.down} failed`;
  if (stats.unknown > 0) return `${stats.unknown} unknown`;
  if (stats.known === 0) {
    return stats.stopped > 0
      ? `${stats.stopped} stopped · no live workload`
      : "no live workload";
  }
  if (stats.stopped > 0) return `${stats.known} live · ${stats.stopped} stopped`;
  if (stats.system > 0) return `${stats.workload} workload · ${stats.system} system`;
  return "all healthy";
}
