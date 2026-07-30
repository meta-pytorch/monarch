/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { useMemo } from "react";
import { useApi } from "./useApi";
import { isSystemActor } from "./format";
import { Actor } from "../types";

/**
 * Per-status health weight. The dashboard's own health number is derived from
 * the *current* state of each actor (from /api/actors) so it stays consistent
 * with the actor total and the "N healthy" caption — unlike the backend's
 * summary.health_score, which is a weighted average over historical status
 * *events* (and so dings normal states like "client").
 */
const WEIGHT: Record<string, number> = {
  idle: 100,
  client: 100,
  processing: 100,
  saving: 100,
  loading: 95,
  created: 90,
  initializing: 90,
  stopping: 30,
  unknown: 60,
  failed: 0,
  stopped: 0,
};

export interface ActorStats {
  actors: Actor[];
  total: number;
  /**
   * Count per current state — sums to `total`. Monarch infra actors are
   * bucketed under "system" rather than their (usually null) status.
   */
  byStatus: Record<string, number>;
  /** Health over workload actors only (system actors excluded). */
  health: number;
  healthy: number;
  unknown: number;
  down: number;
  /** Infra actors (agents, loggers, controllers, …). */
  system: number;
  /** Non-system actors the health score is computed over. */
  workload: number;
  loading: boolean;
  updatedAt: number | null;
}

/** Authoritative, internally-consistent actor stats from /api/actors. */
export function useActorStats(pollMs = 3000): ActorStats {
  const { data, loading, updatedAt } = useApi<Actor[]>("/actors", pollMs);
  return useMemo(() => {
    const actors = data ?? [];
    const byStatus: Record<string, number> = {};
    let sum = 0;
    let known = 0; // workload actors with a reported status (health denominator)
    let healthy = 0;
    let unknown = 0;
    let down = 0;
    let system = 0;
    let workload = 0;
    for (const a of actors) {
      if (isSystemActor(a.full_name)) {
        byStatus.system = (byStatus.system ?? 0) + 1;
        system++;
        continue;
      }
      const s = (a.latest_status ?? "unknown").toLowerCase();
      byStatus[s] = (byStatus[s] ?? 0) + 1;
      workload++;
      // A missing/unreported status is a telemetry gap, not a health signal, so
      // it's counted for display but EXCLUDED from the health average — a
      // momentary null must never make a healthy actor look degraded.
      if (a.latest_status == null || s === "unknown") {
        unknown++;
        continue;
      }
      sum += WEIGHT[s] ?? 60;
      known++;
      if ((WEIGHT[s] ?? 60) === 100) healthy++;
      if (s === "failed" || s === "stopped") down++;
    }
    const total = actors.length;
    const health = known ? Math.round(sum / known) : 100;
    return { actors, total, byStatus, health, healthy, unknown, down, system, workload, loading, updatedAt };
  }, [data, loading, updatedAt]);
}
