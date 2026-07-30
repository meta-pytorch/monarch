/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { useMemo } from "react";
import { useApi } from "./useApi";

export interface EndpointVol {
  endpoint: string;
  total: number;
  completed: number;
}

export interface MessageStats {
  /** Distinct messages observed in the window. */
  total: number;
  /** Reached only `queued` — serialized/delivered, not yet picked up by a handler. */
  queued: number;
  /**
   * Reached `active` with no terminal event yet — handler running. (A handler
   * using deferred status reporting also sits here until it reports its own
   * terminal status, so "active" means "picked up, no terminal seen".)
   */
  active: number;
  /** Reached `complete` — handler returned Ok. */
  completed: number;
  /** Reached `failed` — handler errored (terminal). */
  failed: number;
  /** completed / (completed + failed) over messages whose handlers finished; 1 when none have finished. */
  successRate: number;
  /** active + queued — not yet in a terminal state. */
  inProgress: number;
  endpoints: EndpointVol[];
}

/**
 * Raw shape of the `/api/message-stats` response. Computed server-side (see
 * `db.get_message_stats`) so the browser never issues raw per-poll scans of the
 * message tables — those stream back through the telemetry scanner as messages
 * that are themselves recorded as `queued` events, inflating the table being
 * read. `pairs` is the distinct actor->actor list for the topology overlay.
 */
export interface MessageStatsPayload {
  lifecycle: { queued: number; active: number; completed: number; failed: number };
  endpoints: Array<{ endpoint: string; total: number; completed: number }>;
  pairs: Array<[string, string]>;
}

/**
 * Message metrics from the deduped handler lifecycle. Monarch telemetry records
 * only handler-lifecycle states (queued -> active -> complete | failed) with no
 * request/response distinction, so these are presented as lifecycle counts
 * rather than an "acked vs fire-and-forget" split. Reads the shared, cached
 * `/message-stats` endpoint (coalesced with the topology overlay's poll).
 * Returns null until the first successful load (callers fall back to summary).
 */
export function useMessageStats(pollMs = 5000): { data: MessageStats | null; error: boolean } {
  const { data, error } = useApi<MessageStatsPayload>("/message-stats", pollMs);

  const stats = useMemo<MessageStats | null>(() => {
    if (!data) return null;
    const queued = Number(data.lifecycle?.queued) || 0;
    const active = Number(data.lifecycle?.active) || 0;
    const completed = Number(data.lifecycle?.completed) || 0;
    const failed = Number(data.lifecycle?.failed) || 0;
    const finished = completed + failed;
    return {
      total: queued + active + completed + failed,
      queued,
      active,
      completed,
      failed,
      successRate: finished > 0 ? completed / finished : 1,
      inProgress: active + queued,
      endpoints: (data.endpoints ?? []).map((e) => ({
        endpoint: String(e.endpoint ?? "(none)"),
        total: Number(e.total) || 0,
        completed: Number(e.completed) || 0,
      })),
    };
  }, [data]);

  return { data: stats, error: error != null };
}
