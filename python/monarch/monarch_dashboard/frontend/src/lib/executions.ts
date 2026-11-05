/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import type { Execution } from "../types";

export const EXECUTION_STATUS: Record<
  Execution["status"],
  { label: string; color: string }
> = {
  running: { label: "Running", color: "var(--st-processing)" },
  succeeded: { label: "Succeeded", color: "var(--st-idle)" },
  failed: { label: "Failed", color: "var(--st-failed)" },
  interrupted: { label: "Interrupted", color: "var(--st-stopping)" },
  unknown: { label: "Unknown", color: "var(--st-unknown)" },
};

/** Elapsed execution time, including jobs which are still running. */
export function executionDuration(
  execution: Execution,
  now = Date.now(),
): string {
  const seconds = Math.max(
    0,
    ((execution.finished_us ?? now * 1000) - execution.started_us) / 1e6,
  );
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  if (seconds < 3600)
    return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}
