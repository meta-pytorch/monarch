/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { useMemo } from "react";
import { useApi } from "./useApi";
import { ActorHealth, computeActorHealth } from "./actorHealth";
import { Actor } from "../types";

export interface ActorStats extends ActorHealth {
  actors: Actor[];
  loading: boolean;
  updatedAt: number | null;
}

/** Authoritative, internally-consistent actor stats from /api/actors. */
export function useActorStats(pollMs = 3000): ActorStats {
  const { data, loading, updatedAt } = useApi<Actor[]>("/actors", pollMs);
  return useMemo(() => {
    const actors = data ?? [];
    return { ...computeActorHealth(actors), actors, loading, updatedAt };
  }, [data, loading, updatedAt]);
}
