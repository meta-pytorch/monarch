/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { useState, useEffect, useCallback, useRef } from "react";

/** API base — proxied in dev, same-origin in prod. */
const API_BASE = "/api";

export interface ApiState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

/**
 * Hook for fetching data from the dashboard API.
 *
 * Triggers a fetch on mount and whenever `path` changes.  Automatically
 * re-fetches every `pollIntervalMs` milliseconds (default 1000).
 * Set `pollIntervalMs` to 0 to disable polling.
 *
 * Only the initial fetch sets loading=true; subsequent polls update
 * data silently to avoid flashing a loading state.
 */
export function useApi<T>(path: string, pollIntervalMs: number = 1000): ApiState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const initialFetchDone = useRef(false);

  const fetchData = useCallback(() => {
    if (!initialFetchDone.current) {
      setLoading(true);
    }
    setError(null);
    fetch(`${API_BASE}${path}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((json) => {
        setData(json as T);
        setLoading(false);
        initialFetchDone.current = true;
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
        initialFetchDone.current = true;
      });
  }, [path]);

  useEffect(() => {
    initialFetchDone.current = false;
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    if (pollIntervalMs <= 0) return;
    const id = setInterval(fetchData, pollIntervalMs);
    return () => clearInterval(id);
  }, [fetchData, pollIntervalMs]);

  return { data, loading, error, refetch: fetchData };
}
