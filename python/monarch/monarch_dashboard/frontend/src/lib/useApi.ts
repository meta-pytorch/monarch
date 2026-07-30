/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import { useEffect, useState } from "react";

const API_BASE = "/api";

export interface ApiState<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  updatedAt: number | null;
  refetch: () => void;
}

/**
 * Shared polling registry keyed by path. Multiple components polling the same
 * endpoint share ONE request + timer (so e.g. the header health chip and the
 * Overview don't each hit /actors), and all polling pauses while the tab is
 * hidden. This is the observer-load optimization: viewing the dashboard should
 * add as little mesh traffic as possible.
 */
interface Entry {
  path: string;
  interval: number;
  data: unknown;
  error: string | null;
  updatedAt: number | null;
  loading: boolean;
  subs: Set<() => void>;
  timer: ReturnType<typeof setInterval> | null;
  inFlight: boolean;
}

const store = new Map<string, Entry>();
let pageVisible =
  typeof document === "undefined" || document.visibilityState !== "hidden";
let visibilityBound = false;

function notify(e: Entry) {
  e.subs.forEach((fn) => fn());
}

function fetchNow(e: Entry) {
  if (e.inFlight) return;
  e.inFlight = true;
  fetch(`${API_BASE}${e.path}`)
    .then((r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    })
    .then((json) => {
      e.data = json;
      e.error = null;
      e.loading = false;
      e.updatedAt = Date.now();
    })
    .catch((err) => {
      e.error = String(err?.message ?? err);
      e.loading = false;
    })
    .finally(() => {
      e.inFlight = false;
      notify(e);
    });
}

function startTimer(e: Entry) {
  if (e.timer || e.interval <= 0 || !pageVisible) return;
  e.timer = setInterval(() => {
    if (pageVisible) fetchNow(e);
  }, e.interval);
}

function stopTimer(e: Entry) {
  if (e.timer) {
    clearInterval(e.timer);
    e.timer = null;
  }
}

function bindVisibility() {
  if (visibilityBound || typeof document === "undefined") return;
  visibilityBound = true;
  document.addEventListener("visibilitychange", () => {
    const v = document.visibilityState !== "hidden";
    if (v === pageVisible) return;
    pageVisible = v;
    if (v) {
      // Resumed: refresh once immediately, then restart timers.
      store.forEach((e) => {
        fetchNow(e);
        startTimer(e);
      });
    } else {
      store.forEach(stopTimer);
    }
  });
}

/**
 * Poll a dashboard API endpoint. `pollMs=0` fetches once. Same-path callers are
 * transparently coalesced onto a single shared poller.
 */
export function useApi<T>(path: string, pollMs = 3000): ApiState<T> {
  const [, force] = useState(0);

  useEffect(() => {
    bindVisibility();
    let e = store.get(path);
    if (!e) {
      e = {
        path,
        interval: pollMs,
        data: null,
        error: null,
        updatedAt: null,
        loading: true,
        subs: new Set(),
        timer: null,
        inFlight: false,
      };
      store.set(path, e);
    } else if (pollMs > 0) {
      // Poll at the fastest cadence any subscriber asked for.
      e.interval = e.interval <= 0 ? pollMs : Math.min(e.interval, pollMs);
    }
    const rerender = () => force((n) => n + 1);
    e.subs.add(rerender);

    if (e.data == null && !e.inFlight) fetchNow(e);
    else rerender();
    startTimer(e);

    return () => {
      const cur = store.get(path);
      if (!cur) return;
      cur.subs.delete(rerender);
      if (cur.subs.size === 0) {
        stopTimer(cur);
        store.delete(path);
      }
    };
  }, [path, pollMs]);

  const e = store.get(path);
  return {
    data: (e?.data ?? null) as T | null,
    loading: e?.loading ?? true,
    error: e?.error ?? null,
    updatedAt: e?.updatedAt ?? null,
    refetch: () => {
      const cur = store.get(path);
      if (cur) fetchNow(cur);
    },
  };
}
