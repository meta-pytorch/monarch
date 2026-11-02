/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

export interface HostSample {
  timestamp_us: number;
  hostname: string;
  cpu_percent: number;
  cpu_count: number | null;
  load_avg_1m: number;
  load_avg_5m: number;
  load_avg_15m: number;
  memory_total_bytes: number;
  memory_available_bytes: number;
  memory_used_bytes: number;
  swap_used_bytes: number;
  disk_root_free_bytes: number | null;
  disk_tmp_free_bytes: number | null;
  net_rx_bytes_per_sec?: number | null;
  net_tx_bytes_per_sec?: number | null;
  disk_read_bytes_per_sec?: number | null;
  disk_write_bytes_per_sec?: number | null;
}

export interface GpuSample {
  timestamp_us: number;
  hostname: string;
  gpu_uuid: string;
  gpu_index: number;
  gpu_name: string;
  utilization_percent: number | null;
  memory_used_bytes: number;
  memory_total_bytes: number;
  temperature_c: number | null;
  power_watts: number | null;
}

export interface ResourceHost {
  hostname: string;
  latest: HostSample | null;
  gpus: GpuSample[];
}

export interface ResourceInventory {
  generated_at_us: number;
  hosts: ResourceHost[];
  truncated: boolean;
  host_metrics_available: boolean;
  gpu_metrics_available: boolean;
}

export interface ResourceHistory {
  hostname: string;
  start_us: number;
  end_us: number;
  hosts: HostSample[];
  gpus: GpuSample[];
  truncated: boolean;
  max_gap_seconds: number;
}

export const GiB = 1024 ** 3;
export const MiB = 1024 ** 2;
export const percent = (value: number | null | undefined) => value == null ? "—" : `${value.toFixed(1)}%`;
export const bytes = (value: number | null | undefined) => value == null ? "—" : `${(value / GiB).toFixed(1)} GiB`;
export const pressure = (sample: HostSample) => sample.memory_total_bytes > 0
  ? 100 * (1 - sample.memory_available_bytes / sample.memory_total_bytes) : null;

export function sampleAge(timestamp: number | null | undefined, serverNow: number, updatedAt: number | null) {
  return timestamp == null ? null : Math.max(0, (serverNow - timestamp) / 1_000_000 + (updatedAt == null ? 0 : (Date.now() - updatedAt) / 1000));
}

export const ageLabel = (age: number | null) => age == null ? "No sample" : `${age > 10 ? "Stale · " : ""}${Math.floor(age)}s ago`;

export function points<T extends { timestamp_us: number }>(samples: T[], value: (s: T) => number | null | undefined, maxGap: number): Array<[number, number | null]> {
  const result: Array<[number, number | null]> = [];
  samples.forEach((s, i) => {
    const prev = samples[i - 1];
    if (prev && s.timestamp_us - prev.timestamp_us > maxGap * 1_000_000) result.push([(s.timestamp_us + prev.timestamp_us) / 2000, null]);
    result.push([s.timestamp_us / 1000, value(s) ?? null]);
  });
  return result;
}
