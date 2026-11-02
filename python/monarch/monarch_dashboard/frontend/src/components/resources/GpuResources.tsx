/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { ageLabel, bytes, GiB, GpuSample, percent, points, ResourceHistory, sampleAge } from "../../lib/resources";
import { EmptyState } from "../common/ui";
import { ResourceChart } from "./ResourceChart";

export function GpuResources({ history, updatedAt }: { history: ResourceHistory; updatedAt: number | null }) {
  const byGpu = new Map<string, GpuSample[]>();
  history.gpus.forEach(s => {
    const samples = byGpu.get(s.gpu_uuid) ?? [];
    samples.push(s);
    byGpu.set(s.gpu_uuid, samples);
  });
  const devices = [...byGpu.values()].sort((a, b) => a[a.length - 1].gpu_index - b[b.length - 1].gpu_index);
  const series = (value: (s: GpuSample) => number | null) => devices.slice(0, 8).map(samples => ({
    name: `GPU ${samples[samples.length - 1].gpu_index}`,
    data: points(samples, value, history.max_gap_seconds),
  }));
  const props = { start: history.start_us, end: history.end_us };
  return (
    <div className="stack">
      <div className="resource-heading"><h2>GPUs</h2><span className="count-tag">{devices.length} devices with recent samples</span></div>
      <p className="resource-note">All devices visible to NVML on this host, including other jobs. Utilization measures kernel-active time. Unsupported readings remain unavailable.</p>
      {devices.length === 0 ? <EmptyState label="No recent GPU readings. This host may have no NVIDIA GPUs, sampling may be disabled, or NVML may be unavailable." /> : (
        <>
          <section className="panel resource-table-scroll"><table className="resource-table" aria-label={`GPU inventory for ${history.hostname}`}>
            <thead><tr><th scope="col">Device</th><th scope="col">Utilization</th><th scope="col">Memory used / total</th><th scope="col">Temperature</th><th scope="col">Power</th><th scope="col">Sample</th></tr></thead>
            <tbody>{devices.map(samples => {
              const gpu = samples[samples.length - 1];
              return <tr key={gpu.gpu_uuid}>
                <td><strong>GPU {gpu.gpu_index}</strong> · {gpu.gpu_name}<div className="resource-note">{gpu.gpu_uuid}</div></td>
                <td>{gpu.utilization_percent == null ? "Unavailable" : percent(gpu.utilization_percent)}</td>
                <td>{bytes(gpu.memory_used_bytes)} / {bytes(gpu.memory_total_bytes)}</td>
                <td>{gpu.temperature_c == null ? "Unavailable" : `${gpu.temperature_c} °C`}</td>
                <td>{gpu.power_watts == null ? "Unavailable" : `${gpu.power_watts.toFixed(1)} W`}</td>
                <td>{ageLabel(sampleAge(gpu.timestamp_us, history.end_us, updatedAt))}</td>
              </tr>;
            })}</tbody>
          </table></section>
          {devices.length > 8 && <p className="resource-note">Charts show the first eight GPUs by device index. All observed devices are listed above.</p>}
          <div className="resource-charts gpu-charts">
            <ResourceChart {...props} title="GPU utilization" unit="%" max={100} series={series(s => s.utilization_percent)} />
            <ResourceChart {...props} title="GPU memory used" unit="GiB" series={series(s => s.memory_used_bytes / GiB)} />
            <ResourceChart {...props} title="GPU temperature" unit="°C" series={series(s => s.temperature_c)} />
            <ResourceChart {...props} title="GPU power" unit="W" series={series(s => s.power_watts)} />
          </div>
        </>
      )}
    </div>
  );
}
