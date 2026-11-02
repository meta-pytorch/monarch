/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { useApi } from "../../lib/useApi";
import { ageLabel, bytes, GiB, HostSample, MiB, percent, points, pressure, ResourceHistory, sampleAge } from "../../lib/resources";
import { EmptyState, ErrorState, Loading } from "../common/ui";
import { ResourceChart } from "./ResourceChart";

export function HostResources({ hostname }: { hostname: string }) {
  const { data, loading, error, updatedAt } = useApi<ResourceHistory>(`/resources/history?hostname=${encodeURIComponent(hostname)}`, 5000);
  if (loading && !data) return <Loading label="Loading host resources…" />;
  if (!data) return error ? <ErrorState message={error} /> : null;
  const samples = data.hosts;
  const latest = samples[samples.length - 1];
  const props = { start: data.start_us, end: data.end_us };
  const series = (name: string, value: (sample: HostSample) => number | null | undefined) => ({ name, data: points(samples, value, data.max_gap_seconds) });
  return (
    <div className="stack host-resources">
      <div className="resource-heading"><h2>{hostname}</h2><span className="count-tag">{ageLabel(sampleAge(latest?.timestamp_us, data.end_us, updatedAt))}</span></div>
      <p className="resource-note">Whole-host readings, including other jobs and the controller. Latest five minutes; gaps are left open.</p>
      {error && <ErrorState message={error} />}
      {data.truncated && <p className="snapshot-warning">Sample limit reached. Recent history may be incomplete.</p>}
      {!latest ? <EmptyState label="No recent host samples. Sampling may be disabled or this collector may be unavailable." /> : (
        <>
          <div className="resource-kpis">
            <Metric label="CPU" value={percent(latest.cpu_percent)} note={`${latest.cpu_count ?? "—"} logical CPUs`} />
            <Metric label="RAM pressure" value={percent(pressure(latest))} note="1 − available / total" />
            <Metric label="Available RAM" value={bytes(latest.memory_available_bytes)} note={`of ${bytes(latest.memory_total_bytes)}`} />
            <Metric label="Swap used" value={bytes(latest.swap_used_bytes)} />
            <Metric label="Free /" value={bytes(latest.disk_root_free_bytes)} />
            <Metric label="Free /tmp" value={bytes(latest.disk_tmp_free_bytes)} />
          </div>
          <div className="resource-charts">
            <ResourceChart {...props} title="CPU activity" unit="%" max={100} series={[series("CPU", s => s.cpu_percent)]} />
            <ResourceChart {...props} title="RAM pressure" unit="%" max={100} series={[series("RAM pressure", pressure)]} />
            <ResourceChart {...props} title="Load average" unit="runnable / waiting tasks" series={[series("1 min", s => s.load_avg_1m), series("5 min", s => s.load_avg_5m), series("15 min", s => s.load_avg_15m)]} />
            <ResourceChart {...props} title="Network traffic" unit="MiB/s" series={[series("Receive", s => s.net_rx_bytes_per_sec == null ? null : s.net_rx_bytes_per_sec / MiB), series("Transmit", s => s.net_tx_bytes_per_sec == null ? null : s.net_tx_bytes_per_sec / MiB)]} />
            <ResourceChart {...props} title="Disk I/O" unit="MiB/s" series={[series("Read", s => s.disk_read_bytes_per_sec == null ? null : s.disk_read_bytes_per_sec / MiB), series("Write", s => s.disk_write_bytes_per_sec == null ? null : s.disk_write_bytes_per_sec / MiB)]} />
            <ResourceChart {...props} title="Free disk space" unit="GiB" series={[series("/", s => s.disk_root_free_bytes == null ? null : s.disk_root_free_bytes / GiB), series("/tmp", s => s.disk_tmp_free_bytes == null ? null : s.disk_tmp_free_bytes / GiB)]} />
          </div>
        </>
      )}
    </div>
  );
}

export function Metric({ label, value, note }: { label: string; value: string; note?: string }) {
  return <div className="resource-kpi"><span>{label}</span><strong>{value}</strong>{note && <small>{note}</small>}</div>;
}
