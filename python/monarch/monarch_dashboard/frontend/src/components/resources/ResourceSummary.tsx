/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { useApi } from "../../lib/useApi";
import { bytes, percent, ResourceInventory, sampleAge } from "../../lib/resources";
import { Metric } from "./HostResources";

export function ResourceSummary({ onOpen }: { onOpen: () => void }) {
  const { data, error, updatedAt } = useApi<ResourceInventory>("/resources", 5000);
  if (!data) return <p className="resource-note">{error ? "Resource telemetry unavailable." : "Waiting for resource telemetry…"}</p>;
  const fresh = (timestamp: number) => (sampleAge(timestamp, data.generated_at_us, updatedAt) ?? Infinity) <= 10;
  const hosts = data.hosts.flatMap(h => h.latest && fresh(h.latest.timestamp_us) ? [h.latest] : []);
  const cpuHosts = hosts.filter(h => h.cpu_count != null && h.cpu_count > 0);
  const cpus = cpuHosts.reduce((n, h) => n + (h.cpu_count ?? 0), 0);
  const cpu = cpus ? cpuHosts.reduce((n, h) => n + h.cpu_percent * (h.cpu_count ?? 0), 0) / cpus : null;
  const total = hosts.reduce((n, h) => n + h.memory_total_bytes, 0);
  const available = hosts.reduce((n, h) => n + h.memory_available_bytes, 0);
  const gpus = data.hosts.flatMap(h => h.gpus.filter(g => fresh(g.timestamp_us)));
  const used = gpus.reduce((n, g) => n + g.memory_used_bytes, 0);
  return (
    <section className="panel">
      <div className="panel-head"><h3>Resources</h3><span className="sub">physical hosts · fresh samples only</span><div className="spacer" /><button className="btn sm" onClick={onOpen}>View Resources →</button></div>
      <div className="panel-body stack">
        <div className="resource-kpis">
          <Metric label="Host samples" value={`${hosts.length} / ${data.hosts.length}`} note="fresh / observed hosts" />
          <Metric label="Weighted CPU" value={percent(cpu)} note={`${cpuHosts.length} hosts with CPU counts`} />
          <Metric label="Available RAM" value={total ? bytes(available) : "—"} note={total ? `${percent(100 * (1 - available / total))} RAM pressure` : "No fresh readings"} />
          <Metric label="GPU memory used" value={gpus.length ? bytes(used) : "—"} note={`${gpus.length} GPUs with fresh samples`} />
        </div>
        <p className="resource-note">Host-wide readings include other jobs and may include the controller. Missing or stale samples are excluded.{data.truncated && " Inventory is incomplete: response limit reached."}{error && " Refresh failed; showing previously fetched data."}</p>
      </div>
    </section>
  );
}
