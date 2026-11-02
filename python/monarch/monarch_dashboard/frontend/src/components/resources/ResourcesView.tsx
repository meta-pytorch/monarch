/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from "react";
import { useApi } from "../../lib/useApi";
import { ageLabel, bytes, percent, pressure, ResourceHost, ResourceInventory, sampleAge } from "../../lib/resources";
import { EmptyState, ErrorState, Loading } from "../common/ui";
import { HostResources } from "./HostResources";
import "../../styles/resources.css";

export function ResourcesView() {
  const { data, loading, error, updatedAt } = useApi<ResourceInventory>("/resources", 5000);
  const [selected, setSelected] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<{ key: "host" | "cpu" | "ram" | "available" | "gpu" | "age"; descending: boolean }>({ key: "host", descending: false });
  if (loading && !data) return <Loading label="Loading resources…" />;
  if (!data) return error ? <ErrorState message={error} /> : null;
  const value = (host: ResourceHost): string | number | null => {
    switch (sort.key) {
      case "host": return host.hostname;
      case "cpu": return host.latest?.cpu_percent ?? null;
      case "ram": return host.latest ? pressure(host.latest) : null;
      case "available": return host.latest?.memory_available_bytes ?? null;
      case "gpu": return host.gpus.length;
      case "age": return sampleAge(host.latest?.timestamp_us, data.generated_at_us, updatedAt);
    }
  };
  const hosts = data.hosts.filter(h => h.hostname.toLowerCase().includes(search.toLowerCase())).sort((a, b) => {
    const av = value(a), bv = value(b);
    if (av == null) return bv == null ? 0 : 1;
    if (bv == null) return -1;
    const delta = typeof av === "string" && typeof bv === "string" ? av.localeCompare(bv) : Number(av) - Number(bv);
    return sort.descending ? -delta : delta;
  });
  const columns: Array<[typeof sort.key, string]> = [["host", "Host"], ["cpu", "CPU"], ["ram", "RAM pressure"], ["available", "Available RAM"], ["gpu", "GPUs observed"], ["age", "Host sample"]];
  const hostname = selected ?? data.hosts[0]?.hostname;
  return (
    <div className="stack">
      <div className="resource-heading"><h2>Resources</h2><span className="count-tag">{data.hosts.length} physical hosts observed</span></div>
      <p className="resource-note">Host-wide metrics, deduplicated across logical host units. This inventory may include the controller; unavailable collectors are not counted as zero.</p>
      {error && <ErrorState message={error} />}
      {data.truncated && <p className="snapshot-warning">Inventory limit reached. Coverage is incomplete.</p>}
      <section className="panel">
        <div className="panel-head"><h3>Host inventory</h3><div className="spacer" /><input className="resource-search" aria-label="Search hosts" placeholder="Search hosts…" value={search} onChange={e => setSearch(e.target.value)} /></div>
        <div className="resource-table-scroll"><table className="resource-table" aria-label="Host inventory">
          <thead><tr>{columns.map(([key, label]) => <th key={key} scope="col" aria-sort={sort.key === key ? sort.descending ? "descending" : "ascending" : "none"}><button className="resource-sort" onClick={() => setSort({ key, descending: sort.key === key ? !sort.descending : key !== "host" })}>{label}{sort.key === key ? sort.descending ? " ↓" : " ↑" : ""}</button></th>)}</tr></thead>
          <tbody>{hosts.map(h => <tr key={h.hostname} className={hostname === h.hostname ? "selected" : ""}>
            <td><button className="resource-host-link" onClick={() => setSelected(h.hostname)} aria-pressed={hostname === h.hostname}>{h.hostname}</button></td>
            <td>{percent(h.latest?.cpu_percent)}</td><td>{h.latest ? percent(pressure(h.latest)) : "—"}</td><td>{bytes(h.latest?.memory_available_bytes)}</td><td>{h.gpus.length || "—"}</td><td>{ageLabel(sampleAge(h.latest?.timestamp_us, data.generated_at_us, updatedAt))}</td>
          </tr>)}</tbody>
        </table></div>
        {hosts.length === 0 && <EmptyState label={search ? "No matching hosts" : "No resource samples yet. Sampling may be disabled or collectors may be unavailable."} />}
      </section>
      {hostname && <HostResources key={hostname} hostname={hostname} />}
    </div>
  );
}
