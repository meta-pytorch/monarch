/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from "react";
import { Actor } from "../types";
import { StatusBadge } from "./StatusBadge";
import { formatTimestamp } from "../utils/status";
import { useApi } from "../hooks/useApi";

interface ActorTableProps {
  meshId: number;
  onActorClick: (actor: Actor) => void;
}

type SortDir = "asc" | "desc";

/** Table listing actors in a given mesh, with latest status. */
export function ActorTable({ meshId, onActorClick }: ActorTableProps) {
  const { data: actors, loading, error } = useApi<Actor[]>(`/actors?mesh_id=${meshId}`);
  const [sortCol, setSortCol] = useState<string>("rank");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const handleSort = (key: string) => {
    if (sortCol === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortCol(key);
      setSortDir("asc");
    }
  };

  if (loading) return <div className="loading-state">Loading actors...</div>;
  if (error) return <div className="error-state">Error: {error}</div>;
  if (!actors || actors.length === 0) {
    return <div className="empty-state">No actors in this mesh</div>;
  }

  const sorted = [...actors].sort((a, b) => {
    const aVal = (a as any)[sortCol] ?? "";
    const bVal = (b as any)[sortCol] ?? "";
    const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
    return sortDir === "asc" ? cmp : -cmp;
  });

  const columns = [
    { key: "full_name", label: "Name" },
    { key: "rank", label: "Rank" },
    { key: "latest_status", label: "Status" },
    { key: "latest_status_timestamp_us", label: "Last Updated" },
  ];

  return (
    <div className="table-section">
      <h2 className="table-title">Actors</h2>
      <div className="table-wrapper">
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  className={sortCol === col.key ? `sorted-${sortDir}` : ""}
                >
                  {col.label}
                  {sortCol === col.key && (
                    <span className="sort-arrow">
                      {sortDir === "asc" ? " \u2191" : " \u2193"}
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((actor) => (
              <ActorRow key={actor.id} actor={actor} onClick={() => onActorClick(actor)} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** Individual actor row that fetches its latest status. */
function ActorRow({ actor, onClick }: { actor: Actor; onClick: () => void }) {
  // The /actors?mesh_id= endpoint returns base actor data without latest_status.
  // Fetch the single-actor endpoint which includes it.
  const { data: detail } = useApi<Actor>(`/actors/${actor.id}`);
  const status = detail?.latest_status ?? null;
  const lastUpdated = detail?.latest_status_timestamp_us;

  return (
    <tr onClick={onClick} className="clickable-row">
      <td><span className="mono-cell">{actor.full_name}</span></td>
      <td>{actor.rank}</td>
      <td><StatusBadge status={status} /></td>
      <td>
        <span className="mono-cell">
          {lastUpdated ? formatTimestamp(lastUpdated) : "—"}
        </span>
      </td>
    </tr>
  );
}
