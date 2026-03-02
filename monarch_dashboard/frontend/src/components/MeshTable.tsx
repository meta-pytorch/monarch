/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from "react";
import { Mesh } from "../types";
import { StatusBadge } from "./StatusBadge";
import { formatTimestamp, formatShape, worstStatus } from "../utils/status";
import { useApi } from "../hooks/useApi";

interface MeshTableProps {
  /** API path to fetch meshes from (e.g. "/meshes?class=Host"). */
  apiPath: string;
  /** Columns to display. */
  columns: ColumnDef[];
  /** Called when a row is clicked. */
  onRowClick: (mesh: Mesh) => void;
  /** Label for the table section. */
  title: string;
}

interface ColumnDef {
  key: string;
  label: string;
}

type SortDir = "asc" | "desc";

/** Reusable sortable table for mesh entities at any hierarchy level. */
export function MeshTable({ apiPath, columns, onRowClick, title }: MeshTableProps) {
  const { data: meshes, loading, error } = useApi<Mesh[]>(apiPath);
  const [sortCol, setSortCol] = useState<string>("id");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  // For each mesh, fetch actors to determine child count + aggregate status.
  // We do a secondary fetch for actors by mesh to compute status.

  const handleSort = (key: string) => {
    if (sortCol === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortCol(key);
      setSortDir("asc");
    }
  };

  if (loading) return <div className="loading-state">Loading {title}...</div>;
  if (error) return <div className="error-state">Error: {error}</div>;
  if (!meshes || meshes.length === 0) {
    return <div className="empty-state">No {title.toLowerCase()} found</div>;
  }

  const sorted = [...meshes].sort((a, b) => {
    const aVal = (a as any)[sortCol] ?? "";
    const bVal = (b as any)[sortCol] ?? "";
    const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
    return sortDir === "asc" ? cmp : -cmp;
  });

  return (
    <div className="table-section">
      <h2 className="table-title">{title}</h2>
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
            {sorted.map((mesh) => (
              <MeshRow
                key={mesh.id}
                mesh={mesh}
                columns={columns}
                onClick={() => onRowClick(mesh)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** Individual mesh row that fetches its own actor data for status. */
function MeshRow({
  mesh,
  columns,
  onClick,
}: {
  mesh: Mesh;
  columns: ColumnDef[];
  onClick: () => void;
}) {
  const { data: actors } = useApi<any[]>(`/actors?mesh_id=${mesh.id}`);
  const { data: children } = useApi<Mesh[]>(`/meshes/${mesh.id}/children`);

  const childCount = children?.length ?? 0;
  const actorCount = actors?.length ?? 0;

  // Aggregate status from actors' latest status.
  const statuses = (actors ?? []).map((a: any) => a.latest_status);
  const aggStatus = statuses.length > 0 ? worstStatus(statuses) : null;

  // If this mesh has children but no direct actors, look at child meshes
  // (we'll approximate from given data).

  const cellValue = (key: string): React.ReactNode => {
    switch (key) {
      case "given_name": return mesh.given_name;
      case "full_name": return <span className="mono-cell">{mesh.full_name}</span>;
      case "class": return <span className="class-tag">{mesh.class}</span>;
      case "shape_json": return <span className="mono-cell">{formatShape(mesh.shape_json)}</span>;
      case "children": return childCount;
      case "actors": return actorCount;
      case "status": return <StatusBadge status={aggStatus} />;
      case "timestamp_us": return <span className="mono-cell">{formatTimestamp(mesh.timestamp_us)}</span>;
      case "id": return mesh.id;
      default: return (mesh as any)[key] ?? "—";
    }
  };

  return (
    <tr onClick={onClick} className="clickable-row">
      {columns.map((col) => (
        <td key={col.key}>{cellValue(col.key)}</td>
      ))}
    </tr>
  );
}
