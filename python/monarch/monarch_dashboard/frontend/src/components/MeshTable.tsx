/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from "react";
import { StatusBadge } from "./StatusBadge";
import { formatTimestamp, formatShape, leafName } from "../utils/status";
import { useApi } from "../hooks/useApi";

interface EntityTableProps {
  /** API path to fetch entities from. */
  apiPath: string;
  /** Columns to display. */
  columns: ColumnDef[];
  /** Called when a row is clicked. */
  onRowClick: (entity: any) => void;
  /** Label for the table section. */
  title: string;
  /** Optional client-side filter applied to rows after fetch. */
  clientFilter?: (rows: any[]) => any[];
}

interface ColumnDef {
  key: string;
  label: string;
}

type SortDir = "asc" | "desc";

/** Reusable sortable table for any entity at any hierarchy level. */
export function MeshTable({ apiPath, columns, onRowClick, title, clientFilter }: EntityTableProps) {
  const { data: rawEntities, loading, error } = useApi<any[]>(apiPath);
  const entities = clientFilter && rawEntities ? clientFilter(rawEntities) : rawEntities;
  const [sortCol, setSortCol] = useState<string>("id");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

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
  if (!entities || entities.length === 0) {
    return <div className="empty-state">No {title.toLowerCase()} found</div>;
  }

  const sorted = [...entities].sort((a, b) => {
    const aVal = a[sortCol] ?? "";
    const bVal = b[sortCol] ?? "";
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
            {sorted.map((entity) => (
              <tr
                key={entity.id}
                onClick={() => onRowClick(entity)}
                className="clickable-row"
              >
                {columns.map((col) => (
                  <td key={col.key}>{renderCell(col.key, entity)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function renderCell(key: string, entity: any): React.ReactNode {
  const val = entity[key];
  switch (key) {
    case "given_name":
    case "name":
    case "hostname":
    case "mesh_name":
      return val ?? "\u2014";
    case "mesh_class":
      return val ?? "\u2014";
    case "full_name":
      return <span className="mono-cell">{leafName(val)}</span>;
    case "shape_json":
      return <span className="mono-cell">{formatShape(val)}</span>;
    case "status":
    case "latest_status":
      return <StatusBadge status={val} />;
    case "timestamp_us":
    case "status_timestamp_us":
      return (
        <span className="mono-cell">
          {val ? formatTimestamp(val) : "\u2014"}
        </span>
      );
    case "pid":
    case "id":
    case "rank":
      return val;
    default:
      return val ?? "\u2014";
  }
}
