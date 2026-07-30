/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { Handle, Position, NodeProps } from "@xyflow/react";
import { DagTier } from "../../types";
import { statusMeta } from "../../lib/status";
import { IconActor, IconHost, IconProc } from "../common/icons";

export interface MonarchNodeData {
  tier: DagTier;
  label: string;
  subtitle: string;
  status: string;
  isController: boolean;
  meshLabel?: string;
  meshColor?: string;
  count?: number;
  countUnit?: string;
  hasChildren: boolean;
  expanded: boolean;
  hiddenCount: number;
  dimmed: boolean;
  selected: boolean;
  [key: string]: unknown;
}

const MESH_TIERS = new Set(["host_mesh", "proc_mesh", "actor_mesh"]);

function tierGlyph(tier: DagTier, size = 15) {
  if (tier.startsWith("host")) return <IconHost size={size} />;
  if (tier.startsWith("proc")) return <IconProc size={size} />;
  return <IconActor size={size} />;
}

export function MonarchNode({ data }: NodeProps) {
  const d = data as MonarchNodeData;
  const isMesh = MESH_TIERS.has(d.tier);
  const color = d.isController ? "#6b8afd" : statusMeta(d.status).color;
  const pulse = d.status === "processing" || d.status === "idle";
  const showDot = d.status !== "n/a";

  return (
    <div
      className={`rf-node${isMesh ? " mesh" : ""}${d.dimmed ? " dim" : ""}${
        d.selected ? " sel" : ""
      }${pulse ? " pulse" : ""}`}
      style={{ ["--n-color" as string]: color }}
    >
      <Handle type="target" position={Position.Top} id="top" />
      <Handle type="target" position={Position.Left} id="left" />
      <div className="bar" />
      <div className="body">
        <span className="glyph">{tierGlyph(d.tier)}</span>
        <span className="txt">
          <span className="tier">{d.isController ? "Controller · " : ""}{d.subtitle}</span>
          <span className="name">{d.label}</span>
        </span>
        {typeof d.count === "number" && (
          <span className="count" title={`${d.count} ${d.countUnit ?? ""}`.trim()}>
            {d.count}
            {d.countUnit ? <em>{d.countUnit}</em> : null}
          </span>
        )}
        {showDot && <span className="st-dot" />}
      </div>
      {d.meshLabel && (
        <div className="rf-mesh" style={{ ["--m-color" as string]: d.meshColor }}>
          <span className="rf-mesh-dot" />
          <span className="rf-mesh-name">{d.meshLabel}</span>
        </div>
      )}
      {d.hasChildren && !d.expanded && d.hiddenCount > 0 && (
        <span className="kids">+{d.hiddenCount}</span>
      )}
      <Handle type="source" position={Position.Bottom} id="bottom" />
      <Handle type="source" position={Position.Right} id="right" />
      {/* Target-side anchors so message arcs can enter on the same side. */}
      <Handle type="target" position={Position.Bottom} id="bottom-t" />
      <Handle type="target" position={Position.Right} id="right-t" />
    </div>
  );
}
