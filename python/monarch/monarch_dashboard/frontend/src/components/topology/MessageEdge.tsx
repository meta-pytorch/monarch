/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { EdgeProps } from "@xyflow/react";

/**
 * Message edge rendered as a quadratic arc bowing away from the node row
 * (downward in top-down layout, rightward in left-right). The bow depth scales
 * with endpoint distance so parallel same-tier edges nest instead of crossing.
 */
export function MessageEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  style,
  data,
}: EdgeProps) {
  const dir = (data as { dir?: string } | undefined)?.dir ?? "TB";
  let cx: number;
  let cy: number;
  if (dir === "TB") {
    const sag = Math.min(240, 28 + 0.3 * Math.abs(targetX - sourceX));
    cx = (sourceX + targetX) / 2;
    cy = Math.max(sourceY, targetY) + sag;
  } else {
    const sag = Math.min(240, 28 + 0.3 * Math.abs(targetY - sourceY));
    cx = Math.max(sourceX, targetX) + sag;
    cy = (sourceY + targetY) / 2;
  }
  const d = `M ${sourceX},${sourceY} Q ${cx},${cy} ${targetX},${targetY}`;
  return (
    <path
      id={id}
      d={d}
      className="react-flow__edge-path rf-msg-edge"
      style={style}
      fill="none"
    />
  );
}
