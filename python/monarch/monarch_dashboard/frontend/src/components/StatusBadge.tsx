/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";

/** Colored status dot with label. */
export function StatusBadge({ status }: { status: string | null | undefined }) {
  const s = status?.toLowerCase() ?? "unknown";
  return (
    <span className="status-badge" data-status={s}>
      <span className="status-dot" />
      {s}
    </span>
  );
}
