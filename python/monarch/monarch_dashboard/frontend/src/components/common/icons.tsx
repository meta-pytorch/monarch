/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";

type P = { className?: string; size?: number; style?: React.CSSProperties };

const svg = (children: React.ReactNode) => ({ className, size = 16, style }: P) => (
  <svg
    className={className}
    style={style}
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.8"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    {children}
  </svg>
);

export const IconOverview = svg(
  <>
    <rect x="3" y="3" width="7" height="9" rx="1.5" />
    <rect x="14" y="3" width="7" height="5" rx="1.5" />
    <rect x="14" y="12" width="7" height="9" rx="1.5" />
    <rect x="3" y="16" width="7" height="5" rx="1.5" />
  </>
);

export const IconTopology = svg(
  <>
    <circle cx="12" cy="5" r="2.4" />
    <circle cx="5" cy="18" r="2.4" />
    <circle cx="19" cy="18" r="2.4" />
    <path d="M10.4 6.8 6.6 15.9M13.6 6.8l3.8 9.1M7.4 18h9.2" />
  </>
);

export const IconHierarchy = svg(
  <>
    <rect x="9" y="3" width="6" height="4" rx="1" />
    <rect x="3" y="17" width="6" height="4" rx="1" />
    <rect x="15" y="17" width="6" height="4" rx="1" />
    <path d="M12 7v4M6 17v-3h12v3M12 11v3" />
  </>
);

export const IconHost = svg(
  <>
    <rect x="3" y="4" width="18" height="7" rx="1.5" />
    <rect x="3" y="13" width="18" height="7" rx="1.5" />
    <path d="M7 7.5h.01M7 16.5h.01" />
  </>
);

export const IconProc = svg(
  <>
    <rect x="6" y="6" width="12" height="12" rx="1.5" />
    <path d="M9 1v3M15 1v3M9 20v3M15 20v3M1 9h3M1 15h3M20 9h3M20 15h3" />
  </>
);

export const IconActor = svg(
  <>
    <path d="M12 2 3 7v10l9 5 9-5V7z" />
    <path d="M3 7l9 5 9-5M12 12v10" />
  </>
);

export const IconMessage = svg(
  <>
    <path d="M3 8h13l-3-3M21 16H8l3 3" />
  </>
);

export const IconDelivery = svg(
  <>
    <path d="M20 6 9 17l-5-5" />
  </>
);

export const IconHealth = svg(
  <>
    <path d="M3 12h4l2-6 4 12 2-6h6" />
  </>
);

export const IconAlert = svg(
  <>
    <path d="M12 3 2 20h20L12 3z" />
    <path d="M12 10v4M12 17h.01" />
  </>
);

export const IconClose = svg(<path d="M6 6l12 12M18 6 6 18" />);
export const IconFit = svg(
  <>
    <path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5" />
  </>
);
export const IconExpand = svg(<path d="M12 5v14M5 12h14" />);
export const IconCollapse = svg(<path d="M5 12h14" />);
export const IconDirection = svg(
  <>
    <path d="M12 4v16M12 20l-4-4M12 20l4-4" />
  </>
);
export const IconEye = svg(
  <>
    <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z" />
    <circle cx="12" cy="12" r="2.5" />
  </>
);
export const IconRefresh = svg(
  <>
    <path d="M21 12a9 9 0 1 1-3-6.7L21 7" />
    <path d="M21 3v4h-4" />
  </>
);
export const IconLayers = svg(
  <>
    <path d="M12 3 2 8l10 5 10-5-10-5z" />
    <path d="M2 12l10 5 10-5M2 16l10 5 10-5" />
  </>
);
export const IconChevron = svg(<path d="M9 6l6 6-6 6" />);
export const IconPyspy = svg(
  <>
    <circle cx="12" cy="12" r="9" />
    <path d="M12 3v4M12 17v4M3 12h4M17 12h4M6 6l2.5 2.5M18 6l-2.5 2.5" />
    <circle cx="12" cy="12" r="2.5" />
  </>
);
export const IconSearch = svg(
  <>
    <circle cx="11" cy="11" r="7" />
    <path d="M21 21l-4.3-4.3" />
  </>
);
