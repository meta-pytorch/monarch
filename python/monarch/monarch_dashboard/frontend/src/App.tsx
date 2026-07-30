/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from "react";
import { useActorStats } from "./lib/actorStats";
import { healthBand } from "./lib/status";
import { relativeTime } from "./lib/format";
import {
  IconOverview,
  IconTopology,
  IconHierarchy,
} from "./components/common/icons";
import { OverviewView } from "./components/overview/OverviewView";
import { TopologyView } from "./components/topology/TopologyView";
import { HierarchyView } from "./components/hierarchy/HierarchyView";

type ViewId = "overview" | "topology" | "hierarchy";

const NAV: Array<{ id: ViewId; label: string; Icon: React.FC<{ className?: string; size?: number }> }> = [
  { id: "overview", label: "Overview", Icon: IconOverview },
  { id: "topology", label: "Topology", Icon: IconTopology },
  { id: "hierarchy", label: "Explorer", Icon: IconHierarchy },
];

function MonarchLogo() {
  return (
    <svg className="brand-logo" viewBox="0 0 171 170" width="26" height="26" fill="none" aria-hidden="true">
      <g clipPath="url(#mlogo)">
        <path
          d="M87.7837 115.185L20.5159 119.007C14.6855 119.339 10.6965 114.477 10.9063 109.489C11.0701 107.885 11.583 106.326 12.3997 104.94C12.6864 104.512 13.0159 104.095 13.3912 103.696L15.1595 101.817C16.7859 100.574 18.8198 99.7661 21.1686 99.6374L95.9088 95.5456L87.7837 115.185ZM107.124 4.08886C116.809 -6.20282 133.528 4.623 128.123 17.6864L102.076 80.6412L31.4477 84.5075L107.124 4.08886Z"
          fill="#FDBD97"
        />
        <path
          d="M14.0932 118.284C7.37588 111.629 11.727 100.154 21.1636 99.6372L149.005 92.6394C159.639 92.0573 164.707 105.52 156.335 112.109L88.7152 165.328C80.0742 172.128 67.727 171.423 59.9146 163.683L14.0932 118.284Z"
          fill="#EC6C46"
        />
      </g>
      <defs>
        <clipPath id="mlogo">
          <rect width="170" height="170" fill="white" transform="translate(0.84375)" />
        </clipPath>
      </defs>
    </svg>
  );
}

function HealthChip() {
  const { total, health, down, updatedAt, loading } = useActorStats();
  const band = healthBand(health);
  const stale = updatedAt != null && Date.now() - updatedAt > 8000;

  return (
    <>
      {down > 0 && (
        <span className="live-dot stale" title={`${down} actors down`}>
          <i />
          {down} down
        </span>
      )}
      <span className={`live-dot${stale ? " stale" : ""}`} title="Live polling every 2s">
        <i />
        {stale ? "reconnecting" : "live"}
        <span style={{ color: "var(--text-3)", fontWeight: 500 }}>· {relativeTime(updatedAt)}</span>
      </span>
      {!loading && total > 0 && (
        <span className="health-chip">
          <span className="health-chip-label">Health</span>
          <span className="health-chip-val" style={{ color: band.color }}>{health}</span>
          <span className="health-chip-pill" style={{ background: band.color }}>{band.label}</span>
        </span>
      )}
    </>
  );
}

function App() {
  const [view, setView] = useState<ViewId>("overview");

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <MonarchLogo />
          <span className="brand-name">Monarch</span>
          <span className="brand-sub">Dashboard</span>
          <span className="brand-badge">Beta</span>
        </div>
        <nav className="nav" role="tablist">
          {NAV.map(({ id, label, Icon }) => (
            <button
              key={id}
              role="tab"
              aria-selected={view === id}
              className={`nav-item${view === id ? " active" : ""}`}
              onClick={() => setView(id)}
            >
              <Icon className="nav-ico" size={15} />
              {label}
            </button>
          ))}
        </nav>
        <div className="topbar-spacer" />
        <div className="topbar-right">
          <HealthChip />
        </div>
      </header>

      {view === "overview" && (
        <main className="content">
          <OverviewView />
        </main>
      )}
      {view === "topology" && (
        <main className="content full">
          <TopologyView />
        </main>
      )}
      {view === "hierarchy" && (
        <main className="content">
          <HierarchyView />
        </main>
      )}
    </div>
  );
}

export default App;
