/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { statusMeta } from "../../lib/status";
import { formatTime } from "../../lib/format";
import { IconClose } from "./icons";

/** Colored status pill with dot + glyph-free label (color+animation encode). */
export function StatusPill({ status }: { status: string | null | undefined }) {
  const s = (status ?? "unknown").toLowerCase();
  const m = statusMeta(s);
  const pulsing = s === "processing" || s === "initializing" || s === "saving";
  return (
    <span
      className={`pill${pulsing ? " pulsing" : ""}`}
      style={{ ["--pill-color" as string]: m.color }}
    >
      <span className="dot" />
      {s}
    </span>
  );
}

export function Loading({ label = "Loading…" }: { label?: string }) {
  return (
    <div className="state">
      <div className="spinner" />
      {label}
    </div>
  );
}

export function ErrorState({ message }: { message: string }) {
  return <div className="state err">Failed to load — {message}</div>;
}

export function EmptyState({ label }: { label: string }) {
  return <div className="state">{label}</div>;
}

/** Right-side slide-in drawer with scrim. */
export function Drawer({
  title,
  subtitle,
  onClose,
  children,
}: {
  title: React.ReactNode;
  subtitle?: React.ReactNode;
  onClose: () => void;
  children: React.ReactNode;
}) {
  return (
    <>
      <div className="drawer-scrim" onClick={onClose} />
      <aside className="drawer" role="dialog" aria-label="Details">
        <div className="drawer-head">
          <div style={{ flex: 1, minWidth: 0 }}>
            <div className="title">{title}</div>
            {subtitle && <div className="sub">{subtitle}</div>}
          </div>
          <button className="icon-btn" onClick={onClose} aria-label="Close">
            <IconClose size={16} />
          </button>
        </div>
        <div className="drawer-body">{children}</div>
      </aside>
    </>
  );
}

/** Vertical status-event timeline (shared by actor detail + node drawer). */
export function StatusTimeline({
  events,
}: {
  events: Array<{ id: string; new_status: string; timestamp_us: number; reason: string | null }>;
}) {
  if (events.length === 0) return <div className="state">No status events</div>;
  return (
    <div className="tl">
      {events.map((e) => {
        const c = statusMeta(e.new_status).color;
        return (
          <div key={e.id} className="tl-item" style={{ ["--tl-color" as string]: c }}>
            <div className="tl-top">
              <StatusPill status={e.new_status} />
              <span className="tl-time">{formatTime(e.timestamp_us)}</span>
            </div>
            {e.reason && <div className="tl-reason">{e.reason}</div>}
          </div>
        );
      })}
    </div>
  );
}
