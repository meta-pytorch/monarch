/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useMemo, useState } from "react";
import { useApi } from "../../lib/useApi";
import {
  Actor,
  ActorStatusEvent,
  EntityId,
  Message,
  MessageStatusEvent,
} from "../../types";
import {
  actorInstance,
  actorRole,
  formatDateTime,
  formatTime,
  prettyRole,
  splitMessages,
} from "../../lib/format";
import { messageStatusColor } from "../../lib/status";
import { Loading, StatusPill, StatusTimeline } from "../common/ui";

// Mirrors _DRILL_LIMIT in server/db.py: the drawer shows only the most recent N
// status events / messages because actor_status_events is unbounded.
const DRILL_LIMIT = 200;

function RecentNote({ n }: { n: number }) {
  if (n < DRILL_LIMIT) return null;
  return (
    <span
      style={{ marginLeft: 6, color: "var(--text-3)", fontWeight: 400, fontSize: 11 }}
      title={`Only the most recent ${DRILL_LIMIT} are shown; older history is trimmed.`}
    >
      most recent {DRILL_LIMIT}
    </span>
  );
}

/** A message peer id → readable "Role · instance", or a compact port form. */
function usePeerLabel(): (id: EntityId) => { text: string; title: string; resolved: boolean } {
  const { data: actors } = useApi<Actor[]>("/actors");
  const nameById = useMemo(() => {
    const m = new Map<string, string>();
    (actors ?? []).forEach((a) => m.set(String(a.id), a.full_name));
    return m;
  }, [actors]);
  return (id: EntityId) => {
    const full = nameById.get(String(id));
    if (full) {
      const role = prettyRole(actorRole(full));
      const inst = actorInstance(full);
      return { text: inst ? `${role} · ${inst}` : role, title: full, resolved: true };
    }
    // Mailbox/port pseudo-actor (not a real actor row) — show a compact, stable
    // short form instead of a 19-digit id; full id available on hover.
    const s = String(id);
    return { text: `port ·${s.slice(-5)}`, title: `peer id ${s}`, resolved: false };
  };
}

/** Full actor detail: metadata, status timeline, in/out messages. */
export function ActorDetail({ actorId, compact = false }: { actorId: EntityId; compact?: boolean }) {
  const { data: actor, loading } = useApi<Actor>(`/actors/${actorId}`);
  const { data: events } = useApi<ActorStatusEvent[]>(`/actors/${actorId}/status_events`);
  const { data: messages } = useApi<Message[]>(`/actors/${actorId}/messages`);
  const peerLabel = usePeerLabel();

  if (loading && !actor) return <Loading label="Loading actor…" />;
  if (!actor) return <div className="state err">Actor not found</div>;

  const { incoming, outgoing } = splitMessages(messages ?? [], actorId);

  return (
    <div className="stack">
      <section className="drawer-section">
        <div className="eyebrow">Actor</div>
        <dl className="meta-list">
          <div className="row"><dt>Name</dt><dd className="cell-mono">{actor.full_name}</dd></div>
          <div className="row"><dt>ID</dt><dd className="cell-mono">{actor.id}</dd></div>
          <div className="row"><dt>Rank</dt><dd className="cell-mono">{actor.rank}</dd></div>
          <div className="row"><dt>Mesh</dt><dd className="cell-mono">{actor.mesh_name ?? actor.mesh_id}</dd></div>
          <div className="row"><dt>Status</dt><dd><StatusPill status={actor.latest_status} /></dd></div>
          <div className="row"><dt>Created</dt><dd className="cell-mono">{formatDateTime(actor.timestamp_us)}</dd></div>
        </dl>
      </section>

      <section className="drawer-section">
        <div className="eyebrow">
          Status Timeline <span className="count-tag">{events?.length ?? 0}</span>
          <RecentNote n={events?.length ?? 0} />
        </div>
        <StatusTimeline events={events ?? []} />
      </section>

      <section className="drawer-section">
        <div className="eyebrow">
          Messages
          <span className="count-tag">{incoming.length} in · {outgoing.length} out</span>
          <RecentNote n={messages?.length ?? 0} />
        </div>
        {incoming.length === 0 && outgoing.length === 0 ? (
          <div className="state">No messages</div>
        ) : (
          <div className="table-wrap" style={{ maxHeight: compact ? 260 : 420 }}>
            <table className="tbl">
              <thead>
                <tr><th>Dir</th><th>Peer</th><th>Endpoint</th><th>Status</th></tr>
              </thead>
              <tbody>
                {incoming.slice(0, 40).map((m) => (
                  <MsgRow key={`in-${m.id}`} m={m} dir="in" peer={m.from_actor_id} peerLabel={peerLabel} />
                ))}
                {outgoing.slice(0, 40).map((m) => (
                  <MsgRow key={`out-${m.id}`} m={m} dir="out" peer={m.to_actor_id} peerLabel={peerLabel} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

function MsgRow({
  m,
  dir,
  peer,
  peerLabel,
}: {
  m: Message;
  dir: "in" | "out";
  peer: EntityId;
  peerLabel: (id: EntityId) => { text: string; title: string; resolved: boolean };
}) {
  const [open, setOpen] = useState(false);
  const p = peerLabel(peer);
  return (
    <>
      <tr className="clickable" onClick={() => setOpen((o) => !o)}>
        <td style={{ color: dir === "in" ? "var(--st-processing)" : "var(--accent)" }}>
          {dir === "in" ? "← in" : "→ out"}
        </td>
        <td className={p.resolved ? "" : "cell-mono"} style={p.resolved ? undefined : { color: "var(--text-3)" }} title={p.title}>
          {p.text}
        </td>
        <td><span className="endpoint-tag">{m.endpoint ?? "—"}</span></td>
        <td>
          {m.latest_status ? (
            <span style={{ color: messageStatusColor(m.latest_status) }}>{m.latest_status}</span>
          ) : "—"}
        </td>
      </tr>
      {open && (
        <tr>
          <td colSpan={4} style={{ background: "var(--surface-0)" }}>
            <MsgStatusTimeline messageId={m.id} />
          </td>
        </tr>
      )}
    </>
  );
}

function MsgStatusTimeline({ messageId }: { messageId: EntityId }) {
  const { data: events, loading } = useApi<MessageStatusEvent[]>(
    `/message_status_events?message_id=${messageId}`,
    0
  );
  if (loading) return <div style={{ padding: 6, color: "var(--text-3)" }}>Loading…</div>;
  if (!events || events.length === 0)
    return <div style={{ padding: 6, color: "var(--text-3)" }}>No events</div>;
  return (
    <div className="msg-steps">
      {events.map((e, i) => (
        <span key={e.id} className="msg-step">
          <span className="d" style={{ background: messageStatusColor(e.status) }} />
          <span style={{ color: messageStatusColor(e.status), fontWeight: 600 }}>{e.status}</span>
          <span className="cell-mono" style={{ fontSize: 10 }}>{formatTime(e.timestamp_us)}</span>
          {i < events.length - 1 && <span className="msg-arrow">→</span>}
        </span>
      ))}
    </div>
  );
}
