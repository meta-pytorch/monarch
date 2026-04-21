/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React, { useState } from "react";
import { Actor, ActorStatusEvent, EntityId, Message, MessageStatusEvent } from "../types";
import { StatusBadge } from "./StatusBadge";
import { formatTimestamp, messageStatusColor, splitMessages } from "../utils/status";
import { useApi } from "../hooks/useApi";

interface ActorDetailProps {
  actorId: EntityId;
}

/** Actor detail view: metadata, status timeline, incoming/outgoing messages. */
export function ActorDetail({ actorId }: ActorDetailProps) {
  const { data: actor, loading: aLoading } = useApi<Actor>(`/actors/${actorId}`);
  const { data: events, loading: eLoading } = useApi<ActorStatusEvent[]>(
    `/actors/${actorId}/status_events`
  );
  const { data: messages, loading: mLoading } = useApi<Message[]>(
    `/actors/${actorId}/messages`
  );

  if (aLoading || eLoading || mLoading) {
    return <div className="loading-state">Loading actor detail...</div>;
  }
  if (!actor) {
    return <div className="error-state">Actor not found</div>;
  }

  const { incoming, outgoing } = splitMessages(messages ?? [], actorId);

  return (
    <div className="actor-detail">
      {/* Metadata card */}
      <div className="detail-card">
        <h2 className="detail-card-title">Actor Info</h2>
        <dl className="detail-meta">
          <div className="meta-row">
            <dt>Name</dt>
            <dd className="mono-cell">{actor.full_name}</dd>
          </div>
          <div className="meta-row">
            <dt>ID</dt>
            <dd>{actor.id}</dd>
          </div>
          <div className="meta-row">
            <dt>Rank</dt>
            <dd>{actor.rank}</dd>
          </div>
          <div className="meta-row">
            <dt>Mesh ID</dt>
            <dd>{actor.mesh_id}</dd>
          </div>
          <div className="meta-row">
            <dt>Status</dt>
            <dd><StatusBadge status={actor.latest_status} /></dd>
          </div>
          <div className="meta-row">
            <dt>Created</dt>
            <dd className="mono-cell">{formatTimestamp(actor.timestamp_us)}</dd>
          </div>
        </dl>
      </div>

      {/* Status timeline */}
      <div className="detail-card">
        <h2 className="detail-card-title">
          Status Timeline
          <span className="count-badge">{events?.length ?? 0}</span>
        </h2>
        <div className="timeline">
          {(events ?? []).map((evt) => (
            <div key={evt.id} className="timeline-item">
              <div className="timeline-marker">
                <StatusBadge status={evt.new_status} />
              </div>
              <div className="timeline-content">
                <span className="mono-cell timeline-time">
                  {formatTimestamp(evt.timestamp_us)}
                </span>
                {evt.reason && (
                  <span className="timeline-reason">{evt.reason}</span>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Messages */}
      <div className="detail-card-row">
        <MessageTable
          title="Incoming Messages"
          messages={incoming}
          directionLabel="From"
          directionKey="from_actor_id"
        />
        <MessageTable
          title="Outgoing Messages"
          messages={outgoing}
          directionLabel="To"
          directionKey="to_actor_id"
        />
      </div>
    </div>
  );
}

function MessageTable({
  title,
  messages,
  directionLabel,
  directionKey,
}: {
  title: string;
  messages: Message[];
  directionLabel: string;
  directionKey: "from_actor_id" | "to_actor_id";
}) {
  const [expandedId, setExpandedId] = useState<EntityId | null>(null);

  const toggleExpand = (id: EntityId) => {
    setExpandedId((prev) => (prev === id ? null : id));
  };

  return (
    <div className="detail-card">
      <h2 className="detail-card-title">
        {title}
        <span className="count-badge">{messages.length}</span>
      </h2>
      {messages.length === 0 ? (
        <div className="empty-state">No messages</div>
      ) : (
        <div className="table-wrapper table-compact">
          <table className="data-table">
            <thead>
              <tr>
                <th>{directionLabel}</th>
                <th>Endpoint</th>
                <th>Status</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody>
              {messages.map((msg) => (
                <React.Fragment key={msg.id}>
                  <tr
                    className="clickable-row"
                    onClick={() => toggleExpand(msg.id)}
                  >
                    <td>Actor #{msg[directionKey]}</td>
                    <td>
                      <span className="endpoint-tag">{msg.endpoint ?? "—"}</span>
                    </td>
                    <td>
                      {msg.latest_status ? (
                        <span
                          style={{ color: messageStatusColor(msg.latest_status) }}
                        >
                          {msg.latest_status}
                        </span>
                      ) : "—"}
                    </td>
                    <td>
                      <span className="mono-cell">
                        {formatTimestamp(msg.timestamp_us)}
                      </span>
                    </td>
                  </tr>
                  {expandedId === msg.id && (
                    <tr className="msg-status-expanded-row">
                      <td colSpan={4}>
                        <MessageStatusTimeline messageId={msg.id} />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

/** Fetches and displays the full status event timeline for a single message. */
function MessageStatusTimeline({ messageId }: { messageId: EntityId }) {
  const { data: events, loading } = useApi<MessageStatusEvent[]>(
    `/message_status_events?message_id=${messageId}`,
    0,
  );

  if (loading) {
    return <div className="msg-status-timeline-loading">Loading events...</div>;
  }
  if (!events || events.length === 0) {
    return <div className="msg-status-timeline-empty">No status events</div>;
  }

  return (
    <div className="msg-status-timeline">
      {events.map((evt, i) => (
        <div key={evt.id} className="msg-status-step">
          <span
            className="msg-status-dot"
            style={{ background: messageStatusColor(evt.status) }}
          />
          <span
            className="msg-status-label"
            style={{ color: messageStatusColor(evt.status) }}
          >
            {evt.status}
          </span>
          <span className="mono-cell msg-status-time">
            {formatTimestamp(evt.timestamp_us)}
          </span>
          {i < events.length - 1 && <span className="msg-status-arrow">→</span>}
        </div>
      ))}
    </div>
  );
}
