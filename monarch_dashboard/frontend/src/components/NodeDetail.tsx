/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";
import { DagNode } from "../utils/dagLayout";
import { Actor, ActorStatusEvent, Message } from "../types";
import { StatusBadge } from "./StatusBadge";
import { formatTimestamp, formatShape } from "../utils/status";
import { useApi } from "../hooks/useApi";

interface NodeDetailProps {
  node: DagNode;
  onClose: () => void;
}

/** Slide-in detail panel shown when a DAG node is clicked. */
export function NodeDetail({ node, onClose }: NodeDetailProps) {
  const isActor = node.tier === "actor";

  return (
    <div className="dag-detail-panel" data-testid="dag-detail-panel">
      <div className="dag-detail-header">
        <div>
          <div className="dag-detail-title">{node.label}</div>
          <div className="dag-detail-subtitle">{node.subtitle}</div>
        </div>
        <button
          className="dag-detail-close"
          onClick={onClose}
          aria-label="Close detail panel"
        >
          &times;
        </button>
      </div>

      <div className="dag-detail-body">
        <div className="dag-detail-meta">
          <MetaRow label="ID" value={String(node.entityId)} />
          <MetaRow label="Type" value={node.tier.replace("_", " ")} />
          {node.meshClass && (
            <MetaRow label="Class" value={node.meshClass} />
          )}
          <div className="meta-row">
            <dt>Status</dt>
            <dd><StatusBadge status={node.status} /></dd>
          </div>
        </div>

        {isActor && <ActorDetails actorId={node.entityId} />}
        {!isActor && <MeshDetails meshId={node.entityId} />}
      </div>
    </div>
  );
}

function MetaRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="meta-row">
      <dt>{label}</dt>
      <dd className="mono-cell">{value}</dd>
    </div>
  );
}

/** Actor-specific details: status timeline + messages. */
function ActorDetails({ actorId }: { actorId: number }) {
  const { data: events } = useApi<ActorStatusEvent[]>(
    `/actors/${actorId}/status_events`
  );
  const { data: messages } = useApi<Message[]>(
    `/actors/${actorId}/messages`
  );

  const incoming = (messages ?? []).filter((m) => m.to_actor_id === actorId);
  const outgoing = (messages ?? []).filter((m) => m.from_actor_id === actorId);

  return (
    <>
      {/* Status timeline */}
      <div className="dag-detail-section">
        <h3 className="dag-detail-section-title">
          Status Timeline
          <span className="count-badge">{events?.length ?? 0}</span>
        </h3>
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

      {/* Message summary */}
      <div className="dag-detail-section">
        <h3 className="dag-detail-section-title">
          Messages
          <span className="count-badge">
            {incoming.length} in / {outgoing.length} out
          </span>
        </h3>
        {incoming.length === 0 && outgoing.length === 0 ? (
          <div className="empty-state">No messages</div>
        ) : (
          <div className="table-wrapper table-compact">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Dir</th>
                  <th>Actor</th>
                  <th>Endpoint</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {incoming.slice(0, 10).map((m) => (
                  <tr key={`in-${m.id}`}>
                    <td style={{ color: "var(--status-processing)" }}>&larr; in</td>
                    <td>#{m.from_actor_id}</td>
                    <td><span className="endpoint-tag">{m.endpoint ?? "\u2014"}</span></td>
                    <td>{m.status}</td>
                  </tr>
                ))}
                {outgoing.slice(0, 10).map((m) => (
                  <tr key={`out-${m.id}`}>
                    <td style={{ color: "var(--accent-secondary)" }}>&rarr; out</td>
                    <td>#{m.to_actor_id}</td>
                    <td><span className="endpoint-tag">{m.endpoint ?? "\u2014"}</span></td>
                    <td>{m.status}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </>
  );
}

/** Mesh-specific details: shape, children count. */
function MeshDetails({ meshId }: { meshId: number }) {
  const { data: mesh } = useApi<any>(`/meshes/${meshId}`);
  const { data: children } = useApi<any[]>(`/meshes/${meshId}/children`);
  const { data: actors } = useApi<Actor[]>(`/actors?mesh_id=${meshId}`);

  return (
    <div className="dag-detail-section">
      <h3 className="dag-detail-section-title">Mesh Info</h3>
      <div className="dag-detail-meta">
        {mesh && (
          <>
            <MetaRow label="Full Name" value={mesh.full_name} />
            <MetaRow label="Shape" value={formatShape(mesh.shape_json)} />
          </>
        )}
        <MetaRow
          label="Children"
          value={`${children?.length ?? 0} meshes`}
        />
        <MetaRow
          label="Actors"
          value={`${actors?.length ?? 0} direct`}
        />
      </div>
    </div>
  );
}
