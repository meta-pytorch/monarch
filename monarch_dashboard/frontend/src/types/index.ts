/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/** Data contract types matching the Monarch Dashboard SQLite schema. */

export interface Mesh {
  id: number;
  timestamp_us: number;
  class: string;
  given_name: string;
  full_name: string;
  shape_json: string;
  parent_mesh_id: number | null;
  parent_view_json: string | null;
}

export interface Actor {
  id: number;
  timestamp_us: number;
  mesh_id: number;
  rank: number;
  full_name: string;
  latest_status?: string | null;
  latest_status_timestamp_us?: number | null;
}

export interface ActorStatusEvent {
  id: number;
  timestamp_us: number;
  actor_id: number;
  new_status: string;
  reason: string | null;
}

export interface Message {
  id: number;
  timestamp_us: number;
  from_actor_id: number;
  to_actor_id: number;
  status: string;
  endpoint: string | null;
  port_id: number | null;
}

export interface MessageStatusEvent {
  id: number;
  timestamp_us: number;
  message_id: number;
  status: string;
}

export interface SentMessage {
  id: number;
  timestamp_us: number;
  sender_actor_id: number;
  actor_mesh_id: number;
  view_json: string;
  shape_json: string;
}

/** Navigation breadcrumb item. */
export interface NavItem {
  label: string;
  level: "hosts" | "procs" | "actor_meshes" | "actors" | "actor_detail";
  meshId?: number;
  actorId?: number;
}

/** Aggregate summary returned by GET /api/summary. */
export interface Summary {
  mesh_counts: {
    total: number;
    by_class: Record<string, number>;
  };
  actor_counts: {
    total: number;
    by_status: Record<string, number>;
  };
  message_counts: {
    total: number;
    by_status: Record<string, number>;
    by_endpoint: Record<string, number>;
    delivery_rate: number;
  };
  errors: {
    failed_actors: Array<{
      actor_id: number;
      full_name: string;
      reason: string | null;
      timestamp_us: number;
      mesh_id: number;
    }>;
    stopped_actors: Array<{
      actor_id: number;
      full_name: string;
      reason: string | null;
      timestamp_us: number;
      mesh_id: number;
    }>;
    failed_messages: number;
  };
  timeline: {
    start_us: number;
    end_us: number;
    failure_onset_us: number | null;
    total_status_events: number;
    total_message_events: number;
  };
  health_score: number;
}
