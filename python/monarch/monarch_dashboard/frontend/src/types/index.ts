/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/** Data contract types matching the Monarch Dashboard API. */

/**
 * Entity IDs are 64-bit integers which can exceed JavaScript's
 * Number.MAX_SAFE_INTEGER.  The server always serializes them as
 * strings for type consistency.
 */
export type EntityId = string;

/** A mesh in the hierarchy (Host, Proc, or actor mesh). */
export interface Mesh {
  id: EntityId;
  timestamp_us: number;
  class: string;
  given_name: string;
  full_name: string;
  shape_json: string;
  parent_mesh_id: EntityId | null;
  parent_view_json: string | null;
}

/** An actor (regular actors + system agents like HostAgent, ProcAgent). */
export interface Actor {
  id: EntityId;
  timestamp_us: number;
  mesh_id: EntityId;
  rank: number;
  full_name: string;
  display_name?: string | null;
  latest_status?: string | null;
  status_timestamp_us?: number | null;
}

export interface ActorStatusEvent {
  id: EntityId;
  timestamp_us: number;
  actor_id: EntityId;
  new_status: string;
  reason: string | null;
}

export interface Message {
  id: EntityId;
  timestamp_us: number;
  from_actor_id: EntityId;
  to_actor_id: EntityId;
  endpoint: string | null;
  port_id: EntityId | null;
  latest_status?: string | null;
}

export interface MessageStatusEvent {
  id: EntityId;
  timestamp_us: number;
  message_id: EntityId;
  status: string;
}

export interface SentMessage {
  id: EntityId;
  timestamp_us: number;
  sender_actor_id: EntityId;
  actor_mesh_id: EntityId;
  view_json: string;
  shape_json: string;
}

/** Navigation breadcrumb item. */
export interface NavItem {
  label: string;
  level:
    | "host_meshes"
    | "host_units"
    | "proc_meshes"
    | "proc_units"
    | "actor_meshes"
    | "actors"
    | "actor_detail";
  meshId?: EntityId;
  actorId?: EntityId;
}

/** Aggregate summary returned by GET /api/summary. */
export interface Summary {
  mesh_counts: {
    total: number;
  };
  hierarchy_counts: {
    host_meshes: number;
    proc_meshes: number;
    actor_meshes: number;
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
      actor_id: EntityId;
      full_name: string;
      reason: string | null;
      timestamp_us: number;
      mesh_id: EntityId;
    }>;
    stopped_actors: Array<{
      actor_id: EntityId;
      full_name: string;
      reason: string | null;
      timestamp_us: number;
      mesh_id: EntityId;
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
