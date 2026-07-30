/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

/**
 * Data-contract types matching the Monarch Dashboard API exactly.
 * IDs are 64-bit ints the server serializes as strings for JS safety.
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
  /** Present on list_actors-joined rows, not base mesh rows. */
  mesh_class?: string | null;
  mesh_name?: string | null;
}

/** An actor (user actors + system agents like HostAgent, ProcAgent). */
export interface Actor {
  id: EntityId;
  timestamp_us: number;
  mesh_id: EntityId;
  rank: number;
  full_name: string;
  display_name?: string | null;
  mesh_class?: string | null;
  mesh_name?: string | null;
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
  port_index: EntityId | null;
  latest_status?: string | null;
}

export interface MessageStatusEvent {
  id: EntityId;
  timestamp_us: number;
  message_id: EntityId;
  status: string;
}

/** Aggregate summary from GET /api/summary. */
export interface Summary {
  mesh_counts: { total: number };
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
    failed_actors: ErrorActor[];
    stopped_actors: ErrorActor[];
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

/** Message-throughput histogram from GET /api/message-activity. */
export interface MessageActivity {
  start_us: number;
  end_us: number;
  total: number;
  /** Fixed-size throughput histogram (counts per equal time bucket). */
  buckets: number[];
}

export interface ErrorActor {
  actor_id: EntityId;
  full_name: string;
  reason: string | null;
  timestamp_us: number;
  mesh_id: EntityId;
}

/* ---------------------------- DAG contract ---------------------------- */

/**
 * Node tiers. host / proc / actor come from the admin snapshot DAG;
 * host_mesh / proc_mesh / actor_mesh are synthetic nodes produced by the
 * mesh-view collapse (collapseToMeshes). host_unit / proc_unit are legacy
 * tiers from the removed telemetry-SQL fallback and are no longer emitted.
 */
export type DagTier =
  | "host_mesh"
  | "host_unit"
  | "proc_mesh"
  | "proc_unit"
  | "actor_mesh"
  | "actor"
  | "host"
  | "proc";

export interface ApiDagNode {
  id: string;
  entity_id: number | string;
  tier: DagTier;
  label: string;
  subtitle: string;
  status: string;
  rank?: number;
  telemetry_actor_id?: number | string;
  mesh_name?: string;
  /** Synthetic mesh-view nodes only: number of underlying units/actors. */
  memberCount?: number;
}

export interface ApiDagEdge {
  id: string;
  source_id: string;
  target_id: string;
  type: "hierarchy" | "message";
}

export interface ApiDagData {
  nodes: ApiDagNode[];
  edges: ApiDagEdge[];
  /** True when no snapshot has been captured yet (cold start); nodes/edges are empty. */
  snapshot_pending?: boolean;
}
