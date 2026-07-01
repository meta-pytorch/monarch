/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Remote supervision protocol and adapters for Hyperactor.

#![deny(missing_docs)]

pub mod actor_spawner;
pub mod config;
pub mod keepalive;
pub mod link;
pub mod proc_spawner;
pub mod proto;
pub mod supervision;
pub mod token;

pub use actor_spawner::ActorSpawner;
pub use actor_spawner::ActorSpawnerEndpoint;
pub use keepalive::Keepalive;
pub use keepalive::KeepaliveAck;
pub use keepalive::KeepaliveLink;
pub use keepalive::KeepaliveParams;
pub use keepalive::KeepaliveSupervisor;
pub use keepalive::KeepaliveSupervisorParams;
pub use keepalive::KeepaliveWorker;
pub use keepalive::KeepaliveWorkerParams;
pub use link::LinkSpec;
pub use proc_spawner::ProcSpawner;
pub use proc_spawner::ProcSpawnerEndpoint;
pub use proto::Gspawn;
pub use proto::OrphanPolicy;
pub use proto::RemoteActorDisposition;
pub use proto::SpawnActor;
pub use proto::SpawnProc;
pub use proto::Supervise;
pub use proto::SupervisionOptions;
pub use proto::SupervisorEvent;
pub use proto::WorkerCommand;
pub use proto::WorkerLike;
pub use supervision::Spawn;
pub use supervision::Supervisor;
pub use supervision::Worker;
pub use token::Join;
pub use token::JoinResult;
pub use token::Joined;
pub use token::Options as TokenOptions;
pub use token::Policy as TokenPolicy;
pub use token::RendezvousLike;
pub use token::Token;
pub use token::TokenPeer;
pub use token::create;

#[cfg(test)]
mod token_supervision_tests;
