/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Remote supervision protocol and adapters for Hyperactor.

#![deny(missing_docs)]

pub mod keepalive;
pub mod link;
pub mod proto;
pub mod supervision;
pub mod token;

pub use keepalive::Keepalive;
pub use keepalive::KeepaliveAck;
pub use keepalive::KeepaliveLink;
pub use keepalive::KeepaliveParams;
pub use keepalive::KeepaliveSupervisor;
pub use keepalive::KeepaliveSupervisorParams;
pub use keepalive::KeepaliveWorker;
pub use keepalive::KeepaliveWorkerParams;
pub use link::LinkSpec;
pub use proto::Link;
pub use proto::LinkOptions;
pub use proto::OrphanPolicy;
pub use proto::RemoteActorDisposition;
pub use proto::SupervisedWorker;
pub use proto::WorkerSupervisor;
pub use supervision::Supervisor;
pub use supervision::Worker;
pub use supervision::WorkerLike;
pub use token::Join;
pub use token::JoinResult;
pub use token::Joined;
pub use token::Options as TokenOptions;
pub use token::Policy as TokenPolicy;
pub use token::RendezvousLike;
pub use token::Token;
pub use token::TokenPeer;
pub use token::create;
