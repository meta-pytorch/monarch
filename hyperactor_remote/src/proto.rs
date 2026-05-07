/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Message types for remote supervision sessions.
//!
//! A remote supervision relationship is represented by one worker actor
//! supervising one child actor on behalf of one supervisor. The supervisor
//! initiates the relationship by sending [`Link`] to the worker. The
//! `supervisor` port in [`Link`] identifies the session endpoint that the
//! worker uses for all replies and supervision events. The `link` spec in
//! [`Link`] identifies the implementation actor that the worker spawns as the
//! session liveness mechanism. Later supervisor commands are sent to the worker
//! as [`SupervisedWorker`] messages and carry the same `session_id`; the worker
//! accepts only the active session.

use hyperactor::ActorAddr;
use hyperactor::Bind;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::Unbind;
use hyperactor::actor::StopMode;
use hyperactor::id::Uid;
use hyperactor::supervision::ActorSupervisionEvent;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::link::LinkSpec;

/// Initial request sent by a supervisor proxy to a worker actor.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind,
    Handler,
    HandleClient,
    RefClient
)]
pub struct Link {
    /// Unique identifier for this supervision session.
    pub session_id: Uid,
    /// Supervisor session port used by the worker to report outcomes.
    #[binding(include)]
    pub supervisor: PortRef<WorkerSupervisor>,
    /// The actor that owns the supervisor proxy.
    pub parent: ActorAddr,
    /// Link implementation actor to spawn on the worker side.
    pub link: LinkSpec,
    /// Session policy requested by the supervisor.
    pub options: LinkOptions,
}
wirevalue::register_type!(Link);

/// Options that govern a remote supervision session.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub struct LinkOptions {
    /// Policy to apply when the supervisor session is unlinked or expires.
    pub orphan_policy: OrphanPolicy,
}
wirevalue::register_type!(LinkOptions);

impl Default for LinkOptions {
    fn default() -> Self {
        Self {
            orphan_policy: OrphanPolicy::Stop,
        }
    }
}

/// Worker behavior when its supervisor session is orphaned.
#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub enum OrphanPolicy {
    /// Stop the supervised child if the supervisor unlinks or expires.
    Stop,
    /// Leave the supervised child running after supervisor unlink.
    Detach,
}
wirevalue::register_type!(OrphanPolicy);

/// Commands sent by the supervisor proxy to the supervised worker actor.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind,
    Handler,
    HandleClient,
    RefClient
)]
pub enum SupervisedWorker {
    /// Stop the supervised child.
    Stop {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Stop mode to mirror from the parent side.
        mode: StopMode,
        /// Reason for stopping the supervised child.
        reason: String,
    },
    /// Unlink the supervisor session.
    Unlink {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Reason for unlinking the session.
        reason: String,
    },
}
wirevalue::register_type!(SupervisedWorker);

/// Messages sent by the worker actor to the supervisor session port.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub enum WorkerSupervisor {
    /// The worker accepted the session and identified its supervised child.
    Linked {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Actor address for the supervised child.
        child: ActorAddr,
        /// Friendly display name for the supervised child, if available.
        display_name: Option<String>,
    },
    /// The worker rejected the link request.
    LinkRejected {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Reason the link request failed.
        reason: String,
    },
    /// A supervision event observed by, or synthesized for, the worker.
    SupervisionEvent {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Event to deliver to the supervisor side.
        event: ActorSupervisionEvent,
        /// What the worker knows about the remote actor state.
        disposition: RemoteActorDisposition,
    },
    /// The worker unlinked the session without reporting a supervision event.
    Unlinked {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Reason for unlinking the session.
        reason: String,
    },
}
wirevalue::register_type!(WorkerSupervisor);

/// What a worker-side event establishes about the supervised child.
#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Bind,
    Unbind
)]
pub enum RemoteActorDisposition {
    /// The supervised child reached a terminal local lifecycle state.
    Terminal,
    /// The child is unreachable from the supervisor side and may still be running.
    Unreachable,
}
wirevalue::register_type!(RemoteActorDisposition);
