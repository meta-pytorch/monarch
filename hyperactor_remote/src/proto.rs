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
//! initiates the relationship by sending [`Supervise`] to the worker. The
//! `supervisor` port in [`Supervise`] identifies the session endpoint that the
//! worker uses for all replies and supervision events. The `liveness` spec in
//! [`Supervise`] identifies the implementation actor that the worker spawns as
//! the session liveness mechanism. Later supervisor commands are sent to the
//! worker as [`WorkerCommand`] messages and carry the same `session_id`; the
//! worker accepts only the active session.

use std::marker::PhantomData;

use hyperactor::ActorAddr;
use hyperactor::ActorEnvironment;
use hyperactor::Data;
use hyperactor::HandleClient;
use hyperactor::Handler;
use hyperactor::PortRef;
use hyperactor::RefClient;
use hyperactor::RemoteSpawn;
use hyperactor::actor::StopMode;
use hyperactor::context;
use hyperactor::id::Uid;
use hyperactor::supervision::ActorSupervisionEvent;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::link::LinkSpec;

/// Global actor spawn (gspawn) specification.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub struct Gspawn {
    actor_type: String,
    uid: Uid,
    // Encoded as a byte string rather than a sequence of `u8`s. This is
    // the same bincode encoding, but avoids per-byte serde calls.
    #[serde(with = "serde_bytes")]
    params: Data,
    environment: ActorEnvironment,
}
wirevalue::register_type!(Gspawn);

/// Spawn parameters for actor type `A`, serialized once so that many spawn
/// requests can reuse them without re-encoding.
pub struct SerializedParams<A> {
    data: Data,
    _actor: PhantomData<fn() -> A>,
}

impl<A: RemoteSpawn> SerializedParams<A> {
    /// Serialize `params` for spawning actors of type `A`.
    pub fn new(params: &A::Params) -> anyhow::Result<Self> {
        Ok(Self {
            data: bincode::serde::encode_to_vec(params, bincode::config::legacy())?,
            _actor: PhantomData,
        })
    }
}

impl<A> Clone for SerializedParams<A> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _actor: PhantomData,
        }
    }
}

impl Gspawn {
    /// Create a spawn specification for the registered actor type with a fresh
    /// uid and the caller's persistent environment.
    pub fn for_actor<A>(cx: &impl context::Actor, params: A::Params) -> anyhow::Result<Self>
    where
        A: RemoteSpawn + Named,
    {
        Self::for_actor_uid::<A>(cx, Uid::anonymous(), params)
    }

    /// Create a spawn specification for the registered actor type with an
    /// explicit uid and the caller's persistent environment.
    pub fn for_actor_uid<A>(
        cx: &impl context::Actor,
        uid: Uid,
        params: A::Params,
    ) -> anyhow::Result<Self>
    where
        A: RemoteSpawn + Named,
    {
        Ok(Self::for_actor_uid_in_environment::<A>(
            uid,
            SerializedParams::new(&params)?,
            cx.instance().actor_environment().clone(),
        ))
    }

    /// Create a spawn specification for the registered actor type with an
    /// explicit uid and a caller-shaped persistent environment (AENV-4).
    pub(crate) fn for_actor_uid_in_environment<A>(
        uid: Uid,
        params: SerializedParams<A>,
        environment: ActorEnvironment,
    ) -> Self
    where
        A: RemoteSpawn + Named,
    {
        Self::with_uid(A::typename(), uid, params.data, environment)
    }

    pub(crate) fn with_uid(
        actor_type: impl Into<String>,
        uid: Uid,
        params: Data,
        environment: ActorEnvironment,
    ) -> Self {
        Self {
            actor_type: actor_type.into(),
            uid,
            params,
            environment,
        }
    }

    /// Registered name of the actor to spawn.
    pub fn actor_type(&self) -> &str {
        &self.actor_type
    }

    /// The uid that will identify the spawned actor.
    pub fn uid(&self) -> &Uid {
        &self.uid
    }

    /// The serialized parameters passed to the actor.
    pub fn params(&self) -> &[u8] {
        &self.params
    }

    /// The persistent environment to install on the spawned actor.
    pub fn actor_environment(&self) -> &ActorEnvironment {
        &self.environment
    }

    /// Spawn the actor as a supervised child of `parent`.
    pub async fn spawn_child<C: hyperactor::context::Actor>(
        self,
        parent: &C,
    ) -> anyhow::Result<hyperactor::AnyActorHandle> {
        let Self {
            actor_type,
            uid,
            params,
            environment,
        } = self;
        parent
            .instance()
            .gspawn_uid_in_environment(&actor_type, uid, params, environment)
            .await
    }
}

/// Request sent to an actor spawner to spawn and supervise one registered actor.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Handler,
    HandleClient,
    RefClient
)]
pub struct SpawnActor {
    /// Registered actor spawn specification.
    pub gspawn: Gspawn,
    /// Supervise request to send back.
    pub supervise: Supervise,
}
wirevalue::register_type!(SpawnActor);

/// Request sent to a proc-spawner endpoint to spawn and supervise one proc.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Handler,
    HandleClient,
    RefClient
)]
pub struct SpawnProc {
    /// Proc uid to spawn.
    pub uid: Uid,
    /// Supervise request to send back.
    pub supervise: Supervise,
}
wirevalue::register_type!(SpawnProc);

/// Request sent by a supervisor proxy to a worker actor.
#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    Named,
    PartialEq,
    Eq,
    Handler,
    HandleClient,
    RefClient
)]
pub struct Supervise {
    /// Unique identifier for this supervision session.
    pub session_id: Uid,
    /// Supervisor session port used by the worker to report outcomes.
    pub supervisor: PortRef<SupervisorEvent>,
    /// The actor that owns the supervisor proxy.
    pub parent: ActorAddr,
    /// Liveness implementation actor to spawn on the worker side.
    pub liveness: LinkSpec,
    /// Session policy requested by the supervisor.
    pub options: SupervisionOptions,
    /// Labels the worker sets on every supervision event it reports in this
    /// session. They are opaque to remote supervision.
    pub labels: Flattrs,
}
wirevalue::register_type!(Supervise);

/// Options that govern a remote supervision session.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub struct SupervisionOptions {
    /// Policy to apply when the supervisor session is unlinked or expires.
    pub orphan_policy: OrphanPolicy,
}
wirevalue::register_type!(SupervisionOptions);

impl Default for SupervisionOptions {
    fn default() -> Self {
        Self {
            orphan_policy: OrphanPolicy::Stop,
        }
    }
}

/// Worker behavior when its supervisor session is orphaned.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
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
    Handler,
    HandleClient,
    RefClient
)]
pub enum WorkerCommand {
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
wirevalue::register_type!(WorkerCommand);

hyperactor::behavior!(WorkerLike, Supervise, WorkerCommand);

/// Messages sent by the worker actor to the supervisor session port.
#[derive(Clone, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub enum SupervisorEvent {
    /// The worker accepted the session and identified its supervised child.
    Linked {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Worker actor that owns the supervised child.
        worker: PortRef<WorkerCommand>,
        /// Actor address for the supervised child.
        child: ActorAddr,
        /// Friendly display name for the supervised child, if available.
        display_name: Option<String>,
    },
    /// The worker rejected the supervise request.
    SuperviseRejected {
        /// Unique identifier for this supervision session.
        session_id: Uid,
        /// Reason the supervise request failed.
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
wirevalue::register_type!(SupervisorEvent);

/// What a worker-side event establishes about the supervised child.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Named, PartialEq, Eq)]
pub enum RemoteActorDisposition {
    /// The supervised child reached a terminal local lifecycle state.
    Terminal,
    /// The child is unreachable from the supervisor side and may still be running.
    Unreachable,
}
wirevalue::register_type!(RemoteActorDisposition);

#[cfg(test)]
mod tests {
    use wirevalue::Encoding;

    use super::*;

    /// [`Gspawn`] as encoded before its params were marked as bytes.
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct SeqGspawn {
        actor_type: String,
        uid: Uid,
        params: Vec<u8>,
        environment: ActorEnvironment,
    }

    #[test]
    fn gspawn_params_wire_compatible() {
        let params: Vec<u8> = (0..=255).collect();
        let gspawn = Gspawn::with_uid(
            "test",
            Uid::anonymous(),
            params.clone(),
            ActorEnvironment::default(),
        );
        let seq = SeqGspawn {
            actor_type: gspawn.actor_type.clone(),
            uid: gspawn.uid.clone(),
            params,
            environment: gspawn.environment.clone(),
        };

        let bytes = bincode::serde::encode_to_vec(&gspawn, bincode::config::legacy()).unwrap();
        assert_eq!(
            bytes,
            bincode::serde::encode_to_vec(&seq, bincode::config::legacy()).unwrap()
        );
        let (decoded, _): (SeqGspawn, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::legacy()).unwrap();
        assert_eq!(decoded, seq);

        assert_eq!(
            serde_json::to_vec(&gspawn).unwrap(),
            serde_json::to_vec(&seq).unwrap()
        );

        for encoding in [Encoding::Bincode, Encoding::Json, Encoding::Multipart] {
            let old =
                wirevalue::Any::serialize_with_encoding_as::<Gspawn, _>(encoding, &seq).unwrap();
            assert_eq!(
                old,
                wirevalue::Any::serialize_with_encoding(encoding, &gspawn).unwrap(),
                "{encoding}"
            );
            assert_eq!(old.deserialized::<Gspawn>().unwrap(), gspawn, "{encoding}");
        }
    }
}
