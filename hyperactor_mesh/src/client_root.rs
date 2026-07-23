/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! A native, language-independent client-root capability.
//!
//! A Hyperactor mesh program has exactly one root client. Rust and Python are
//! alternative bootstrap implementations of that one root, not separate root
//! namespaces; worker ProcAgents host actors but never become client roots.
//!
//! [`ClientRootRef`] is an opaque bearer capability to the root, carried in an
//! actor's [`ActorEnvironment`](hyperactor::ActorEnvironment) and inherited by
//! descendants. Its sole authority is to create or reuse a statically declared,
//! root-owned service through the root ProcAgent's [`EnsureClientRootService`]
//! handler.
//!
//! # CROOT invariant registry
//!
//! - **CROOT-1 (singleton binding):** the active client bootstrap binds
//!   [`ClientRootApi`] on the program's one root ProcAgent and stores that exact
//!   reference; worker ProcAgents never bind it.
//! - **CROOT-2 (typed API attenuation):** [`ClientRootRef`] exposes only typed
//!   ensure-service authority and no root administration.
//! - **CROOT-3 (single identity):** exact name/type/parameter equality returns
//!   one cached spawn result; a type or parameter-byte difference for the same
//!   name conflicts. The service actor is created under a fresh random instance
//!   id, which avoids the predictable collision a name-derived id would create;
//!   the create path re-draws the id if it collides with one already present in
//!   `actor_states`.
//! - **CROOT-4 (worker rejection):** a worker ProcAgent has no bound client-root
//!   handler, so a retyped worker reference reaches an unbound port and cannot
//!   create a service.
//! - **CROOT-5 (root ownership):** service lifetime belongs to the root
//!   ProcAgent, never the requester; a reused reference to a service that has
//!   reached a terminal state fails closed rather than returning a dead actor.
//! - **CROOT-6 (validation):** a capability absent from the actor environment
//!   fails closed — [`ClientRootRef::from_env`] returns an error rather than a
//!   silent `None`, before any request is sent.
//! - **CROOT-7 (continuity):** each created service receives the same
//!   [`ClientRootRef`] in its native environment.
//! - **CROOT-8 (runtime neutrality):** Rust and Python roots store the same
//!   native capability under the same environment key.
//! - **CROOT-9 (map layering):** a service's spawn result and lifecycle live
//!   solely in the ProcAgent `actor_states` registry; the service-identity table
//!   records only the (actor type, exact parameter bytes, instance id) a repeat
//!   ensure must match. Only ensure writes that table, and only alongside the
//!   `actor_states` entry, so the two never disagree.
//! - **CROOT-10 (attestation boundary):** the only explicit attestation restores
//!   `ActorRef<A>` immediately after the erased spawn reply whose requested and
//!   recorded actor type was derived from that same `A`; every other reference is
//!   bound or deserialized.
//! - **CROOT-11 (handler resilience):** the `EnsureClientRootService` handler
//!   disables undeliverable-return on its reply and always returns `Ok(())`,
//!   reporting every expected failure through the typed reply; a malformed,
//!   conflicting, or reply-undeliverable request never faults the root ProcAgent.
//! - **CROOT-12 (crate layering):** every client-root type and policy stays in
//!   `hyperactor_mesh` (or a higher composition crate); `hyperactor` remains a
//!   generic attribute carrier with no static mesh dependency.

use std::marker::PhantomData;

use hyperactor::ActorAddr;
use hyperactor::ActorEnvironment;
use hyperactor::ActorHandle;
use hyperactor::ActorRef;
use hyperactor::Data;
use hyperactor::Endpoint;
use hyperactor::OncePortRef;
use hyperactor::RemoteSpawn;
use hyperactor::actor::remote::Remote;
use hyperactor::context;
use hyperactor::id::Label;
use hyperactor_config::AttrValue;
use hyperactor_config::declare_attrs;
use serde::Deserialize;
use serde::Serialize;
use typeuri::Named;

use crate::CodecError;
use crate::Error;
use crate::proc_agent::ProcAgent;

/// A typed failure from a client-root ensure request.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum ClientRootError {
    /// The same service name was already created with a different actor type or
    /// different serialized parameters.
    #[error("conflicting client-root service specification for `{name}`")]
    ConflictingSpec {
        /// The service name that conflicted.
        name: String,
    },

    /// The service actor failed to spawn (the failure is cached and not retried
    /// implicitly).
    #[error("client-root service `{name}` failed to spawn: {message}")]
    Spawn {
        /// The service name.
        name: String,
        /// The spawn failure, rendered as text (the underlying error is not
        /// serializable).
        message: String,
    },

    /// The service exists but is not usable: it has stopped, failed, or is
    /// stopping after spawning, or the root is shutting down. The caller must not
    /// treat it as a live actor.
    #[error("client-root service `{name}` is not available: {reason}")]
    Unavailable {
        /// The service name.
        name: String,
        /// Why the service is unavailable.
        reason: String,
    },
}

/// Ensure a statically declared, root-owned service exists, returning its
/// address. Posted to the root ProcAgent through [`ClientRootApi`].
#[derive(Debug, Serialize, Deserialize, Named)]
pub(crate) struct EnsureClientRootService {
    /// The validated static service name.
    pub(crate) service_name: Label,
    /// The registered remote actor type name resolved from the descriptor.
    pub(crate) actor_type: String,
    /// The exact serialized `RemoteSpawn` parameters.
    pub(crate) params: Data,
    /// Typed reply channel.
    pub(crate) reply: OncePortRef<EnsureClientRootServiceReply>,
}

/// The typed reply to [`EnsureClientRootService`]. A `Named` newtype so it can
/// travel through a `OncePortRef`.
#[derive(Debug, Serialize, Deserialize, Named)]
pub(crate) struct EnsureClientRootServiceReply(pub(crate) Result<ActorAddr, ClientRootError>);

// The narrow, restricted behavior a `ClientRootRef` can drive on the root
// ProcAgent: exactly one message, no root administration. The behavior lives in
// a private module because the macro emits a `pub` type; only a crate-visible
// alias is re-exported, so the protocol never leaks past the crate.
mod protocol {
    use super::EnsureClientRootService;

    hyperactor::behavior!(ClientRootApi, EnsureClientRootService);
}
pub(crate) use protocol::ClientRootApi;

/// An opaque bearer capability to the program's one client root.
///
/// The inner behavior reference is never exposed. Possession of an inherited
/// `ClientRootRef` is the authority under Hyperactor's trusted-actor model
/// (CROOT-2); it is API attenuation, not actor authentication.
#[derive(Clone, Serialize, Deserialize, Named, PartialEq, Eq, Hash)]
pub struct ClientRootRef {
    root: ActorRef<ClientRootApi>,
}

impl ClientRootRef {
    /// Bind [`ClientRootApi`] on the program's root ProcAgent handle and wrap
    /// the resulting reference (CROOT-1). Only the active root bootstrap calls
    /// this; worker ProcAgents never bind the API.
    pub fn bind(root: &ActorHandle<ProcAgent>) -> Self {
        Self {
            root: root.bind::<ClientRootApi>(),
        }
    }

    /// Read the client-root capability from the supplied actor environment,
    /// failing closed when it is absent (CROOT-6). Every consumer (starting
    /// with RDMA) obtains the root here rather than re-deriving it, so the
    /// missing-capability path is enforced in one place.
    pub fn from_env(environment: &ActorEnvironment) -> crate::Result<ClientRootRef> {
        environment.get(CLIENT_ROOT).ok_or(Error::MissingClientRoot)
    }

    /// Construct a reference from an already-bound behavior reference. Used by
    /// the root ProcAgent to seed a created service's environment with the same
    /// capability (CROOT-7).
    pub(crate) fn from_ref(root: ActorRef<ClientRootApi>) -> Self {
        Self { root }
    }

    /// The root behavior address, for typed-error attribution only.
    pub(crate) fn addr(&self) -> &ActorAddr {
        self.root.actor_addr()
    }
}

// A bearer capability must not be copied out of config or diagnostics as a
// reconstructible string. `AttrValue` imposes no display/parse round-trip law,
// so display/`Debug` are redacted and textual parse always fails; serde still
// carries the real bytes, which is how `ActorEnvironment` stores and retrieves
// the value.
impl std::fmt::Debug for ClientRootRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClientRootRef(<redacted>)")
    }
}

impl AttrValue for ClientRootRef {
    fn display(&self) -> String {
        "<redacted>".to_string()
    }

    fn parse(_value: &str) -> Result<Self, anyhow::Error> {
        anyhow::bail!("ClientRootRef is a bearer capability and cannot be parsed from text")
    }
}

declare_attrs! {
    /// The client-root capability carried in an actor's environment.
    pub attr CLIENT_ROOT: ClientRootRef;
}

/// A statically declared, root-owned service: a fixed service name plus a
/// registered actor type `A`. Callers supply `A::Params`; there is no
/// caller-supplied runtime actor type or untyped address.
pub struct ClientRootService<A: RemoteSpawn> {
    name: &'static str,
    _actor: PhantomData<fn() -> A>,
}

impl<A: RemoteSpawn> ClientRootService<A> {
    /// Declare a statically named root-owned service.
    ///
    /// This constructs only the local typed descriptor. [`Self::ensure`] is
    /// the operation that asks the root ProcAgent to create or reuse the actor.
    pub const fn declare(name: &'static str) -> Self {
        Self {
            name,
            _actor: PhantomData,
        }
    }

    /// Ensure this service exists under `client_root`, returning a typed
    /// reference to it. Concurrent identical ensures collapse on the serial root
    /// ProcAgent; a different actor type or parameter bytes for the same name
    /// returns [`ClientRootError::ConflictingSpec`].
    pub async fn ensure(
        &self,
        cx: &impl context::Actor,
        client_root: &ClientRootRef,
        params: A::Params,
    ) -> crate::Result<ActorRef<A>> {
        let service_name = Label::new(self.name).map_err(|e| Error::Other(e.into()))?;
        let actor_type = Remote::collect()
            .name_of::<A>()
            .ok_or_else(|| Error::ActorTypeNotRegistered(std::any::type_name::<A>().to_string()))?
            .to_string();
        let params = bincode::serde::encode_to_vec(&params, bincode::config::legacy())
            .map_err(|e| Error::CodecError(CodecError::BincodeEncodeError(Box::new(e))))?;

        let (reply_handle, reply_receiver) = cx
            .mailbox()
            .open_once_port::<EnsureClientRootServiceReply>();
        client_root.root.post(
            cx,
            EnsureClientRootService {
                service_name,
                actor_type,
                params,
                reply: reply_handle.bind(),
            },
        );
        // A failed reply delivery is a genuine call failure against the root; a
        // typed `ClientRootError` carried in the reply surfaces transparently.
        let EnsureClientRootServiceReply(result) = reply_receiver
            .recv()
            .await
            .map_err(|e| Error::CallError(client_root.addr().clone(), e.into()))?;
        let addr = result?;

        // CROOT-10: `RemoteSpawn` erased `A` to `ActorAddr` after binding the
        // actor. This request's registered actor type came from `A`, and the
        // root ProcAgent returned only the matching service entry, so restore
        // that discarded phantom type here. This is the module's sole `attest`.
        Ok(ActorRef::<A>::attest(addr))
    }
}
