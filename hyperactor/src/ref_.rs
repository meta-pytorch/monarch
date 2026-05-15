/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Typed capability references for Hyperactor actors and ports.

use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;

use derivative::Derivative;
use hyperactor_config::Flattrs;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typeuri::Named;

use crate::Actor;
use crate::ActorAddr;
use crate::ActorHandle;
use crate::Endpoint;
use crate::EndpointLocation;
use crate::PortAddr;
use crate::RemoteEndpoint;
use crate::RemoteHandles;
use crate::RemoteMessage;
use crate::accum::ReducerSpec;
use crate::accum::StreamingReducerOpts;
use crate::actor::Referable;
use crate::context;
use crate::context::MailboxExt;
use crate::mailbox::LostMessage;
use crate::mailbox::MailboxSenderError;
use crate::mailbox::MailboxSenderErrorKind;
use crate::mailbox::PortSink;
use crate::message::Bind;
use crate::message::Bindings;
use crate::message::Unbind;
use crate::port::Port;

/// ActorRefs are typed references to actors.
#[derive(typeuri::Named)]
pub struct ActorRef<A: Referable> {
    pub(crate) actor_addr: ActorAddr,
    // fn() -> A so that the struct remains Send
    phantom: PhantomData<fn() -> A>,
}

impl<A: Referable> ActorRef<A> {
    /// Get the remote port for message type [`M`] for the referenced actor.
    pub fn port<M: RemoteMessage>(&self) -> PortRef<M>
    where
        A: RemoteHandles<M>,
    {
        PortRef::attest(self.actor_addr.port_addr(Port::from(<M as Named>::port())))
    }

    /// The caller guarantees that the provided actor ID is also a valid,
    /// typed reference.  This is usually invoked to provide a guarantee
    /// that an externally-provided actor ID (e.g., through a command
    /// line argument) is a valid reference.
    pub fn attest(actor_addr: ActorAddr) -> Self {
        Self {
            actor_addr,
            phantom: PhantomData,
        }
    }

    /// The actor address corresponding with this reference.
    pub fn actor_addr(&self) -> &ActorAddr {
        &self.actor_addr
    }

    /// Convert this actor reference into its corresponding actor address.
    pub fn into_actor_addr(self) -> ActorAddr {
        self.actor_addr
    }

    /// Attempt to downcast this reference into a (local) actor handle.
    /// This will only succeed when the referenced actor is in the same
    /// proc as the caller.
    pub fn downcast_handle(&self, cx: &impl context::Actor) -> Option<ActorHandle<A>>
    where
        A: Actor,
    {
        cx.instance().proc().resolve_actor_ref(self)
    }
}

impl<A, M> Endpoint<M> for &ActorRef<A>
where
    A: Referable + RemoteHandles<M>,
    M: RemoteMessage,
{
    fn endpoint_location(&self) -> EndpointLocation {
        EndpointLocation::Actor(self.actor_addr.clone())
    }

    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor,
    {
        RemoteEndpoint::post_with_headers(self, cx, Flattrs::new(), message)
    }
}

impl<A, M> RemoteEndpoint<M> for &ActorRef<A>
where
    A: Referable + RemoteHandles<M>,
    M: RemoteMessage,
{
    fn post_with_headers<C>(self, cx: &C, headers: Flattrs, message: M)
    where
        C: context::Actor,
    {
        RemoteEndpoint::post_with_headers(&self.port(), cx, headers, message)
    }
}

// Implement Serialize manually, without requiring A: Serialize
impl<A: Referable> Serialize for ActorRef<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize only the fields that don't depend on A
        self.actor_addr.serialize(serializer)
    }
}

// Implement Deserialize manually, without requiring A: Deserialize
impl<'de, A: Referable> Deserialize<'de> for ActorRef<A> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let actor_addr = <ActorAddr>::deserialize(deserializer)?;
        Ok(ActorRef {
            actor_addr,
            phantom: PhantomData,
        })
    }
}

// Implement Debug manually, without requiring A: Debug
impl<A: Referable> fmt::Debug for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("actor_addr", &self.actor_addr)
            .field("type", &std::any::type_name::<A>())
            .finish()
    }
}

impl<A: Referable> fmt::Display for ActorRef<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.actor_addr, f)?;
        write!(f, "<{}>", std::any::type_name::<A>())
    }
}

// We implement Clone manually to avoid imposing A: Clone.
impl<A: Referable> Clone for ActorRef<A> {
    fn clone(&self) -> Self {
        Self {
            actor_addr: self.actor_addr.clone(),
            phantom: PhantomData,
        }
    }
}

impl<A: Referable> PartialEq for ActorRef<A> {
    fn eq(&self, other: &Self) -> bool {
        self.actor_addr == other.actor_addr
    }
}

impl<A: Referable> Eq for ActorRef<A> {}

impl<A: Referable> PartialOrd for ActorRef<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Referable> Ord for ActorRef<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.actor_addr.cmp(&other.actor_addr)
    }
}

impl<A: Referable> Hash for ActorRef<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.actor_addr.hash(state);
    }
}

/// A reference to a remote port. All messages passed through
/// PortRefs will be serialized. PortRefs are always streaming.
#[derive(Debug, Serialize, Deserialize, Derivative, typeuri::Named)]
#[derivative(PartialEq, Eq, PartialOrd, Hash, Ord)]
pub struct PortRef<M> {
    port_addr: PortAddr,
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    reducer_spec: Option<ReducerSpec>,
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    streaming_opts: StreamingReducerOpts,
    phantom: PhantomData<M>,
    return_undeliverable: bool,
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    unsplit: bool,
}

impl<M: RemoteMessage> PortRef<M> {
    /// The caller attests that the provided port address identifies a
    /// reachable typed port for message type `M`.
    pub fn attest(port_addr: PortAddr) -> Self {
        Self {
            port_addr,
            reducer_spec: None,
            streaming_opts: StreamingReducerOpts::default(),
            phantom: PhantomData,
            return_undeliverable: true,
            unsplit: false,
        }
    }

    /// The caller attests that the provided port address identifies a
    /// reachable typed port for message type `M`.
    pub fn attest_reducible(
        port_addr: PortAddr,
        reducer_spec: Option<ReducerSpec>,
        streaming_opts: StreamingReducerOpts,
    ) -> Self {
        Self {
            port_addr,
            reducer_spec,
            streaming_opts,
            phantom: PhantomData,
            return_undeliverable: true,
            unsplit: false,
        }
    }

    /// Prevents the port from being split.
    pub fn unsplit(mut self) -> Self {
        self.unsplit = true;
        self
    }

    /// The caller attests that the provided actor exposes a reachable handler
    /// port for message type `M`.
    pub fn attest_handler_port(actor: &ActorAddr) -> Self {
        PortRef::<M>::attest(actor.port_addr(Port::from(<M as Named>::port())))
    }

    /// The typehash of this port's reducer, if any. Reducers
    /// may be used to coalesce messages sent to a port.
    pub fn reducer_spec(&self) -> &Option<ReducerSpec> {
        &self.reducer_spec
    }

    /// This port's address.
    pub fn port_addr(&self) -> &PortAddr {
        &self.port_addr
    }

    /// Convert this PortRef into its corresponding port address.
    pub fn into_port_addr(self) -> PortAddr {
        self.port_addr
    }

    /// coerce it into OncePortRef so we can send messages to this port from
    /// APIs requires OncePortRef.
    pub fn into_once(self) -> OncePortRef<M> {
        let return_undeliverable = self.return_undeliverable;
        let unsplit = self.unsplit;
        let mut once = OncePortRef::attest(self.into_port_addr());
        once.return_undeliverable = return_undeliverable;
        once.unsplit = unsplit;
        once
    }

    /// Post a serialized message to this port, provided a sending capability, such as
    /// [`crate::actor::Instance`].
    pub fn post_serialized(
        &self,
        cx: &impl context::Actor,
        mut headers: Flattrs,
        message: wirevalue::Any,
    ) {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        crate::mailbox::headers::set_rust_message_type::<M>(&mut headers);
        cx.post(
            self.port_addr.clone(),
            headers,
            message,
            self.return_undeliverable,
            context::SeqInfoPolicy::AssignNew,
        );
    }

    /// Convert this port into a sink that can be used to send messages using the given capability.
    pub fn into_sink<C: context::Actor>(self, cx: C) -> PortSink<C, M> {
        PortSink::new(cx, self)
    }

    /// Get whether or not messages sent to this port that are undeliverable should
    /// be returned to the sender.
    pub fn get_return_undeliverable(&self) -> bool {
        self.return_undeliverable
    }

    /// Set whether or not messages sent to this port that are undeliverable
    /// should be returned to the sender.
    pub fn return_undeliverable(&mut self, return_undeliverable: bool) {
        self.return_undeliverable = return_undeliverable;
    }
}

impl<M> Endpoint<M> for &PortRef<M>
where
    M: RemoteMessage,
{
    fn endpoint_location(&self) -> EndpointLocation {
        EndpointLocation::Port(self.port_addr.clone())
    }

    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor,
    {
        RemoteEndpoint::post_with_headers(self, cx, Flattrs::new(), message)
    }
}

impl<M> RemoteEndpoint<M> for &PortRef<M>
where
    M: RemoteMessage,
{
    fn post_with_headers<C>(self, cx: &C, headers: Flattrs, message: M)
    where
        C: context::Actor,
    {
        let serialized = match wirevalue::Any::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                self.port_addr.clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        }) {
            Ok(serialized) => serialized,
            Err(err) => {
                cx.instance()
                    .report_lost_message(LostMessage::from_send_error::<M>(
                        cx.mailbox().actor_addr().clone(),
                        self.endpoint_location(),
                        &err,
                    ));
                return;
            }
        };
        self.post_serialized(cx, headers, serialized);
    }
}

impl<M: RemoteMessage> Clone for PortRef<M> {
    fn clone(&self) -> Self {
        Self {
            port_addr: self.port_addr.clone(),
            reducer_spec: self.reducer_spec.clone(),
            streaming_opts: self.streaming_opts.clone(),
            phantom: PhantomData,
            return_undeliverable: self.return_undeliverable,
            unsplit: self.unsplit,
        }
    }
}

impl<M: RemoteMessage> fmt::Display for PortRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.port_addr, f)
    }
}

/// The kind of unbound port.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Named)]
pub enum UnboundPortKind {
    /// A streaming port, which should be reduced with the provided options.
    Streaming(Option<StreamingReducerOpts>),
    /// A OncePort, which must be one-shot aggregated.
    Once,
}

/// The parameters extracted from [`PortRef`] to [`Bindings`].
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, typeuri::Named)]
pub struct UnboundPort(
    pub PortAddr,
    pub Option<ReducerSpec>,
    pub bool, // return_undeliverable
    pub UnboundPortKind,
    pub bool, // unsplit
);
wirevalue::register_type!(UnboundPort);

impl UnboundPort {
    /// Update the port id of this binding.
    pub fn update(&mut self, port_addr: PortAddr) {
        self.0 = port_addr;
    }
}

impl<M: RemoteMessage> From<&PortRef<M>> for UnboundPort {
    fn from(port_ref: &PortRef<M>) -> Self {
        UnboundPort(
            port_ref.port_addr.clone(),
            port_ref.reducer_spec.clone(),
            port_ref.return_undeliverable,
            UnboundPortKind::Streaming(Some(port_ref.streaming_opts.clone())),
            port_ref.unsplit,
        )
    }
}

impl<M: RemoteMessage> Unbind for PortRef<M> {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        bindings.push_back(&UnboundPort::from(self))
    }
}

impl<M: RemoteMessage> Bind for PortRef<M> {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        let UnboundPort(port_addr, reducer_spec, return_undeliverable, port_kind, unsplit) =
            bindings.try_pop_front::<UnboundPort>()?;
        self.port_addr = port_addr;
        self.reducer_spec = reducer_spec;
        self.return_undeliverable = return_undeliverable;
        self.unsplit = unsplit;
        self.streaming_opts = match port_kind {
            UnboundPortKind::Streaming(opts) => opts.unwrap_or_default(),
            UnboundPortKind::Once => {
                anyhow::bail!("OncePortRef cannot be bound to PortRef")
            }
        };
        Ok(())
    }
}

/// A remote reference to a [`OncePort`]. References are serializable
/// and may be passed to remote actors, which can then use it to send
/// a message to this port.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct OncePortRef<M> {
    port_addr: PortAddr,
    reducer_spec: Option<ReducerSpec>,
    return_undeliverable: bool,
    unsplit: bool,
    phantom: PhantomData<M>,
}

impl<M: RemoteMessage> OncePortRef<M> {
    pub(crate) fn attest(port_addr: PortAddr) -> Self {
        Self {
            port_addr,
            reducer_spec: None,
            return_undeliverable: true,
            unsplit: false,
            phantom: PhantomData,
        }
    }

    /// The caller attests that the provided PortId can be
    /// converted to a reachable, typed once port reference.
    pub fn attest_reducible(port_addr: PortAddr, reducer_spec: Option<ReducerSpec>) -> Self {
        Self {
            port_addr,
            reducer_spec,
            return_undeliverable: true,
            unsplit: false,
            phantom: PhantomData,
        }
    }

    /// Prevents the port from being split.
    pub fn unsplit(mut self) -> Self {
        self.unsplit = true;
        self
    }

    /// The typehash of this port's reducer, if any.
    pub fn reducer_spec(&self) -> &Option<ReducerSpec> {
        &self.reducer_spec
    }

    /// This port's address.
    pub fn port_addr(&self) -> &PortAddr {
        &self.port_addr
    }

    /// Convert this OncePortRef into its corresponding port address.
    pub fn into_port_addr(self) -> PortAddr {
        self.port_addr
    }

    /// Get whether or not messages sent to this port that are undeliverable should
    /// be returned to the sender.
    pub fn get_return_undeliverable(&self) -> bool {
        self.return_undeliverable
    }

    /// Set whether or not messages sent to this port that are undeliverable
    /// should be returned to the sender.
    pub fn return_undeliverable(&mut self, return_undeliverable: bool) {
        self.return_undeliverable = return_undeliverable;
    }
}

impl<M> Endpoint<M> for OncePortRef<M>
where
    M: RemoteMessage,
{
    fn endpoint_location(&self) -> EndpointLocation {
        EndpointLocation::Port(self.port_addr.clone())
    }

    fn post<C>(self, cx: &C, message: M)
    where
        C: context::Actor,
    {
        RemoteEndpoint::post_with_headers(self, cx, Flattrs::new(), message)
    }
}

impl<M> RemoteEndpoint<M> for OncePortRef<M>
where
    M: RemoteMessage,
{
    fn post_with_headers<C>(self, cx: &C, mut headers: Flattrs, message: M)
    where
        C: context::Actor,
    {
        crate::mailbox::headers::set_send_timestamp(&mut headers);
        let serialized = match wirevalue::Any::serialize(&message).map_err(|err| {
            MailboxSenderError::new_bound(
                self.port_addr.clone(),
                MailboxSenderErrorKind::Serialize(err.into()),
            )
        }) {
            Ok(serialized) => serialized,
            Err(err) => {
                cx.instance()
                    .report_lost_message(LostMessage::from_send_error::<M>(
                        cx.mailbox().actor_addr().clone(),
                        self.endpoint_location(),
                        &err,
                    ));
                return;
            }
        };
        cx.post(
            self.port_addr.clone(),
            headers,
            serialized,
            self.return_undeliverable,
            context::SeqInfoPolicy::AssignNew,
        );
    }
}

impl<M: RemoteMessage> Clone for OncePortRef<M> {
    fn clone(&self) -> Self {
        Self {
            port_addr: self.port_addr.clone(),
            reducer_spec: self.reducer_spec.clone(),
            return_undeliverable: self.return_undeliverable,
            unsplit: self.unsplit,
            phantom: PhantomData,
        }
    }
}

impl<M: RemoteMessage> fmt::Display for OncePortRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.port_addr, f)
    }
}

impl<M: RemoteMessage> Named for OncePortRef<M> {
    fn typename() -> &'static str {
        wirevalue::intern_typename!(Self, "hyperactor::mailbox::OncePortRef<{}>", M)
    }
}

impl<M: RemoteMessage> From<&OncePortRef<M>> for UnboundPort {
    fn from(port_ref: &OncePortRef<M>) -> Self {
        UnboundPort(
            port_ref.port_addr.clone(),
            port_ref.reducer_spec.clone(),
            true, // return_undeliverable
            UnboundPortKind::Once,
            port_ref.unsplit,
        )
    }
}

impl<M: RemoteMessage> Unbind for OncePortRef<M> {
    fn unbind(&self, bindings: &mut Bindings) -> anyhow::Result<()> {
        bindings.push_back(&UnboundPort::from(self))
    }
}

impl<M: RemoteMessage> Bind for OncePortRef<M> {
    fn bind(&mut self, bindings: &mut Bindings) -> anyhow::Result<()> {
        let UnboundPort(port_addr, reducer_spec, _return_undeliverable, port_kind, unsplit) =
            bindings.try_pop_front::<UnboundPort>()?;
        match port_kind {
            UnboundPortKind::Once => {
                self.port_addr = port_addr;
                self.reducer_spec = reducer_spec;
                self.unsplit = unsplit;
                Ok(())
            }
            UnboundPortKind::Streaming(_) => {
                anyhow::bail!("PortRef cannot be bound to OncePortRef")
            }
        }
    }
}
