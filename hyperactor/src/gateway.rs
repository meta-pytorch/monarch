/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Connectivity layer for Hyperactor procs.
//!
//! A proc by itself is an isolated actor runtime. It owns local actor
//! lifecycle and mailboxes, but it communicates with other procs by
//! attaching to a gateway. The gateway encapsulates the proc's connectivity
//! layer: it gives attached procs an advertised location, accepts inbound
//! traffic for that location, and forwards outbound traffic to destinations
//! outside the proc.
//!
//! This separation lets us compose different topologies without changing
//! proc identity. A host can attach all of its procs to one gateway, so the
//! gateway multiplexes ingress to those procs and routes egress on their
//! behalf. A proc from a foreign host can also attach through another host's
//! gateway, inheriting that host's advertised location while still retaining
//! its own proc id. Gateways can also act as pure proxies when they do not
//! own any local procs.
//!
//! From the channel/connectivity perspective, each location has one gateway.
//! Operationally, a gateway is both a proc multiplexer for ingress and a
//! router for egress.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;

use crate::Location;
use crate::ProcAddr;
use crate::ProcId;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::DeliveryError;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::IntoBoxedMailboxSender as _;
use crate::mailbox::MailboxSender as _;
use crate::mailbox::MailboxServer as _;
use crate::mailbox::MailboxServerHandle;
use crate::mailbox::MessageEnvelope;
use crate::mailbox::PortHandle;
use crate::mailbox::Undeliverable;
use crate::mailbox::UnroutableMailboxSender;
use crate::proc::Proc;
use crate::proc::WeakProc;

/// Connectivity boundary for one or more procs.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<GatewayState>,
}

struct GatewayState {
    /// The location to use when no server is active.
    fallback_location: Location,

    /// The location used when constructing routeable addresses for
    /// newly bound refs.
    default_location: RwLock<Location>,

    /// Sender used to forward messages outside of the proc.
    forwarder: BoxedMailboxSender,

    /// Procs attached to this gateway, keyed by runtime identity.
    procs: RwLock<HashMap<ProcId, WeakProc>>,

    /// Locations currently served by this gateway. The last location
    /// is the default advertised location.
    active_servers: RwLock<Vec<Location>>,
}

impl Gateway {
    /// Create a fresh unserved gateway with dial-based forwarding.
    pub fn new() -> Self {
        Self::configured(
            channel::reserve_local_addr().into(),
            DialMailboxRouter::new().into_boxed(),
        )
    }

    /// Create a fresh unserved local-only gateway.
    pub fn isolated() -> Self {
        Self::configured(
            channel::reserve_local_addr().into(),
            BoxedMailboxSender::new(UnroutableMailboxSender),
        )
    }

    /// Return the process-wide global gateway.
    pub fn global() -> &'static Self {
        static GLOBAL_GATEWAY: OnceLock<Gateway> = OnceLock::new();
        GLOBAL_GATEWAY.get_or_init(Self::new)
    }

    pub(crate) fn configured(default_location: Location, forwarder: BoxedMailboxSender) -> Self {
        Self {
            inner: Arc::new(GatewayState {
                fallback_location: default_location.clone(),
                default_location: RwLock::new(default_location),
                forwarder,
                procs: RwLock::new(HashMap::new()),
                active_servers: RwLock::new(Vec::new()),
            }),
        }
    }

    /// The gateway's default advertised location.
    pub fn default_location(&self) -> Location {
        self.inner.default_location.read().unwrap().clone()
    }

    /// Set the gateway's default advertised location.
    pub fn set_default_location(&self, location: Location) {
        *self.inner.default_location.write().unwrap() = location;
    }

    /// Construct a routeable proc address using this gateway's default location.
    pub fn proc_addr(&self, proc_id: &ProcId) -> ProcAddr {
        ProcAddr::new(proc_id.clone(), self.default_location())
    }

    pub(crate) fn forwarder(&self) -> &BoxedMailboxSender {
        &self.inner.forwarder
    }

    pub(crate) fn attach(&self, proc: &Proc) {
        let proc_id = proc.proc_id().clone();
        if let Some(existing) = self
            .inner
            .procs
            .write()
            .unwrap()
            .insert(proc_id.clone(), proc.downgrade())
            && existing.upgrade().is_some()
        {
            panic!("gateway already has a live proc with id {}", proc_id)
        }
    }

    pub(crate) fn serve_rx(
        &self,
        rx: impl channel::Rx<MessageEnvelope> + Send + 'static,
    ) -> MailboxServerHandle {
        WeakGateway::new(self).serve(rx)
    }

    /// Serve this gateway on the provided channel address.
    ///
    /// When serving [`ChannelAddr::any`] for the local transport, the gateway
    /// uses the local address that was reserved when the gateway was created.
    /// Serving also updates the gateway's default location to the served
    /// address.
    pub fn serve(&self, addr: ChannelAddr) -> Result<GatewayServeHandle, ChannelError> {
        let (location, handle) = self.serve_inner(addr)?;
        Ok(GatewayServeHandle {
            gateway: self.clone(),
            location,
            handle,
            stopped: Arc::new(AtomicBool::new(false)),
        })
    }

    fn serve_inner(
        &self,
        addr: ChannelAddr,
    ) -> Result<(Location, MailboxServerHandle), ChannelError> {
        let addr = self.resolve_serve_addr(addr);
        let (addr, rx) = channel::serve(addr)?;
        let location = Location::from(addr);
        self.add_server(location.clone());
        Ok((location, self.serve_rx(rx)))
    }

    fn resolve_serve_addr(&self, addr: ChannelAddr) -> ChannelAddr {
        if addr == ChannelAddr::any(ChannelTransport::Local)
            && self.inner.active_servers.read().unwrap().is_empty()
            && matches!(self.inner.fallback_location.addr(), ChannelAddr::Local(_))
        {
            return self.inner.fallback_location.addr().clone();
        }
        addr
    }

    fn add_server(&self, location: Location) {
        let mut active_servers = self.inner.active_servers.write().unwrap();
        active_servers.push(location.clone());
        *self.inner.default_location.write().unwrap() = location;
    }

    fn remove_server(&self, location: &Location) {
        let mut active_servers = self.inner.active_servers.write().unwrap();
        if let Some(index) = active_servers.iter().rposition(|active| active == location) {
            active_servers.remove(index);
        }
        let default_location = active_servers
            .last()
            .cloned()
            .unwrap_or_else(|| self.inner.fallback_location.clone());
        *self.inner.default_location.write().unwrap() = default_location;
    }

    /// Flush pending gateway traffic.
    ///
    /// This first flushes the muxers for all live procs attached to the
    /// gateway, then flushes the gateway's forwarder. Flushing the proc muxers
    /// drains local delivery and any return paths rooted in attached procs;
    /// flushing the forwarder drains outbound traffic that the gateway has
    /// routed away from those procs.
    ///
    /// The live proc set is snapshotted before awaiting, so we do not hold the
    /// proc map while flushing. Procs that have already been dropped are
    /// ignored. Concurrent posts may race with this operation; `flush` only
    /// guarantees that each flushed sender observes its usual sender-level
    /// flush semantics at the time it is flushed.
    pub(crate) async fn flush(&self) -> Result<(), anyhow::Error> {
        let procs = self
            .inner
            .procs
            .read()
            .unwrap()
            .values()
            .filter_map(WeakProc::upgrade)
            .collect::<Vec<_>>();
        for proc in procs {
            proc.muxer().flush().await?;
        }
        self.inner.forwarder.flush().await
    }
}

impl fmt::Debug for Gateway {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Gateway")
            .field("default_location", &self.default_location())
            .finish()
    }
}

/// A running gateway server.
#[derive(Debug)]
pub struct GatewayServeHandle {
    gateway: Gateway,
    location: Location,
    handle: MailboxServerHandle,
    stopped: Arc<AtomicBool>,
}

impl GatewayServeHandle {
    /// Signal the gateway server to stop.
    pub fn stop(&self, reason: &str) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.handle.stop(reason);
            self.gateway.remove_server(&self.location);
        }
    }
}

impl Future for GatewayServeHandle {
    type Output = <MailboxServerHandle as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `handle` is structurally pinned with `GatewayServeHandle`:
        // this type is `!Unpin` because `MailboxServerHandle` is `!Unpin`, it
        // has no `Drop` impl that moves `handle`, and no method moves `handle`
        // out of a pinned `GatewayServeHandle`.
        let handle = unsafe {
            self.as_mut()
                .map_unchecked_mut(|container| &mut container.handle)
        };
        let result = handle.poll(cx);
        if result.is_ready() {
            // SAFETY: We only mutate unpinned bookkeeping fields after polling
            // the pinned `handle`; this does not move `handle` or any other
            // pinned field out of `self`.
            let this = unsafe { self.get_unchecked_mut() };
            if !this.stopped.swap(true, Ordering::AcqRel) {
                this.gateway.remove_server(&this.location);
            }
        }
        result
    }
}

#[derive(Clone, Debug)]
struct WeakGateway(Weak<GatewayState>);

impl WeakGateway {
    fn new(gateway: &Gateway) -> Self {
        Self(Arc::downgrade(&gateway.inner))
    }

    fn upgrade(&self) -> Option<Gateway> {
        self.0.upgrade().map(|inner| Gateway { inner })
    }
}

#[async_trait]
impl crate::mailbox::MailboxSender for WeakGateway {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        match self.upgrade() {
            Some(gateway) => gateway.post(envelope, return_handle),
            None => envelope.undeliverable(
                DeliveryError::BrokenLink("failed to upgrade WeakGateway".to_string()),
                return_handle,
            ),
        }
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        match self.upgrade() {
            Some(gateway) => Gateway::flush(&gateway).await,
            None => Ok(()),
        }
    }
}

#[async_trait]
impl crate::mailbox::MailboxSender for Gateway {
    fn post_unchecked(
        &self,
        envelope: MessageEnvelope,
        return_handle: PortHandle<Undeliverable<MessageEnvelope>>,
    ) {
        let dest_proc = envelope.dest().actor_addr().proc_addr();
        let weak_proc = self
            .inner
            .procs
            .read()
            .unwrap()
            .get(dest_proc.id())
            .cloned();
        let proc = weak_proc.as_ref().and_then(WeakProc::upgrade);

        if weak_proc.is_some() && proc.is_none() {
            let mut procs = self.inner.procs.write().unwrap();
            if procs
                .get(dest_proc.id())
                .and_then(WeakProc::upgrade)
                .is_none()
            {
                procs.remove(dest_proc.id());
            }
        }

        if let Some(proc) = proc {
            if proc.is_local_delivery_target(&dest_proc) {
                proc.muxer().post(envelope, return_handle);
                return;
            }
        }

        self.inner.forwarder.post(envelope, return_handle)
    }

    async fn flush(&self) -> Result<(), anyhow::Error> {
        Gateway::flush(self).await
    }
}
