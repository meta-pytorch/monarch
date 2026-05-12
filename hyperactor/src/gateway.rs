/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared reachability state for Hyperactor procs.

use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;

use crate::Location;
use crate::ProcAddr;
use crate::ProcId;
use crate::channel::ChannelAddr;
use crate::channel::ChannelTransport;
use crate::mailbox::BoxedMailboxSender;
use crate::mailbox::MailboxSender as _;
use crate::mailbox::PanickingMailboxSender;

/// Shared ingress, egress, and advertised reachability state for one or more procs.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<GatewayState>,
}

struct GatewayState {
    /// The location used when constructing routeable addresses for
    /// newly bound refs.
    default_location: RwLock<Location>,

    /// Sender used to forward messages outside of the proc.
    forwarder: BoxedMailboxSender,
}

impl Gateway {
    /// Create a fresh local-only gateway.
    pub fn new() -> Self {
        Self::configured(
            ChannelAddr::any(ChannelTransport::Local).into(),
            BoxedMailboxSender::new(PanickingMailboxSender),
        )
    }

    /// Return the process-wide default gateway.
    pub fn default() -> &'static Self {
        static DEFAULT_GATEWAY: OnceLock<Gateway> = OnceLock::new();
        DEFAULT_GATEWAY.get_or_init(Self::new)
    }

    pub(crate) fn configured(default_location: Location, forwarder: BoxedMailboxSender) -> Self {
        Self {
            inner: Arc::new(GatewayState {
                default_location: RwLock::new(default_location),
                forwarder,
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

    pub(crate) async fn flush(&self) -> Result<(), anyhow::Error> {
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
