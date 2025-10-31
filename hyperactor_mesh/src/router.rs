/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module supports (in-process) global routing in hyperactor meshes.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::OnceLock;

use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelError;
use hyperactor::channel::ChannelTransport;
use hyperactor::mailbox::DialMailboxRouter;
use hyperactor::mailbox::MailboxRouter;
use hyperactor::mailbox::MailboxServer;
use tokio::sync::Mutex;

/// The shared, global router for this process.
pub fn global() -> &'static Router {
    static GLOBAL_ROUTER: OnceLock<Router> = OnceLock::new();
    GLOBAL_ROUTER.get_or_init(Router::new)
}

/// Router augments [`MailboxRouter`] with additional APIs and
/// bookeeping relevant to meshes.
pub struct Router {
    router: MailboxRouter,
    #[allow(dead_code)] // `servers` isn't read
    servers: Mutex<HashMap<ChannelTransport, ChannelAddr>>,
}

/// Deref so that we can use the [`MailboxRouter`] APIs directly.
impl Deref for Router {
    type Target = MailboxRouter;

    fn deref(&self) -> &Self::Target {
        &self.router
    }
}

impl Router {
    /// Create a new router.
    fn new() -> Self {
        Self {
            router: MailboxRouter::new(),
            servers: Mutex::new(HashMap::new()),
        }
    }

    /// Serve this router on the provided transport, returning the address.
    /// Servers are memoized, and we maintain only one per transport; thus
    /// subsequent calls using the same transport will return the same address.
    #[allow(dead_code)]
    #[tracing::instrument(skip(self))]
    pub async fn serve(&self, transport: &ChannelTransport) -> Result<ChannelAddr, ChannelError> {
        let mut servers = self.servers.lock().await;
        if let Some(addr) = servers.get(transport) {
            return Ok(addr.clone());
        }

        let (addr, rx) = channel::serve(ChannelAddr::any(transport.clone()))?;
        self.router.clone().serve(rx);
        servers.insert(transport.clone(), addr.clone());
        Ok(addr)
    }

    /// Binds a [`DialMailboxRouter`] directly into this router. Specifically, each
    /// prefix served by `router` is bound directly into this [`MailboxRouter`].
    pub fn bind_dial_router(&self, router: &DialMailboxRouter) {
        for prefix in router.prefixes() {
            self.router.bind(prefix, router.clone());
        }
    }
}
