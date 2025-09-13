/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines [`Host`], which represents all the procs running on a host.
//! The procs themselves are managed by an implementation of [`ProcManager`], which may,
//! for example, fork new processes for each proc, or spawn them in the same process
//! for testing purposes.
//!
//! The primary purpose of a host is to manage the lifecycle of these procs, and to
//! serve as a single front-end for all the procs on a host, multiplexing network
//! channels.
//!
//! ## Channel muxing
//!
//! A [`Host`] maintains a single frontend address, through which all procs are accessible
//! through direct addressing: the id of each proc is the `ProcId::Direct(frontend_addr, proc_name)`.
//! In the following, the frontend address is denoted by `*`. The host listens on `*` and
//! multiplexes messages based on the proc name. When spawning procs, the host maintains
//! backend channels with separate addresses. In the diagram `#` is the backend address of
//! the host, while `#n` is the backend address for proc *n*. The host forwards messages
//! to the appropriate backend channel, while procs forward messages to the host backend
//! channel at `#`.
//!
//! ```text
//!                      ┌────────────┐
//!                  ┌───▶  proc *,1  │
//!                  │ #1└────────────┘
//!                  │                 
//!  ┌──────────┐    │   ┌────────────┐
//!  │   Host   │◀───┼───▶  proc *,2  │
//! *└──────────┘#   │ #2└────────────┘
//!                  │                 
//!                  │   ┌────────────┐
//!                  └───▶  proc *,3  │
//!                    #3└────────────┘
//! ```
use std::collections::HashMap;

use async_trait::async_trait;

use crate::ProcId;
use crate::channel;
use crate::channel::ChannelAddr;
use crate::channel::ChannelError;
use crate::channel::ChannelTransport;
use crate::mailbox::DialMailboxRouter;
use crate::mailbox::MailboxServer;
use crate::mailbox::MailboxServerHandle;

/// The type of error produced by host operations.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// A channel error occurred during a host operation.
    #[error(transparent)]
    ChannelError(#[from] ChannelError),

    /// The named proc already exists and cannot be spawned.
    #[error("proc '{0}' already exists")]
    ProcExists(String),
}

/// A trait describing a manager of procs, responsible for bootstrapping
/// procs on a host, and managing their lifetimes.
#[async_trait]
pub trait ProcManager {
    /// The preferred transport for this ProcManager.
    /// In practice this will be [`ChannelTransport::Local`]
    /// for testing, and [`ChannelTransport::Unix`] for external
    /// processes.
    fn transport(&self) -> ChannelTransport;

    /// Spawn a new proc with the provided proc id. The proc
    /// should use the provided forwarder address for messages
    /// destined outside of the proc. The returned address accepts
    /// messages destined for the proc.
    async fn spawn(
        &self,
        proc_id: ProcId,
        forwarder_addr: ChannelAddr,
    ) -> Result<ChannelAddr, HostError>;

    // TODO: full lifecycle management; perhaps mimick the Command API.
}

/// A host, managing the lifecycle of several procs, and their backend
/// routing, as described in this module's documentation.
pub struct Host {
    procs: HashMap<String, ChannelAddr>,
    frontend_addr: ChannelAddr,
    backend_addr: ChannelAddr,
    router: DialMailboxRouter,
    manager: Box<dyn ProcManager>,
}

impl Host {
    /// Serve a host using the provided ProcManager, on the provided `addr`.
    /// On success, the host will multiplex messages for procs on the host
    /// on the address of the host.
    pub async fn serve(
        manager: Box<dyn ProcManager>,
        addr: ChannelAddr,
    ) -> Result<(Self, MailboxServerHandle), HostError> {
        let (frontend_addr, frontend_rx) = channel::serve(addr).await?;
        let router = DialMailboxRouter::new();

        // Establish a backend channel on the preferred transport. We currently simply
        // serve the same router on both.
        //
        // This works because this setup assumes only direct-addressed procs, and thus
        // the `DialMailboxRouter` will manage a common channel pool to other hosts.
        let (backend_addr, backend_rx) =
            channel::serve(ChannelAddr::any(manager.transport())).await?;
        router.clone().serve(backend_rx);

        let host = Host {
            procs: HashMap::new(),
            frontend_addr,
            backend_addr,
            router: router.clone(),
            manager,
        };

        Ok((host, router.serve(frontend_rx)))
    }

    /// The address which accepts messages destined for this host.
    pub fn addr(&self) -> &ChannelAddr {
        &self.frontend_addr
    }

    /// Spawn a new process with the given `name`. On success, the proc has been
    /// spawned, and is reachable through the returned, direct-addressed ProcId,
    /// which will be `ProcId::Direct(self.addr(), name)`.
    pub async fn spawn(&mut self, name: String) -> Result<ProcId, HostError> {
        if self.procs.contains_key(&name) {
            return Err(HostError::ProcExists(name));
        }

        let proc_id = ProcId::Direct(self.frontend_addr.clone(), name.clone());
        let addr = self
            .manager
            .spawn(proc_id.clone(), self.backend_addr.clone())
            .await?;

        self.router.bind(proc_id.clone().into(), addr.clone());
        self.procs.insert(name, addr);
        Ok(proc_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;
    use crate::Proc;
    use crate::channel::ChannelTransport;
    use crate::context::Mailbox as _;
    use crate::mailbox::IntoBoxedMailboxSender;
    use crate::mailbox::MailboxClient;

    struct TestProcManager {
        procs: Arc<Mutex<HashMap<ProcId, Proc>>>,
    }

    #[async_trait]
    impl ProcManager for TestProcManager {
        fn transport(&self) -> ChannelTransport {
            ChannelTransport::Local
        }

        async fn spawn(
            &self,
            proc_id: ProcId,
            forwarder_addr: ChannelAddr,
        ) -> Result<ChannelAddr, HostError> {
            let transport = forwarder_addr.transport();
            let proc = Proc::new(
                proc_id.clone(),
                MailboxClient::dial(forwarder_addr)?.into_boxed(),
            );
            let (proc_addr, rx) = channel::serve(ChannelAddr::any(transport)).await?;
            self.procs
                .lock()
                .unwrap()
                .insert(proc_id.clone(), proc.clone());
            let _handle = proc.clone().serve(rx);
            Ok(proc_addr)
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let procs = Arc::new(Mutex::new(HashMap::new()));
        let (mut host, _handle) = Host::serve(
            Box::new(TestProcManager {
                procs: Arc::clone(&procs),
            }),
            ChannelAddr::any(ChannelTransport::Local),
        )
        .await
        .unwrap();

        let proc_id1 = host.spawn("proc1".to_string()).await.unwrap();
        assert_eq!(
            proc_id1,
            ProcId::Direct(host.addr().clone(), "proc1".to_string())
        );
        assert!(procs.lock().unwrap().contains_key(&proc_id1));

        let proc_id2 = host.spawn("proc2".to_string()).await.unwrap();
        assert!(procs.lock().unwrap().contains_key(&proc_id2));

        let proc1 = procs.lock().unwrap().get(&proc_id1).unwrap().clone();
        let proc2 = procs.lock().unwrap().get(&proc_id2).unwrap().clone();

        // Make sure they can talk to each other:
        let (instance1, _handle) = proc1.instance("client").unwrap();
        let (instance2, _handle) = proc2.instance("client").unwrap();

        let (port, mut rx) = instance1.mailbox().open_port();

        port.bind().send(&instance2, "hello".to_string()).unwrap();
        assert_eq!(rx.recv().await.unwrap(), "hello".to_string());
    }
}
