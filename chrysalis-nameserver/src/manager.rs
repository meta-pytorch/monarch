/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrysalis_core::Pid;
use chrysalis_transport::DatagramAddr;
use chrysalis_transport::DatagramSocket;
use chrysalis_transport::LinkLocalError;
use chrysalis_transport::LinkLocalProtocol;
use chrysalis_transport::Route;
use chrysalis_transport::RouteGate;
use chrysalis_transport::Router;
use chrysalis_transport::Stream;
use thiserror::Error;
use tokio::sync::watch;

use crate::EnumerationCursor;
use crate::EnumerationResult;
use crate::NameserverService;
use crate::ParentLink;
use crate::ParentLinkError;
use crate::ProcEntry;
use crate::RejectCode;
use crate::Resolution;
use crate::ResolveConsistency;
use crate::ResolveError;
use crate::UpstreamNameserver;
use crate::VersionRange;
use crate::link::Completion;
use crate::link::CompletionGuard;

const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Bootstrap information for one logical parent nameserver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceConfig {
    identity: ParentIdentity,
    endpoints: Vec<ParentEndpoint>,
    retry_delay: Duration,
}

/// The identity constraint applied during parent bootstrap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParentIdentity {
    /// Require the parent's certificate-derived PID to equal this value.
    Pinned(Pid),
    /// Accept the first authenticated parent that completes the nameserver handshake and pin it.
    Discover,
}

/// One shared application and link-local control path to a parent gateway.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentEndpoint {
    address: DatagramAddr,
}

impl ParentEndpoint {
    /// Constructs one parent path.
    pub fn new(address: DatagramAddr) -> Self {
        Self { address }
    }

    /// Returns the shared control and application address.
    pub fn address(&self) -> &DatagramAddr {
        &self.address
    }
}

impl NamespaceConfig {
    /// Constructs a parent configuration with one or more alternative addresses.
    pub fn try_new(
        parent: Pid,
        endpoints: Vec<ParentEndpoint>,
    ) -> Result<Self, NamespaceConfigError> {
        if parent.is_link_local() {
            return Err(NamespaceConfigError::ReservedParent);
        }
        if endpoints.is_empty() {
            return Err(NamespaceConfigError::NoAddresses);
        }
        Ok(Self {
            identity: ParentIdentity::Pinned(parent),
            endpoints,
            retry_delay: DEFAULT_RETRY_DELAY,
        })
    }

    /// Constructs a parent configuration that discovers its identity from the first connection.
    pub fn try_discover(endpoints: Vec<ParentEndpoint>) -> Result<Self, NamespaceConfigError> {
        if endpoints.is_empty() {
            return Err(NamespaceConfigError::NoAddresses);
        }
        Ok(Self {
            identity: ParentIdentity::Discover,
            endpoints,
            retry_delay: DEFAULT_RETRY_DELAY,
        })
    }

    /// Sets the delay after one unsuccessful pass over all addresses.
    pub fn with_retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }

    /// Returns the parent identity constraint.
    pub const fn identity(&self) -> ParentIdentity {
        self.identity
    }

    /// Returns the alternative parent paths in attempt order.
    pub fn endpoints(&self) -> &[ParentEndpoint] {
        &self.endpoints
    }

    /// Returns the delay after an unsuccessful address round.
    pub const fn retry_delay(&self) -> Duration {
        self.retry_delay
    }
}

/// An invalid parent namespace configuration.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NamespaceConfigError {
    /// PID zero cannot authenticate a parent nameserver.
    #[error("reserved link-local parent PID")]
    ReservedParent,

    /// No address can reach the configured parent.
    #[error("parent configuration has no addresses")]
    NoAddresses,
}

/// A terminal parent-link manager failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParentManagerError {
    /// The parent explicitly rejected the nameserver link.
    Rejected {
        /// The authenticated parent that rejected the link.
        parent: Pid,
        /// The local child PID presented to the parent.
        child: Pid,
        /// The address used for the rejected connection.
        address: DatagramAddr,
        /// The protocol-level rejection reason.
        code: RejectCode,
    },
}

impl fmt::Display for ParentManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { child, code, .. } => {
                formatter.write_str("parent rejected nameserver link for PID ")?;
                for byte in child.as_bytes() {
                    write!(formatter, "{byte:02x}")?;
                }
                write!(formatter, ": {code}")
            }
        }
    }
}

impl std::error::Error for ParentManagerError {}

/// Observable parent connection state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParentManagerStatus {
    /// No parent control connection is active.
    Connecting,
    /// A QUIC connection is active through one configured address.
    Connected {
        /// The authenticated PID of the active parent.
        peer: Pid,
        /// The address used for this connection incarnation.
        address: DatagramAddr,
        /// The parent-allocated link incarnation.
        link: crate::LinkId,
    },
    /// The manager stopped retrying after a terminal protocol failure.
    Failed {
        /// The terminal failure.
        error: ParentManagerError,
    },
    /// The manager terminated.
    Stopped,
}

/// Maintains one reconnecting parent nameserver link across alternative addresses.
///
/// Nameserver streams are selected from the shared link-local transport by their reserved protocol
/// identifier. Other link-local protocols can independently reuse the same pooled QUIC connection.
pub struct ParentLinkManager<T: DatagramSocket> {
    current: watch::Receiver<Option<ParentLink>>,
    status: watch::Receiver<ParentManagerStatus>,
    completion: Arc<Completion>,
    _protocol: LinkLocalProtocol<T>,
}

impl<T: DatagramSocket + 'static> ParentLinkManager<T> {
    /// Spawns a reconnecting parent-link manager.
    pub fn spawn(
        config: NamespaceConfig,
        protocol: LinkLocalProtocol<T>,
        service: Arc<NameserverService>,
        local_entry: ProcEntry,
        versions: VersionRange,
    ) -> Result<Self, crate::ParentLinkError> {
        Self::spawn_inner(config, protocol, service, local_entry, versions, None)
    }

    /// Spawns a reconnecting parent-link manager that maintains a gated default route.
    pub fn spawn_with_router(
        config: NamespaceConfig,
        protocol: LinkLocalProtocol<T>,
        service: Arc<NameserverService>,
        local_entry: ProcEntry,
        versions: VersionRange,
        router: Arc<Router>,
    ) -> Result<Self, crate::ParentLinkError> {
        Self::spawn_inner(
            config,
            protocol,
            service,
            local_entry,
            versions,
            Some(router),
        )
    }

    fn spawn_inner(
        config: NamespaceConfig,
        protocol: LinkLocalProtocol<T>,
        service: Arc<NameserverService>,
        local_entry: ProcEntry,
        versions: VersionRange,
        router: Option<Arc<Router>>,
    ) -> Result<Self, crate::ParentLinkError> {
        if local_entry.pid != service.authority() {
            return Err(crate::ParentLinkError::LocalPidMismatch);
        }
        if config.identity == ParentIdentity::Pinned(service.authority()) {
            return Err(crate::ParentLinkError::SelfLink);
        }
        let (current_tx, current) = watch::channel(None);
        let (status_tx, status) = watch::channel(ParentManagerStatus::Connecting);
        let completion = Arc::new(Completion::default());
        let task_completion = completion.clone();
        let task_protocol = protocol.clone();
        tokio::spawn(async move {
            let _guard = CompletionGuard(&task_completion);
            supervise(
                config,
                task_protocol,
                service,
                local_entry,
                versions,
                router,
                current_tx,
                status_tx,
                task_completion.clone(),
            )
            .await;
        });
        Ok(Self {
            current,
            status,
            completion,
            _protocol: protocol,
        })
    }

    /// Subscribes to coalescing parent connection status changes.
    pub fn subscribe(&self) -> watch::Receiver<ParentManagerStatus> {
        self.status.clone()
    }

    /// Resolves through the current parent, waiting across reconnects when necessary.
    pub async fn resolve(
        &self,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Result<Resolution, ResolveError> {
        let mut current = self.current.clone();
        loop {
            let link = current.borrow().clone();
            if let Some(link) = link
                && let Ok(result) = link.resolve(pid, consistency).await
            {
                return Ok(result);
            }
            tokio::select! {
                changed = current.changed() => {
                    if changed.is_err() {
                        return Err(ResolveError::Closed);
                    }
                }
                () = self.completion.cancelled() => return Err(ResolveError::Closed),
            }
        }
    }

    /// Enumerates through the current parent, waiting across reconnects when necessary.
    pub async fn enumerate(
        &self,
        cursor: Option<EnumerationCursor>,
        limit: u32,
        consistency: ResolveConsistency,
    ) -> Result<EnumerationResult, ResolveError> {
        let mut current = self.current.clone();
        loop {
            let link = current.borrow().clone();
            if let Some(link) = link
                && let Ok(result) = link.enumerate(cursor, limit, consistency).await
            {
                return Ok(result);
            }
            tokio::select! {
                changed = current.changed() => {
                    if changed.is_err() {
                        return Err(ResolveError::Closed);
                    }
                }
                () = self.completion.cancelled() => return Err(ResolveError::Closed),
            }
        }
    }

    /// Idempotently requests manager and active-link shutdown.
    pub fn shutdown(&self) {
        self.completion.shutdown();
    }

    /// Waits for the manager task and its active link to terminate.
    pub async fn join(&self) {
        self.completion.join().await;
    }
}

impl<T: DatagramSocket + 'static> UpstreamNameserver for ParentLinkManager<T> {
    fn resolve(
        &self,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Pin<Box<dyn Future<Output = Result<Resolution, ResolveError>> + Send + '_>> {
        Box::pin(ParentLinkManager::resolve(self, pid, consistency))
    }

    fn enumerate(
        &self,
        cursor: Option<EnumerationCursor>,
        limit: u32,
        consistency: ResolveConsistency,
    ) -> Pin<Box<dyn Future<Output = Result<EnumerationResult, ResolveError>> + Send + '_>> {
        Box::pin(ParentLinkManager::enumerate(
            self,
            cursor,
            limit,
            consistency,
        ))
    }
}

impl<T: DatagramSocket> Drop for ParentLinkManager<T> {
    fn drop(&mut self) {
        self.completion.shutdown();
    }
}

async fn supervise<T: DatagramSocket + 'static>(
    config: NamespaceConfig,
    protocol: LinkLocalProtocol<T>,
    service: Arc<NameserverService>,
    local_entry: ProcEntry,
    versions: VersionRange,
    router: Option<Arc<Router>>,
    current: watch::Sender<Option<ParentLink>>,
    status: watch::Sender<ParentManagerStatus>,
    completion: Arc<Completion>,
) {
    let mut next_endpoint = 0usize;
    let mut pinned = match config.identity {
        ParentIdentity::Pinned(parent) => Some(parent),
        ParentIdentity::Discover => None,
    };
    let mut connected_once = false;
    loop {
        let (endpoint, completed_round) = next_endpoint_in(&config.endpoints, &mut next_endpoint);
        let address = endpoint.address.clone();
        let connection = tokio::select! {
            result = dial_parent(&protocol, pinned, address.clone()) => result,
            () = completion.cancelled() => break,
        };
        match connection {
            Ok((peer, stream)) => {
                if peer == service.authority() {
                    status.send_replace(ParentManagerStatus::Connecting);
                    if completed_round {
                        tokio::select! {
                            () = tokio::time::sleep(config.retry_delay) => {}
                            () = completion.cancelled() => break,
                        }
                    }
                    continue;
                }
                let link = ParentLink::spawn(
                    peer,
                    address.clone(),
                    stream,
                    service.clone(),
                    local_entry.clone(),
                    versions,
                )
                .expect("validated parent link configuration");
                let active = tokio::select! {
                    result = link.wait_active() => result,
                    () = completion.cancelled() => {
                        link.shutdown();
                        link.join().await;
                        break;
                    }
                };
                let link_id = match active {
                    Ok(link_id) => link_id,
                    Err(error) => {
                        link.join().await;
                        if let ParentLinkError::Rejected { code } = error
                            && rejection_is_terminal(code, connected_once)
                        {
                            current.send_replace(None);
                            status.send_replace(ParentManagerStatus::Failed {
                                error: ParentManagerError::Rejected {
                                    parent: peer,
                                    child: service.authority(),
                                    address,
                                    code,
                                },
                            });
                            return;
                        }
                        status.send_replace(ParentManagerStatus::Connecting);
                        if completed_round {
                            tokio::select! {
                                () = tokio::time::sleep(config.retry_delay) => {}
                                () = completion.cancelled() => break,
                            }
                        }
                        continue;
                    }
                };
                connected_once = true;
                pinned = Some(peer);
                current.send_replace(Some(link.clone()));
                status.send_replace(ParentManagerStatus::Connected {
                    peer,
                    address,
                    link: link_id,
                });
                let route_gate = router.as_ref().map(|router| {
                    let gate = RouteGate::new();
                    router.set_default(Route::gated(endpoint.address.clone(), gate.clone()));
                    gate
                });
                let shutting_down = tokio::select! {
                    () = link.join() => false,
                    () = completion.cancelled() => {
                        true
                    }
                };
                if let Some(gate) = route_gate {
                    gate.close();
                    router
                        .as_ref()
                        .expect("route gate requires router")
                        .remove_default();
                }
                if shutting_down {
                    link.shutdown();
                    link.join().await;
                }
                current.send_replace(None);
                if shutting_down {
                    break;
                }
                status.send_replace(ParentManagerStatus::Connecting);
            }
            Err(_) => {
                status.send_replace(ParentManagerStatus::Connecting);
            }
        }
        if completed_round {
            tokio::select! {
                () = tokio::time::sleep(config.retry_delay) => {}
                () = completion.cancelled() => break,
            }
        }
    }
    current.send_replace(None);
    status.send_replace(ParentManagerStatus::Stopped);
}

fn rejection_is_terminal(code: RejectCode, connected_once: bool) -> bool {
    match code {
        RejectCode::IncompatibleVersion | RejectCode::IdentityMismatch => true,
        RejectCode::AlreadyLinked => !connected_once,
        RejectCode::Unavailable => false,
    }
}

async fn dial_parent<T: DatagramSocket>(
    protocol: &LinkLocalProtocol<T>,
    parent: Option<Pid>,
    address: DatagramAddr,
) -> Result<(Pid, Stream), LinkLocalError> {
    match parent {
        Some(parent) => protocol
            .dial(parent, address)
            .await
            .map(|stream| (parent, stream)),
        None => protocol.dial_unpinned(address).await,
    }
}

fn next_endpoint_in(endpoints: &[ParentEndpoint], next: &mut usize) -> (ParentEndpoint, bool) {
    let endpoint = endpoints[*next].clone();
    *next = (*next + 1) % endpoints.len();
    (endpoint, *next == 0)
}

#[cfg(test)]
mod tests {
    use chrysalis_transport::DatagramAddr;

    use super::*;

    const PARENT: Pid = Pid::from_bytes([1; 16]);
    const CHILD: Pid = Pid::from_bytes([2; 16]);

    fn address(value: u8) -> DatagramAddr {
        DatagramAddr::new("test", [value])
    }

    fn endpoint(value: u8) -> ParentEndpoint {
        ParentEndpoint::new(address(value))
    }

    #[test]
    fn configuration_requires_authenticated_parent_and_address() {
        assert_eq!(
            NamespaceConfig::try_new(Pid::LINK_LOCAL, vec![endpoint(1)]),
            Err(NamespaceConfigError::ReservedParent)
        );
        assert_eq!(
            NamespaceConfig::try_new(PARENT, Vec::new()),
            Err(NamespaceConfigError::NoAddresses)
        );
        let config = NamespaceConfig::try_new(PARENT, vec![endpoint(1), endpoint(2)])
            .unwrap()
            .with_retry_delay(Duration::from_millis(10));
        assert_eq!(config.identity(), ParentIdentity::Pinned(PARENT));
        assert_eq!(config.endpoints(), [endpoint(1), endpoint(2)]);
        assert_eq!(config.retry_delay(), Duration::from_millis(10));

        let config = NamespaceConfig::try_discover(vec![endpoint(3)]).unwrap();
        assert_eq!(config.identity(), ParentIdentity::Discover);
        assert_eq!(config.endpoints(), [endpoint(3)]);
        assert_eq!(
            NamespaceConfig::try_discover(Vec::new()),
            Err(NamespaceConfigError::NoAddresses)
        );
    }

    #[test]
    fn address_cycle_preserves_order_and_marks_round_boundary() {
        let endpoints = [endpoint(1), endpoint(2), endpoint(3)];
        let mut next = 0;
        assert_eq!(
            next_endpoint_in(&endpoints, &mut next),
            (endpoint(1), false)
        );
        assert_eq!(
            next_endpoint_in(&endpoints, &mut next),
            (endpoint(2), false)
        );
        assert_eq!(next_endpoint_in(&endpoints, &mut next), (endpoint(3), true));
        assert_eq!(
            next_endpoint_in(&endpoints, &mut next),
            (endpoint(1), false)
        );
    }

    #[test]
    fn only_terminal_admission_failures_stop_initial_connection() {
        assert!(rejection_is_terminal(
            RejectCode::IncompatibleVersion,
            false
        ));
        assert!(rejection_is_terminal(RejectCode::IdentityMismatch, false));
        assert!(rejection_is_terminal(RejectCode::AlreadyLinked, false));
        assert!(!rejection_is_terminal(RejectCode::AlreadyLinked, true));
        assert!(!rejection_is_terminal(RejectCode::Unavailable, false));
    }

    #[test]
    fn terminal_rejection_identifies_the_local_pid() {
        let error = ParentManagerError::Rejected {
            parent: PARENT,
            child: CHILD,
            address: address(1),
            code: RejectCode::AlreadyLinked,
        };
        assert_eq!(
            error.to_string(),
            "parent rejected nameserver link for PID 02020202020202020202020202020202: already has an active link"
        );
    }
}
