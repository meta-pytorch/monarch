/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::net::UdpSocket as StdUdpSocket;
use std::sync::Arc;

use chrysalis_core::LinkContext;
use chrysalis_core::Pid;
use chrysalis_core::PidPrefix;
use chrysalis_nameserver::ApplyError;
use chrysalis_nameserver::ChildLinkServer;
use chrysalis_nameserver::DEFAULT_ENUMERATION_PAGE_SIZE;
use chrysalis_nameserver::EnumerationCursor;
use chrysalis_nameserver::EnumerationResult;
use chrysalis_nameserver::Labels;
use chrysalis_nameserver::LocalEntryError;
use chrysalis_nameserver::Locator;
use chrysalis_nameserver::NAMESERVER_LINK_PROTOCOL;
use chrysalis_nameserver::NameserverService;
use chrysalis_nameserver::NamespaceConfig;
use chrysalis_nameserver::ParentLinkError;
use chrysalis_nameserver::ParentLinkManager;
use chrysalis_nameserver::ParentManagerStatus;
use chrysalis_nameserver::ProcEntry;
use chrysalis_nameserver::Resolution;
use chrysalis_nameserver::ResolveConsistency;
use chrysalis_nameserver::ResolveError;
use chrysalis_nameserver::VERSION_1;
use chrysalis_nameserver::VERSION_3;
use chrysalis_nameserver::VersionRange;
use chrysalis_transport::DatagramAddr;
use chrysalis_transport::DatagramSocket;
use chrysalis_transport::DatagramSwitch;
use chrysalis_transport::IncomingStream;
use chrysalis_transport::LinkLocalError;
use chrysalis_transport::LinkLocalMux;
use chrysalis_transport::LinkLocalProtocolId;
use chrysalis_transport::QuicConfig;
use chrysalis_transport::QuicIdentity;
use chrysalis_transport::QuicTransport;
use chrysalis_transport::QuicTransportError;
use chrysalis_transport::Router;
use chrysalis_transport::Stream;
use chrysalis_transport::SwitchSocket;
use chrysalis_transport::UdpSocket;
use thiserror::Error;
use tokio::sync::watch;
use tracing::error;

use crate::link_protocol::HandlerFuture;
use crate::link_protocol::LinkProtocolManager;
use crate::link_protocol::ParentTarget;
use crate::link_protocol::Registration;
use crate::socket::DynSocket;

const DEFAULT_NEGATIVE_TTL_MILLIS: u64 = 1_000;
const MAX_ENUMERATION_RESTARTS: usize = 8;

/// One carrier binding and the QUIC identity served on it.
#[derive(Debug)]
pub struct TransportConfig {
    binding: TransportBinding,
    identity: QuicIdentity,
    quic: QuicConfig,
}

#[derive(Debug)]
enum TransportBinding {
    Carrier(Arc<dyn DatagramSocket>),
    DirectUdp {
        socket: StdUdpSocket,
        local_addr: DatagramAddr,
    },
    RoutedUdp {
        socket: std::net::UdpSocket,
        fallback: Option<Arc<dyn DatagramSocket>>,
        local_addr: chrysalis_transport::DatagramAddr,
    },
}

impl TransportConfig {
    /// Constructs a transport binding from any Chrysalis datagram socket.
    pub fn new<T>(socket: Arc<T>, identity: QuicIdentity) -> Self
    where
        T: DatagramSocket,
    {
        Self {
            binding: TransportBinding::Carrier(socket),
            identity,
            quic: QuicConfig::default(),
        }
    }

    /// Constructs a transport that drives a bound UDP socket directly with io_uring.
    pub fn direct_udp(socket: StdUdpSocket, identity: QuicIdentity) -> io::Result<Self> {
        let advertised = socket.local_addr()?;
        Ok(Self::direct_udp_advertised(socket, advertised, identity))
    }

    /// Constructs a direct UDP transport with an externally reachable advertised address.
    pub fn direct_udp_advertised(
        socket: StdUdpSocket,
        advertised: SocketAddr,
        identity: QuicIdentity,
    ) -> Self {
        let local_addr = UdpSocket::datagram_addr(advertised);
        Self {
            binding: TransportBinding::DirectUdp { socket, local_addr },
            identity,
            quic: QuicConfig::default(),
        }
    }

    /// Constructs a routed UDP transport with an optional non-UDP link-local carrier.
    pub fn routed_udp(
        socket: std::net::UdpSocket,
        fallback: Option<Arc<dyn DatagramSocket>>,
        identity: QuicIdentity,
    ) -> io::Result<Self> {
        let local_addr = UdpSocket::datagram_addr(socket.local_addr()?);
        Ok(Self {
            binding: TransportBinding::RoutedUdp {
                socket,
                fallback,
                local_addr,
            },
            identity,
            quic: QuicConfig::default(),
        })
    }

    /// Replaces the application endpoint and connection QUIC policy.
    pub fn with_quic_config(mut self, quic: QuicConfig) -> Self {
        self.quic = quic;
        self
    }

    fn local_addr(&self) -> &DatagramAddr {
        match &self.binding {
            TransportBinding::Carrier(socket) => socket.local_addr(),
            TransportBinding::DirectUdp { local_addr, .. }
            | TransportBinding::RoutedUdp { local_addr, .. } => local_addr,
        }
    }
}

/// Complete construction policy for one [`Node`].
pub struct NodeConfig {
    transport: TransportConfig,
    locators: Vec<Locator>,
    labels: Labels,
    parent: Option<NamespaceConfig>,
    versions: VersionRange,
    negative_ttl_millis: u64,
    link_protocols: Vec<Registration>,
}

impl NodeConfig {
    /// Constructs a root configuration with one application and link-local control carrier.
    ///
    /// The application carrier's address is advertised with priority zero by default.
    pub fn new(transport: TransportConfig) -> Self {
        let locators = vec![Locator {
            address: transport.local_addr().clone(),
            priority: 0,
        }];
        Self {
            transport,
            locators,
            labels: Labels::new(),
            parent: None,
            versions: VersionRange::try_new(VERSION_1, VERSION_3)
                .expect("supported version range must be ordered"),
            negative_ttl_millis: DEFAULT_NEGATIVE_TTL_MILLIS,
            link_protocols: Vec::new(),
        }
    }

    /// Sets the parent namespace configuration.
    pub fn with_parent(mut self, parent: NamespaceConfig) -> Self {
        self.parent = Some(parent);
        self
    }

    /// Replaces the application locators advertised to the parent.
    pub fn with_locators(mut self, locators: Vec<Locator>) -> Self {
        self.locators = locators;
        self
    }

    /// Replaces the labels published with this process's next-hop information.
    pub fn with_labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    /// Sets the nameserver protocol versions offered on parent and child links.
    pub fn with_versions(mut self, versions: VersionRange) -> Self {
        self.versions = versions;
        self
    }

    /// Sets the root nameserver's negative-result cache lifetime.
    pub fn with_negative_ttl_millis(mut self, value: u64) -> Self {
        self.negative_ttl_millis = value;
        self
    }

    /// Registers a link-local protocol session handler.
    ///
    /// The node opens and supervises one session for every admitted adjacent link.
    pub fn with_link_protocol<F, Fut>(mut self, protocol: LinkLocalProtocolId, handler: F) -> Self
    where
        F: Fn(LinkContext, Stream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler =
            Arc::new(move |context, stream| Box::pin(handler(context, stream)) as HandlerFuture);
        self.link_protocols.push(Registration {
            id: protocol,
            handler,
        });
        self
    }
}

/// A process node that composes transport, routing, and hierarchical nameserver state.
pub struct Node {
    pid: Pid,
    router: Arc<Router>,
    application_transport: Arc<QuicTransport<SwitchSocket>>,
    nameserver: Arc<NameserverService>,
    parent: Option<Arc<ParentLinkManager<SwitchSocket>>>,
    lifecycle: NodeLifecycle,
}

struct NodeComponents {
    datagram_switch: Option<Arc<DatagramSwitch<DynSocket>>>,
    application_transport: Arc<QuicTransport<SwitchSocket>>,
    link_mux: Arc<LinkLocalMux<SwitchSocket>>,
    link_protocols: Arc<LinkProtocolManager>,
    parent: Option<Arc<ParentLinkManager<SwitchSocket>>>,
    children: Arc<ChildLinkServer<SwitchSocket>>,
}

struct NodeLifecycle {
    shutdown: watch::Sender<bool>,
    terminated: watch::Sender<bool>,
}

impl NodeLifecycle {
    fn spawn(components: NodeComponents) -> Self {
        let (shutdown, mut shutdown_receiver) = watch::channel(false);
        let (terminated, _) = watch::channel(false);
        let task_terminated = terminated.clone();
        tokio::spawn(async move {
            let _guard = NodeTerminationGuard(task_terminated);
            tokio::select! {
                () = async {
                    while !*shutdown_receiver.borrow_and_update() {
                        if shutdown_receiver.changed().await.is_err() {
                            return;
                        }
                    }
                } => {}
                () = components.link_protocols.join() => {
                    error!("link protocol manager stopped unexpectedly");
                }
            }

            components.link_protocols.shutdown();
            if let Some(parent) = &components.parent {
                parent.shutdown();
            }
            components.children.shutdown();

            components.link_protocols.join().await;
            if let Some(parent) = &components.parent {
                parent.join().await;
            }
            components.children.join().await;

            components.link_mux.shutdown();
            components.link_mux.join().await;
            components.application_transport.shutdown();
            components.application_transport.join().await;
            if let Some(datagram_switch) = &components.datagram_switch {
                datagram_switch.shutdown();
                datagram_switch.join().await;
            }
        });
        Self {
            shutdown,
            terminated,
        }
    }

    fn shutdown(&self) {
        self.shutdown.send_replace(true);
    }

    async fn join(&self) {
        let mut terminated = self.terminated.subscribe();
        while !*terminated.borrow_and_update() {
            if terminated.changed().await.is_err() {
                return;
            }
        }
    }
}

struct NodeTerminationGuard(watch::Sender<bool>);

impl Drop for NodeTerminationGuard {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

impl Node {
    /// Creates and starts a node from fully bound carrier configurations.
    ///
    /// If construction fails, the `Drop` implementations of each already-created component close
    /// admission and stop its background task before the error is returned.
    pub fn create(config: NodeConfig) -> Result<Self, NodeError> {
        let NodeConfig {
            transport,
            locators,
            labels,
            parent: parent_config,
            versions,
            negative_ttl_millis,
            link_protocols,
        } = config;
        let TransportConfig {
            binding,
            identity,
            quic,
        } = transport;
        let pid = identity.pid();
        let tls_server_name = identity.certificate_server_name().to_owned();
        let router = Arc::new(Router::new());
        let (datagram_switch, application_transport) = match binding {
            TransportBinding::Carrier(socket) => {
                let socket = DynSocket::new(socket);
                let datagram_switch = Arc::new(DatagramSwitch::spawn(socket, router.clone()));
                let application_binding =
                    Arc::new(datagram_switch.bind_routed_many(&[pid, Pid::LINK_LOCAL])?);
                let transport = Arc::new(QuicTransport::spawn_with_config(
                    application_binding,
                    identity.clone(),
                    quic,
                )?);
                (Some(datagram_switch), transport)
            }
            TransportBinding::DirectUdp { socket, .. } => (
                None,
                Arc::new(QuicTransport::spawn_direct_udp_with_config(
                    socket, identity, quic,
                )?),
            ),
            TransportBinding::RoutedUdp {
                socket, fallback, ..
            } => (
                None,
                Arc::new(QuicTransport::spawn_routed_udp_with_config(
                    socket,
                    fallback,
                    router.clone(),
                    identity,
                    quic,
                )?),
            ),
        };
        let link_mux = Arc::new(LinkLocalMux::spawn(
            application_transport.clone(),
            std::iter::once(NAMESERVER_LINK_PROTOCOL)
                .chain(link_protocols.iter().map(|registration| registration.id)),
        )?);
        let nameserver_protocol = link_mux
            .protocol(NAMESERVER_LINK_PROTOCOL)
            .expect("nameserver protocol was registered");
        let nameserver = Arc::new(NameserverService::try_new_with_router(
            pid,
            versions,
            negative_ttl_millis,
            router.clone(),
        )?);
        let local_entry = ProcEntry {
            pid,
            tls_server_name,
            labels,
            locators,
        };
        nameserver.set_local_entry(local_entry.clone())?;
        let parent_retry_delay = parent_config.as_ref().map(NamespaceConfig::retry_delay);
        let parent = parent_config
            .map(|parent| {
                ParentLinkManager::spawn_with_router(
                    parent,
                    nameserver_protocol.clone(),
                    nameserver.clone(),
                    local_entry,
                    versions,
                    router.clone(),
                )
                .map(Arc::new)
            })
            .transpose()?;
        let children = Arc::new(match &parent {
            Some(parent) => ChildLinkServer::spawn_with_upstream(
                nameserver_protocol.clone(),
                nameserver.clone(),
                parent.clone(),
            ),
            None => ChildLinkServer::spawn(nameserver_protocol, nameserver.clone()),
        });
        let managed_protocols = link_protocols
            .into_iter()
            .map(|registration| {
                let protocol = link_mux
                    .protocol(registration.id)
                    .expect("configured link protocol was registered");
                (registration, protocol)
            })
            .collect();
        let parent_target = parent_retry_delay
            .zip(parent.as_ref())
            .map(|(retry_delay, parent)| ParentTarget {
                retry_delay,
                status: parent.subscribe(),
            });
        let link_protocols = Arc::new(LinkProtocolManager::spawn(
            managed_protocols,
            parent_target,
            nameserver.subscribe_child_links(),
        ));
        let lifecycle = NodeLifecycle::spawn(NodeComponents {
            datagram_switch,
            application_transport: application_transport.clone(),
            link_mux,
            link_protocols,
            parent: parent.clone(),
            children,
        });
        Ok(Self {
            pid,
            router,
            application_transport,
            nameserver,
            parent,
            lifecycle,
        })
    }

    /// Returns this process's authenticated PID.
    pub const fn pid(&self) -> Pid {
        self.pid
    }

    /// Returns the local nameserver service.
    pub fn nameserver(&self) -> &Arc<NameserverService> {
        &self.nameserver
    }

    /// Returns the application datagram router.
    pub fn router(&self) -> &Arc<Router> {
        &self.router
    }

    /// Returns the application QUIC transport.
    pub fn transport(&self) -> &Arc<QuicTransport<SwitchSocket>> {
        &self.application_transport
    }

    /// Subscribes to parent connection changes, or returns `None` for a root node.
    pub fn subscribe_parent(&self) -> Option<watch::Receiver<ParentManagerStatus>> {
        self.parent.as_ref().map(|parent| parent.subscribe())
    }

    /// Resolves a process through local delegated state or the configured parent.
    pub async fn resolve(
        &self,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Result<Resolution, NodeError> {
        if consistency == ResolveConsistency::Cached {
            let local = self.nameserver.resolve(pid).await;
            if matches!(local, Resolution::Found { .. }) {
                return Ok(local);
            }
        }
        match &self.parent {
            Some(parent) => Ok(parent.resolve(pid, consistency).await?),
            None => Ok(self.nameserver.resolve(pid).await),
        }
    }

    /// Expands a PID prefix against the visible namespace.
    pub async fn expand_pid(
        &self,
        prefix: PidPrefix,
        consistency: ResolveConsistency,
    ) -> Result<Pid, NodeError> {
        let entries = self.enumerate(consistency).await?;
        expand_pid(entries.into_iter().map(|entry| entry.pid), prefix)
    }

    /// Enumerates one revision-stable page through this node's nameserver path.
    pub async fn enumerate_page(
        &self,
        cursor: Option<EnumerationCursor>,
        limit: u32,
        consistency: ResolveConsistency,
    ) -> Result<EnumerationResult, NodeError> {
        if consistency == ResolveConsistency::Refresh
            && let Some(parent) = &self.parent
        {
            return Ok(parent.enumerate(cursor, limit, consistency).await?);
        }
        Ok(self.nameserver.enumerate(cursor, limit).await)
    }

    /// Enumerates all processes, restarting if the directory changes between pages.
    pub async fn enumerate(
        &self,
        consistency: ResolveConsistency,
    ) -> Result<Vec<ProcEntry>, NodeError> {
        for _ in 0..MAX_ENUMERATION_RESTARTS {
            let mut entries = Vec::new();
            let mut cursor = None;
            while let EnumerationResult::Page(page) = self
                .enumerate_page(cursor, DEFAULT_ENUMERATION_PAGE_SIZE, consistency)
                .await?
            {
                entries.extend(page.entries);
                let Some(next) = page.next else {
                    return Ok(entries);
                };
                cursor = Some(next);
            }
            tokio::task::yield_now().await;
        }
        Err(NodeError::EnumerationChanged)
    }

    /// Opens an authenticated bidirectional application stream to `pid`.
    pub async fn dial(
        &self,
        pid: Pid,
        consistency: ResolveConsistency,
    ) -> Result<Stream, NodeError> {
        let Resolution::Found { entry, .. } = self.resolve(pid, consistency).await? else {
            return Err(NodeError::NotFound { pid });
        };
        if entry.locators.is_empty() {
            return Err(NodeError::NoLocators { pid });
        }
        let server_name = entry.tls_server_name;
        let mut locators = entry.locators;
        locators.sort_by_key(|locator| locator.priority);
        let mut last_error = None;
        for locator in locators {
            match self
                .application_transport
                .dial_with_server_name(pid, locator.address, server_name.clone())
                .await
            {
                Ok(stream) => return Ok(stream),
                Err(error) => last_error = Some(error),
            }
        }
        Err(NodeError::Dial(
            last_error.expect("nonempty locators must produce a dial result"),
        ))
    }

    /// Accepts the next authenticated incoming application stream.
    pub async fn accept(&self) -> Result<IncomingStream, NodeError> {
        Ok(self.application_transport.accept().await?)
    }

    /// Idempotently requests shutdown of every node-owned component.
    pub fn shutdown(&self) {
        self.lifecycle.shutdown();
    }

    /// Waits for every node-owned task and carrier to terminate after [`Self::shutdown`].
    pub async fn join(&self) {
        self.lifecycle.join().await;
    }
}

fn expand_pid<I>(pids: I, prefix: PidPrefix) -> Result<Pid, NodeError>
where
    I: IntoIterator<Item = Pid>,
{
    let mut matches = pids.into_iter().filter(|pid| prefix.matches(*pid));
    let Some(pid) = matches.next() else {
        return Err(NodeError::PrefixNotFound { prefix });
    };
    if matches.next().is_some() {
        return Err(NodeError::AmbiguousPrefix { prefix });
    }
    Ok(pid)
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A node construction, resolution, or connection failure.
#[derive(Debug, Error)]
pub enum NodeError {
    /// A carrier or QUIC endpoint could not be created.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The local nameserver could not be created.
    #[error(transparent)]
    Nameserver(#[from] ApplyError),

    /// The local process entry does not match the node identity.
    #[error(transparent)]
    LocalEntry(#[from] LocalEntryError),

    /// The parent link configuration is invalid.
    #[error(transparent)]
    Parent(#[from] ParentLinkError),

    /// The parent link closed during resolution.
    #[error(transparent)]
    Resolve(#[from] ResolveError),

    /// The namespace changed throughout every bounded enumeration attempt.
    #[error("namespace kept changing during enumeration")]
    EnumerationChanged,

    /// The target was absent from the namespace.
    #[error("process is not present in the namespace: {pid:?}")]
    NotFound { pid: Pid },

    /// The target has no usable application locator.
    #[error("process has no application locators: {pid:?}")]
    NoLocators { pid: Pid },

    /// No visible process has the requested PID prefix.
    #[error("process PID prefix is not present in the namespace: {prefix}")]
    PrefixNotFound { prefix: PidPrefix },

    /// More than one visible process has the requested PID prefix.
    #[error("process PID prefix is ambiguous: {prefix}")]
    AmbiguousPrefix { prefix: PidPrefix },

    /// Every advertised locator failed to establish QUIC.
    #[error(transparent)]
    Dial(QuicTransportError),

    /// The application transport stopped accepting streams.
    #[error(transparent)]
    Transport(#[from] QuicTransportError),

    /// The link-local stream mux could not be constructed.
    #[error(transparent)]
    LinkLocal(#[from] LinkLocalError),
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use chrysalis_nameserver::ParentEndpoint;
    use chrysalis_nameserver::ParentManagerError;
    use chrysalis_nameserver::RejectCode;
    use chrysalis_transport::InprocNetwork;
    use rcgen::BasicConstraints;
    use rcgen::CertificateParams;
    use rcgen::CertifiedIssuer;
    use rcgen::ExtendedKeyUsagePurpose;
    use rcgen::IsCa;
    use rcgen::KeyPair;
    use rcgen::KeyUsagePurpose;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::time::timeout;

    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const TEST_LINK_PROTOCOL: LinkLocalProtocolId =
        LinkLocalProtocolId::from_bytes(*b"test.protocol.01");
    const UNKNOWN_LINK_PROTOCOL: LinkLocalProtocolId =
        LinkLocalProtocolId::from_bytes(*b"test.protocol.02");

    #[test]
    fn pid_prefix_expansion_requires_one_match() {
        let first = Pid::from_bytes([0xab; 16]);
        let second = Pid::from_bytes([0xac; 16]);

        assert_eq!(
            expand_pid([first, second], "ab".parse().expect("parse unique prefix"))
                .expect("expand unique prefix"),
            first
        );
        assert!(matches!(
            expand_pid(
                [first, second],
                "a".parse().expect("parse ambiguous prefix")
            ),
            Err(NodeError::AmbiguousPrefix { .. })
        ));
        assert!(matches!(
            expand_pid([first, second], "ff".parse().expect("parse absent prefix")),
            Err(NodeError::PrefixNotFound { .. })
        ));
    }

    fn test_node_identities(count: usize) -> Vec<QuicIdentity> {
        let mut issuer_params = CertificateParams::default();
        issuer_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        issuer_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let issuer = CertifiedIssuer::self_signed(
            issuer_params,
            KeyPair::generate().expect("generate test issuer key"),
        )
        .expect("generate test issuer");
        let trust_roots = issuer.pem();
        (0..count)
            .map(|_| {
                let key = KeyPair::generate().expect("generate test key");
                let mut params = CertificateParams::new(vec!["localhost".to_owned()])
                    .expect("construct test certificate parameters");
                params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
                params.extended_key_usages = vec![
                    ExtendedKeyUsagePurpose::ClientAuth,
                    ExtendedKeyUsagePurpose::ServerAuth,
                ];
                let certificate = params
                    .signed_by(&key, &issuer)
                    .expect("sign test certificate");
                QuicIdentity::new(
                    certificate.der().as_ref(),
                    format!("{}{}", certificate.pem(), trust_roots).into_bytes(),
                    key.serialize_pem().into_bytes(),
                    trust_roots.as_bytes().to_vec(),
                    "localhost",
                )
            })
            .collect()
    }

    async fn wait_for_publication(node: &Node, pid: Pid) {
        timeout(TEST_TIMEOUT, async {
            while node.nameserver().get(pid).await.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("process publication timed out");
    }

    async fn wait_for_withdrawal(node: &Node, pid: Pid) {
        timeout(TEST_TIMEOUT, async {
            while node.nameserver().get(pid).await.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("process withdrawal timed out");
    }

    async fn wait_for_parent(node: &Node, expected: Pid) {
        let mut status = node
            .subscribe_parent()
            .expect("node must have a configured parent");
        timeout(TEST_TIMEOUT, async {
            loop {
                if matches!(
                    *status.borrow(),
                    ParentManagerStatus::Connected { peer, .. } if peer == expected
                ) {
                    return;
                }
                status
                    .changed()
                    .await
                    .expect("parent manager stopped before connecting");
            }
        })
        .await
        .expect("parent connection timed out");
    }

    async fn assert_stream_round_trip(source: Arc<Node>, target: Arc<Node>) {
        let accepting = tokio::spawn({
            let source = source.clone();
            let target = target.clone();
            async move {
                let mut incoming = target.accept().await.expect("accept application stream");
                assert_eq!(incoming.source(), source.pid());
                let mut request = Vec::new();
                incoming
                    .stream_mut()
                    .recv_mut()
                    .read_to_end(&mut request)
                    .await
                    .expect("read request");
                assert_eq!(request, b"ping");
                incoming
                    .stream_mut()
                    .send_mut()
                    .write_all(b"pong")
                    .await
                    .expect("write response");
                incoming
                    .stream_mut()
                    .send_mut()
                    .finish()
                    .await
                    .expect("finish response");
            }
        });
        let mut stream = timeout(
            TEST_TIMEOUT,
            source.dial(target.pid(), ResolveConsistency::Cached),
        )
        .await
        .expect("application dial timed out")
        .expect("dial target");
        stream
            .send_mut()
            .write_all(b"ping")
            .await
            .expect("write request");
        stream.send_mut().finish().await.expect("finish request");
        let mut response = Vec::new();
        stream
            .recv_mut()
            .read_to_end(&mut response)
            .await
            .expect("read response");
        assert_eq!(response, b"pong");
        timeout(TEST_TIMEOUT, accepting)
            .await
            .expect("accept task timed out")
            .expect("accept task failed");
    }

    #[tokio::test]
    async fn child_publication_enables_application_stream_round_trip() {
        let network = InprocNetwork::new(NonZeroUsize::new(1024).expect("nonzero capacity"));
        let parent_socket = Arc::new(network.bind(1).expect("bind parent"));
        let child_socket = Arc::new(network.bind(2).expect("bind child"));
        let parent_address = parent_socket.local_addr().clone();
        let mut identities = test_node_identities(2).into_iter();
        let parent_identity = identities.next().expect("parent identity");
        let parent = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::new(
                parent_socket,
                parent_identity,
            )))
            .expect("create parent node"),
        );
        let child_identity = identities.next().expect("child identity");
        let child = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(child_socket, child_identity)).with_parent(
                    NamespaceConfig::try_new(
                        parent.pid(),
                        vec![ParentEndpoint::new(parent_address)],
                    )
                    .expect("construct parent config")
                    .with_retry_delay(Duration::from_millis(10)),
                ),
            )
            .expect("create child node"),
        );

        wait_for_publication(&parent, child.pid()).await;
        assert!(parent.router().get(child.pid()).is_some());
        assert_stream_round_trip(parent.clone(), child.clone()).await;

        child.shutdown();
        parent.shutdown();
        timeout(TEST_TIMEOUT, child.join())
            .await
            .expect("child shutdown timed out");
        timeout(TEST_TIMEOUT, parent.join())
            .await
            .expect("parent shutdown timed out");
    }

    #[tokio::test]
    async fn direct_udp_nodes_publish_and_exchange_streams() {
        let parent_socket = std::net::UdpSocket::bind("[::]:0").expect("bind parent UDP socket");
        let parent_port = parent_socket
            .local_addr()
            .expect("read parent UDP address")
            .port();
        let parent_address =
            std::net::SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, parent_port));
        let child_socket = std::net::UdpSocket::bind("[::]:0").expect("bind child UDP socket");
        let child_address = std::net::SocketAddr::from((
            std::net::Ipv6Addr::LOCALHOST,
            child_socket
                .local_addr()
                .expect("read child UDP address")
                .port(),
        ));
        let mut identities = test_node_identities(2).into_iter();
        let parent = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::direct_udp_advertised(
                parent_socket,
                parent_address,
                identities.next().expect("parent identity"),
            )))
            .expect("create parent node"),
        );
        let child = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::direct_udp_advertised(
                    child_socket,
                    child_address,
                    identities.next().expect("child identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_new(
                        parent.pid(),
                        vec![ParentEndpoint::new(UdpSocket::datagram_addr(
                            parent_address,
                        ))],
                    )
                    .expect("construct parent config")
                    .with_retry_delay(Duration::from_millis(10)),
                ),
            )
            .expect("create child node"),
        );

        wait_for_publication(&parent, child.pid()).await;
        assert_stream_round_trip(parent.clone(), child.clone()).await;
        let stats = parent.transport().io_stats();
        assert!(stats.transmit_calls > 0);
        assert!(stats.transmit_bytes > 0);
        assert!(stats.receive_calls > 0);
        assert!(stats.receive_bytes > 0);

        child.shutdown();
        parent.shutdown();
        timeout(TEST_TIMEOUT, child.join())
            .await
            .expect("child shutdown timed out");
        timeout(TEST_TIMEOUT, parent.join())
            .await
            .expect("parent shutdown timed out");
    }

    #[tokio::test]
    async fn duplicate_child_pid_reports_terminal_parent_rejection() {
        let network = InprocNetwork::new(NonZeroUsize::new(1024).expect("nonzero capacity"));
        let parent_socket = Arc::new(network.bind(21).expect("bind parent"));
        let first_socket = Arc::new(network.bind(22).expect("bind first child"));
        let duplicate_socket = Arc::new(network.bind(23).expect("bind duplicate child"));
        let parent_address = parent_socket.local_addr().clone();
        let mut identities = test_node_identities(2).into_iter();
        let parent = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::new(
                parent_socket,
                identities.next().expect("parent identity"),
            )))
            .expect("create parent node"),
        );
        let child_identity = identities.next().expect("child identity");
        let parent_config = || {
            NamespaceConfig::try_new(
                parent.pid(),
                vec![ParentEndpoint::new(parent_address.clone())],
            )
            .expect("construct parent config")
            .with_retry_delay(Duration::from_millis(10))
        };
        let first = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(first_socket, child_identity.clone()))
                    .with_parent(parent_config()),
            )
            .expect("create first child"),
        );
        wait_for_publication(&parent, first.pid()).await;

        let duplicate = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(duplicate_socket, child_identity))
                    .with_parent(parent_config()),
            )
            .expect("create duplicate child"),
        );
        assert_eq!(duplicate.pid(), first.pid());
        let mut status = duplicate
            .subscribe_parent()
            .expect("duplicate child must have a parent");
        let error = timeout(TEST_TIMEOUT, async {
            loop {
                if let ParentManagerStatus::Failed { error } = status.borrow().clone() {
                    return error;
                }
                status
                    .changed()
                    .await
                    .expect("parent manager stopped without a status");
            }
        })
        .await
        .expect("duplicate rejection timed out");
        assert!(matches!(
            error,
            ParentManagerError::Rejected {
                parent: rejected_parent,
                child: rejected_child,
                code: RejectCode::AlreadyLinked,
                ..
            } if rejected_parent == parent.pid() && rejected_child == duplicate.pid()
        ));

        duplicate.shutdown();
        first.shutdown();
        parent.shutdown();
        timeout(TEST_TIMEOUT, duplicate.join())
            .await
            .expect("duplicate child shutdown timed out");
        timeout(TEST_TIMEOUT, first.join())
            .await
            .expect("first child shutdown timed out");
        timeout(TEST_TIMEOUT, parent.join())
            .await
            .expect("parent shutdown timed out");
    }

    #[tokio::test]
    async fn child_can_discover_parent_identity_from_address() {
        let network = InprocNetwork::new(NonZeroUsize::new(1024).expect("nonzero capacity"));
        let parent_socket = Arc::new(network.bind(11).expect("bind parent"));
        let child_socket = Arc::new(network.bind(12).expect("bind child"));
        let parent_address = parent_socket.local_addr().clone();
        let mut identities = test_node_identities(2).into_iter();
        let parent = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::new(
                parent_socket,
                identities.next().expect("parent identity"),
            )))
            .expect("create parent node"),
        );
        let child = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    child_socket,
                    identities.next().expect("child identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_discover(vec![ParentEndpoint::new(parent_address)])
                        .expect("construct discovery config")
                        .with_retry_delay(Duration::from_millis(10)),
                ),
            )
            .expect("create child node"),
        );

        wait_for_publication(&parent, child.pid()).await;
        wait_for_parent(&child, parent.pid()).await;
        assert_stream_round_trip(parent.clone(), child.clone()).await;

        child.shutdown();
        parent.shutdown();
        timeout(TEST_TIMEOUT, child.join())
            .await
            .expect("child shutdown timed out");
        timeout(TEST_TIMEOUT, parent.join())
            .await
            .expect("parent shutdown timed out");
    }

    #[tokio::test]
    async fn rejected_tls_peer_does_not_stop_other_connections() {
        let network = InprocNetwork::new(NonZeroUsize::new(1024).expect("nonzero capacity"));
        let root_socket = Arc::new(network.bind(31).expect("bind root"));
        let rejected_socket = Arc::new(network.bind(32).expect("bind rejected child"));
        let accepted_socket = Arc::new(network.bind(33).expect("bind accepted child"));
        let root_address = root_socket.local_addr().clone();
        let mut trusted = test_node_identities(2).into_iter();
        let root = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::new(
                root_socket,
                trusted.next().expect("root identity"),
            )))
            .expect("create root node"),
        );
        let parent = || {
            NamespaceConfig::try_new(root.pid(), vec![ParentEndpoint::new(root_address.clone())])
                .expect("construct parent config")
                .with_retry_delay(Duration::from_millis(10))
        };
        let rejected = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    rejected_socket,
                    test_node_identities(1).remove(0),
                ))
                .with_parent(parent()),
            )
            .expect("create rejected child"),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(root.nameserver().get(rejected.pid()).await.is_none());

        let accepted = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    accepted_socket,
                    trusted.next().expect("accepted identity"),
                ))
                .with_parent(parent()),
            )
            .expect("create accepted child"),
        );
        wait_for_publication(&root, accepted.pid()).await;
        assert_stream_round_trip(root.clone(), accepted.clone()).await;

        rejected.shutdown();
        accepted.shutdown();
        root.shutdown();
        timeout(TEST_TIMEOUT, rejected.join())
            .await
            .expect("rejected child shutdown timed out");
        timeout(TEST_TIMEOUT, accepted.join())
            .await
            .expect("accepted child shutdown timed out");
        timeout(TEST_TIMEOUT, root.join())
            .await
            .expect("root shutdown timed out");
    }

    #[tokio::test]
    async fn custom_link_local_streams_coexist_with_nameserver_traffic() {
        let network = InprocNetwork::new(NonZeroUsize::new(1024).expect("nonzero capacity"));
        let parent_socket = Arc::new(network.bind(3).expect("bind parent"));
        let child_socket = Arc::new(network.bind(4).expect("bind child"));
        let parent_address = parent_socket.local_addr().clone();
        let mut identities = test_node_identities(2).into_iter();
        let (parent_sessions, mut parent_session_rx) = tokio::sync::mpsc::unbounded_channel();
        let parent_handler = move |context: LinkContext, mut stream: Stream| {
            let parent_sessions = parent_sessions.clone();
            async move {
                let mut request = Vec::new();
                stream
                    .recv_mut()
                    .read_to_end(&mut request)
                    .await
                    .expect("read custom request");
                assert_eq!(request, b"ping");
                stream
                    .send_mut()
                    .write_all(b"pong")
                    .await
                    .expect("write custom response");
                stream
                    .send_mut()
                    .finish()
                    .await
                    .expect("finish custom response");
                parent_sessions
                    .send(context)
                    .expect("record parent session");
            }
        };
        let parent = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    parent_socket,
                    identities.next().expect("parent identity"),
                ))
                .with_link_protocol(TEST_LINK_PROTOCOL, parent_handler),
            )
            .expect("create parent node"),
        );
        let parent_pid = parent.pid();
        let (child_sessions, mut child_session_rx) = tokio::sync::mpsc::unbounded_channel();
        let child_handler = move |context: LinkContext, mut stream: Stream| {
            let child_sessions = child_sessions.clone();
            async move {
                stream
                    .send_mut()
                    .write_all(b"ping")
                    .await
                    .expect("write custom request");
                stream
                    .send_mut()
                    .finish()
                    .await
                    .expect("finish custom request");
                let mut response = Vec::new();
                stream
                    .recv_mut()
                    .read_to_end(&mut response)
                    .await
                    .expect("read custom response");
                assert_eq!(response, b"pong");
                child_sessions.send(context).expect("record child session");
            }
        };
        let unsupported_sessions = Arc::new(AtomicUsize::new(0));
        let unsupported_handler = {
            let unsupported_sessions = unsupported_sessions.clone();
            move |_: LinkContext, _: Stream| {
                let unsupported_sessions = unsupported_sessions.clone();
                async move {
                    unsupported_sessions.fetch_add(1, Ordering::Relaxed);
                }
            }
        };
        let child = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(
                    child_socket,
                    identities.next().expect("child identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_new(
                        parent.pid(),
                        vec![ParentEndpoint::new(parent_address.clone())],
                    )
                    .expect("construct parent config")
                    .with_retry_delay(Duration::from_millis(10)),
                )
                .with_link_protocol(TEST_LINK_PROTOCOL, child_handler)
                .with_link_protocol(UNKNOWN_LINK_PROTOCOL, unsupported_handler),
            )
            .expect("create child node"),
        );

        wait_for_publication(&parent, child.pid()).await;
        let child_pid = child.pid();
        let parent_context = timeout(TEST_TIMEOUT, parent_session_rx.recv())
            .await
            .expect("parent protocol session timed out")
            .expect("parent protocol manager stopped");
        let child_context = timeout(TEST_TIMEOUT, child_session_rx.recv())
            .await
            .expect("child protocol session timed out")
            .expect("child protocol manager stopped");
        assert_eq!(parent_context.link(), child_context.link());
        assert_eq!(parent_context.peer(), child_pid);
        assert_eq!(parent_context.side(), chrysalis_core::LinkSide::Child);
        assert_eq!(child_context.peer(), parent_pid);
        assert_eq!(child_context.side(), chrysalis_core::LinkSide::Parent);
        let restarted_parent = timeout(TEST_TIMEOUT, parent_session_rx.recv())
            .await
            .expect("restarted parent protocol session timed out")
            .expect("parent protocol manager stopped");
        let restarted_child = timeout(TEST_TIMEOUT, child_session_rx.recv())
            .await
            .expect("restarted child protocol session timed out")
            .expect("child protocol manager stopped");
        assert_eq!(restarted_parent.link(), parent_context.link());
        assert_eq!(restarted_child.link(), child_context.link());
        assert_eq!(unsupported_sessions.load(Ordering::Relaxed), 0);

        assert_stream_round_trip(parent.clone(), child.clone()).await;
        child.shutdown();
        parent.shutdown();
        timeout(TEST_TIMEOUT, child.join())
            .await
            .expect("child shutdown timed out");
        timeout(TEST_TIMEOUT, parent.join())
            .await
            .expect("parent shutdown timed out");
    }

    #[tokio::test]
    async fn link_local_protocol_registration_rejects_duplicates_and_nameserver_id() {
        let network = InprocNetwork::new(NonZeroUsize::new(16).expect("nonzero capacity"));
        let mut identities = test_node_identities(2).into_iter();
        let duplicate = Node::create(
            NodeConfig::new(TransportConfig::new(
                Arc::new(network.bind(5).expect("bind duplicate node")),
                identities.next().expect("duplicate identity"),
            ))
            .with_link_protocol(TEST_LINK_PROTOCOL, |_, _| async {})
            .with_link_protocol(TEST_LINK_PROTOCOL, |_, _| async {}),
        );
        assert!(matches!(
            duplicate,
            Err(NodeError::LinkLocal(LinkLocalError::DuplicateProtocol(id)))
                if id == TEST_LINK_PROTOCOL
        ));

        let reserved = Node::create(
            NodeConfig::new(TransportConfig::new(
                Arc::new(network.bind(6).expect("bind reserved node")),
                identities.next().expect("reserved identity"),
            ))
            .with_link_protocol(NAMESERVER_LINK_PROTOCOL, |_, _| async {}),
        );
        assert!(matches!(
            reserved,
            Err(NodeError::LinkLocal(LinkLocalError::DuplicateProtocol(id)))
                if id == NAMESERVER_LINK_PROTOCOL
        ));
    }

    #[tokio::test]
    async fn application_stream_round_trips_through_gateway_node() {
        let network = InprocNetwork::new(NonZeroUsize::new(1024).expect("nonzero capacity"));
        let root_socket = Arc::new(network.bind(10).expect("bind root"));
        let gateway_socket = Arc::new(network.bind(11).expect("bind gateway"));
        let leaf_socket = Arc::new(network.bind(12).expect("bind leaf"));
        let root_address = root_socket.local_addr().clone();
        let gateway_address = gateway_socket.local_addr().clone();
        let leaf_address = leaf_socket.local_addr().clone();
        let mut identities = test_node_identities(3).into_iter();

        let root_identity = identities.next().expect("root identity");
        let root = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::new(
                root_socket,
                root_identity,
            )))
            .expect("create root node"),
        );
        let gateway_identity = identities.next().expect("gateway identity");
        let gateway = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(gateway_socket, gateway_identity))
                    .with_parent(
                        NamespaceConfig::try_new(
                            root.pid(),
                            vec![ParentEndpoint::new(root_address)],
                        )
                        .expect("construct root config")
                        .with_retry_delay(Duration::from_millis(10)),
                    ),
            )
            .expect("create gateway node"),
        );
        let leaf_identity = identities.next().expect("leaf identity");
        let leaf = Arc::new(
            Node::create(
                NodeConfig::new(TransportConfig::new(leaf_socket, leaf_identity)).with_parent(
                    NamespaceConfig::try_new(
                        gateway.pid(),
                        vec![ParentEndpoint::new(gateway_address.clone())],
                    )
                    .expect("construct gateway config")
                    .with_retry_delay(Duration::from_millis(10)),
                ),
            )
            .expect("create leaf node"),
        );

        wait_for_publication(&root, leaf.pid()).await;
        wait_for_parent(&gateway, root.pid()).await;
        wait_for_parent(&leaf, gateway.pid()).await;
        assert!(root.router().get(leaf.pid()).is_some());
        assert!(gateway.router().get(leaf.pid()).is_some());
        assert!(gateway.router().default_route().is_some());
        assert!(leaf.router().default_route().is_some());

        let Resolution::Found {
            entry: cached_leaf, ..
        } = leaf
            .resolve(leaf.pid(), ResolveConsistency::Cached)
            .await
            .expect("resolve leaf locally")
        else {
            panic!("leaf must resolve from its local namespace");
        };
        assert!(
            cached_leaf
                .locators
                .iter()
                .all(|locator| locator.address == leaf_address)
        );
        let Resolution::Found {
            entry: refreshed_leaf,
            ..
        } = leaf
            .resolve(leaf.pid(), ResolveConsistency::Refresh)
            .await
            .expect("refresh leaf through parent")
        else {
            panic!("parent must resolve the published leaf");
        };
        assert!(
            refreshed_leaf
                .locators
                .iter()
                .all(|locator| locator.address == gateway_address)
        );

        let cached = leaf
            .enumerate(ResolveConsistency::Cached)
            .await
            .expect("enumerate leaf cache");
        assert_eq!(
            cached.iter().map(|entry| entry.pid).collect::<Vec<_>>(),
            vec![leaf.pid()]
        );
        let refreshed = leaf
            .enumerate(ResolveConsistency::Refresh)
            .await
            .expect("enumerate through root");
        let mut actual_pids = refreshed.iter().map(|entry| entry.pid).collect::<Vec<_>>();
        actual_pids.sort_unstable();
        let mut expected_pids = vec![root.pid(), gateway.pid(), leaf.pid()];
        expected_pids.sort_unstable();
        assert_eq!(actual_pids, expected_pids);
        assert!(refreshed.iter().all(|entry| {
            entry
                .locators
                .iter()
                .all(|locator| locator.address == gateway_address)
        }));

        assert_stream_round_trip(root.clone(), leaf.clone()).await;
        assert_stream_round_trip(leaf.clone(), root.clone()).await;

        leaf.shutdown();
        gateway.shutdown();
        root.shutdown();
        timeout(TEST_TIMEOUT, leaf.join())
            .await
            .expect("leaf shutdown timed out");
        timeout(TEST_TIMEOUT, gateway.join())
            .await
            .expect("gateway shutdown timed out");
        timeout(TEST_TIMEOUT, root.join())
            .await
            .expect("root shutdown timed out");
    }

    #[tokio::test]
    async fn short_lived_children_are_withdrawn_after_clean_shutdown() {
        let network = InprocNetwork::new(NonZeroUsize::new(1024).expect("nonzero capacity"));
        let root_socket = Arc::new(network.bind(20).expect("bind root"));
        let root_address = root_socket.local_addr().clone();
        let mut identities = test_node_identities(6).into_iter();
        let root = Arc::new(
            Node::create(NodeConfig::new(TransportConfig::new(
                root_socket,
                identities.next().expect("root identity"),
            )))
            .expect("create root node"),
        );

        for address in 21..26 {
            let child_socket = Arc::new(network.bind(address).expect("bind child"));
            let child = Node::create(
                NodeConfig::new(TransportConfig::new(
                    child_socket,
                    identities.next().expect("child identity"),
                ))
                .with_parent(
                    NamespaceConfig::try_new(
                        root.pid(),
                        vec![ParentEndpoint::new(root_address.clone())],
                    )
                    .expect("construct root config")
                    .with_retry_delay(Duration::from_millis(10)),
                ),
            )
            .expect("create child node");
            wait_for_publication(&root, child.pid()).await;
            let child_pid = child.pid();
            child.shutdown();
            timeout(TEST_TIMEOUT, child.join())
                .await
                .expect("child shutdown timed out");
            wait_for_withdrawal(&root, child_pid).await;
        }

        assert_eq!(
            root.enumerate(ResolveConsistency::Cached)
                .await
                .expect("enumerate root")
                .iter()
                .map(|entry| entry.pid)
                .collect::<Vec<_>>(),
            vec![root.pid()]
        );
        root.shutdown();
        timeout(TEST_TIMEOUT, root.join())
            .await
            .expect("root shutdown timed out");
    }
}
