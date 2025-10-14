/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! This module defines a proc allocator interface as well as a multi-process
//! (local) allocator, [`ProcessAllocator`].

pub mod local;
pub(crate) mod logtailer;
pub mod process;
pub mod remoteprocess;
pub mod sim;

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use hyperactor::ActorRef;
use hyperactor::Named;
use hyperactor::ProcId;
use hyperactor::RemoteMessage;
use hyperactor::WorldId;
use hyperactor::channel;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::ChannelRx;
use hyperactor::channel::ChannelTransport;
use hyperactor::channel::MetaTlsAddr;
use hyperactor::config;
pub use local::LocalAlloc;
pub use local::LocalAllocator;
use mockall::predicate::*;
use mockall::*;
use ndslice::Shape;
use ndslice::Slice;
use ndslice::view::Extent;
use ndslice::view::Point;
pub use process::ProcessAlloc;
pub use process::ProcessAllocator;
use serde::Deserialize;
use serde::Serialize;
use strum::AsRefStr;

use crate::alloc::test_utils::MockAllocWrapper;
use crate::assign::Ranks;
use crate::proc_mesh::mesh_agent::ProcMeshAgent;
use crate::shortuuid::ShortUuid;

/// Errors that occur during allocation operations.
#[derive(Debug, thiserror::Error)]
pub enum AllocatorError {
    #[error("incomplete allocation; expected: {0}")]
    Incomplete(Extent),

    /// The requested shape is too large for the allocator.
    #[error("not enough resources; requested: {requested}, available: {available}")]
    NotEnoughResources { requested: Extent, available: usize },

    /// An uncategorized error from an underlying system.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Constraints on the allocation.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AllocConstraints {
    /// Aribitrary name/value pairs that are interpreted by individual
    /// allocators to control allocation process.
    pub match_labels: HashMap<String, String>,
}

/// A specification (desired state) of an alloc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocSpec {
    /// The requested extent of the alloc.
    // We currently assume that this shape is dense.
    // This should be validated, or even enforced by
    // way of types.
    pub extent: Extent,

    /// Constraints on the allocation.
    pub constraints: AllocConstraints,

    /// If specified, return procs using direct addressing with
    /// the provided proc name.
    pub proc_name: Option<String>,

    /// The transport to use for the procs in this alloc.
    pub transport: ChannelTransport,
}

/// The core allocator trait, implemented by all allocators.
#[automock(type Alloc=MockAllocWrapper;)]
#[async_trait]
pub trait Allocator {
    /// The type of [`Alloc`] produced by this allocator.
    type Alloc: Alloc;

    /// Create a new allocation. The allocation itself is generally
    /// returned immediately (after validating parameters, etc.);
    /// the caller is expected to respond to allocation events as
    /// the underlying procs are incrementally allocated.
    async fn allocate(&mut self, spec: AllocSpec) -> Result<Self::Alloc, AllocatorError>;
}

/// A proc's status. A proc can only monotonically move from
/// `Created` to `Running` to `Stopped`.
#[derive(
    Clone,
    Debug,
    PartialEq,
    EnumAsInner,
    Serialize,
    Deserialize,
    AsRefStr,
    Named
)]
pub enum ProcState {
    /// A proc was added to the alloc.
    Created {
        /// A key to uniquely identify a created proc. The key is used again
        /// to identify the created proc as Running.
        create_key: ShortUuid,
        /// Its assigned point (in the alloc's extent).
        point: Point,
        /// The system process ID of the created child process.
        pid: u32,
    },
    /// A proc was started.
    Running {
        /// The key used to identify the created proc.
        create_key: ShortUuid,
        /// The proc's assigned ID.
        proc_id: ProcId,
        /// Reference to this proc's mesh agent. In the future, we'll reserve a
        /// 'well known' PID (0) for this purpose.
        mesh_agent: ActorRef<ProcMeshAgent>,
        /// The address of this proc. The endpoint of this address is
        /// the proc's mailbox, which accepts [`hyperactor::mailbox::MessageEnvelope`]s.
        addr: ChannelAddr,
    },
    /// A proc was stopped.
    Stopped {
        create_key: ShortUuid,
        reason: ProcStopReason,
    },
    /// Allocation process encountered an irrecoverable error. Depending on the
    /// implementation, the allocation process may continue transiently and calls
    /// to next() may return some events. But eventually the allocation will not
    /// be complete. Callers can use the `description` to determine the reason for
    /// the failure.
    /// Allocation can then be cleaned up by calling `stop()`` on the `Alloc` and
    /// drain the iterator for clean shutdown.
    Failed {
        /// The world ID of the failed alloc.
        ///
        /// TODO: this is not meaningful with direct addressing.
        world_id: WorldId,
        /// A description of the failure.
        description: String,
    },
}

impl fmt::Display for ProcState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcState::Created {
                create_key,
                point,
                pid,
            } => {
                write!(f, "{}: created at ({}) with PID {}", create_key, point, pid)
            }
            ProcState::Running { proc_id, addr, .. } => {
                write!(f, "{}: running at {}", proc_id, addr)
            }
            ProcState::Stopped { create_key, reason } => {
                write!(f, "{}: stopped: {}", create_key, reason)
            }
            ProcState::Failed {
                description,
                world_id,
            } => {
                write!(f, "{}: failed: {}", world_id, description)
            }
        }
    }
}

/// The reason a proc stopped.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnumAsInner)]
pub enum ProcStopReason {
    /// The proc stopped gracefully, e.g., with exit code 0.
    Stopped,
    /// The proc exited with the provided error code and stderr
    Exited(i32, String),
    /// The proc was killed. The signal number is indicated;
    /// the flags determines whether there was a core dump.
    Killed(i32, bool),
    /// The proc failed to respond to a watchdog request within a timeout.
    Watchdog,
    /// The host running the proc failed to respond to a watchdog request
    /// within a timeout.
    HostWatchdog,
    /// The proc failed for an unknown reason.
    Unknown,
}

impl fmt::Display for ProcStopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => write!(f, "stopped"),
            Self::Exited(code, stderr) => {
                if stderr.is_empty() {
                    write!(f, "exited with code {}", code)
                } else {
                    write!(f, "exited with code {}: {}", code, stderr)
                }
            }
            Self::Killed(signal, dumped) => {
                write!(f, "killed with signal {} (core dumped={})", signal, dumped)
            }
            Self::Watchdog => write!(f, "proc watchdog failure"),
            Self::HostWatchdog => write!(f, "host watchdog failure"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// An alloc is a specific allocation, returned by an [`Allocator`].
#[automock]
#[async_trait]
pub trait Alloc {
    /// Return the next proc event. `None` indicates that there are
    /// no more events, and that the alloc is stopped.
    async fn next(&mut self) -> Option<ProcState>;

    /// The spec against which this alloc is executing.
    fn spec(&self) -> &AllocSpec;

    /// The shape of the alloc.
    fn extent(&self) -> &Extent;

    /// The shape of the alloc. (Deprecated.)
    fn shape(&self) -> Shape {
        let slice = Slice::new_row_major(self.extent().sizes());
        Shape::new(self.extent().labels().to_vec(), slice).unwrap()
    }

    /// The world id of this alloc, uniquely identifying the alloc.
    /// Note: This will be removed in favor of a different naming scheme,
    /// once we exise "worlds" from hyperactor core.
    fn world_id(&self) -> &WorldId;

    /// The channel transport used the procs in this alloc.
    fn transport(&self) -> ChannelTransport {
        self.spec().transport.clone()
    }

    /// Stop this alloc, shutting down all of its procs. A clean
    /// shutdown should result in Stop events from all allocs,
    /// followed by the end of the event stream.
    async fn stop(&mut self) -> Result<(), AllocatorError>;

    /// Stop this alloc and wait for all procs to stop. Call will
    /// block until all ProcState events have been drained.
    async fn stop_and_wait(&mut self) -> Result<(), AllocatorError> {
        self.stop().await?;
        while let Some(event) = self.next().await {
            tracing::debug!("drained event: {:?}", event);
        }
        Ok(())
    }

    /// Returns whether the alloc is a local alloc: that is, its procs are
    /// not independent processes, but just threads in the selfsame process.
    fn is_local(&self) -> bool {
        false
    }

    fn router_addr(&self) -> AllocChannelAddr {
        AllocChannelAddr(ChannelAddr::any(self.transport()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AllocatedProc {
    pub create_key: ShortUuid,
    pub proc_id: ProcId,
    pub addr: ChannelAddr,
    pub mesh_agent: ActorRef<ProcMeshAgent>,
}

impl fmt::Display for AllocatedProc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AllocatedProc {{ create_key: {}, proc_id: {}, addr: {}, mesh_agent: {} }}",
            self.create_key, self.proc_id, self.addr, self.mesh_agent
        )
    }
}

#[async_trait]
pub(crate) trait AllocExt {
    /// Perform initial allocation, consuming events until the alloc is fully
    /// running. Returns the ranked procs.
    async fn initialize(&mut self) -> Result<Vec<AllocatedProc>, AllocatorError>;
}

#[async_trait]
impl<A: ?Sized + Send + Alloc> AllocExt for A {
    async fn initialize(&mut self) -> Result<Vec<AllocatedProc>, AllocatorError> {
        // We wait for the full allocation to be running before returning the mesh.
        let shape = self.shape().clone();

        let mut created = Ranks::new(shape.slice().len());
        let mut running = Ranks::new(shape.slice().len());

        while !running.is_full() {
            let Some(state) = self.next().await else {
                // Alloc finished before it was fully allocated.
                return Err(AllocatorError::Incomplete(self.extent().clone()));
            };

            let name = state.arm().unwrap_or("unknown");

            match state {
                ProcState::Created {
                    create_key, point, ..
                } => {
                    let rank = point.rank();
                    if let Some(old_create_key) = created.insert(rank, create_key.clone()) {
                        tracing::warn!(
                            "rank {rank} reassigned from {old_create_key} to {create_key}"
                        );
                    }
                    tracing::info!(
                        name = name,
                        rank = rank,
                        "proc with create key {}, rank {}: created",
                        create_key,
                        rank
                    );
                    // tracing::info!("created: {} rank {}: created", create_key, rank);
                }
                ProcState::Running {
                    create_key,
                    proc_id,
                    mesh_agent,
                    addr,
                } => {
                    let Some(rank) = created.rank(&create_key) else {
                        tracing::warn!(
                            name = name,
                            "proc id {proc_id} with create key {create_key} \
                            is running, but was not created"
                        );
                        continue;
                    };

                    let allocated_proc = AllocatedProc {
                        create_key,
                        proc_id: proc_id.clone(),
                        addr: addr.clone(),
                        mesh_agent: mesh_agent.clone(),
                    };
                    if let Some(old_allocated_proc) = running.insert(*rank, allocated_proc.clone())
                    {
                        tracing::warn!(
                            name = name,
                            "duplicate running notifications for {rank}: \
                            old:{old_allocated_proc}; \
                            new:{allocated_proc}"
                        )
                    }
                    tracing::info!(
                        name = name,
                        "proc {} rank {}: running at addr:{addr} mesh_agent:{mesh_agent}",
                        proc_id,
                        rank
                    );
                }
                // TODO: We should push responsibility to the allocator, which
                // can choose to either provide a new proc or emit a
                // ProcState::Failed to fail the whole allocation.
                ProcState::Stopped { create_key, reason } => {
                    tracing::error!(
                        name = name,
                        "allocation failed for proc with create key {}: {}",
                        create_key,
                        reason
                    );
                    return Err(AllocatorError::Other(anyhow::Error::msg(reason)));
                }
                ProcState::Failed {
                    world_id,
                    description,
                } => {
                    tracing::error!(
                        name = name,
                        "allocation failed for world {}: {}",
                        world_id,
                        description
                    );
                    return Err(AllocatorError::Other(anyhow::Error::msg(description)));
                }
            }
        }

        // We collect all the ranks at this point of completion, so that we can
        // avoid holding Rcs across awaits.
        Ok(running.into_iter().map(Option::unwrap).collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocChannelAddr(ChannelAddr);

impl AllocChannelAddr {
    pub(crate) fn new(addr: ChannelAddr) -> Self {
        AllocChannelAddr(addr)
    }

    pub(crate) fn from_serve(serve_addr: &ChannelAddr) -> Self {
        let new_addr = match serve_addr {
            ChannelAddr::Tcp(socket) => {
                let mut new_socket = socket.clone();
                new_socket.set_port(0);
                ChannelAddr::Tcp(new_socket)
            }
            ChannelAddr::MetaTls(MetaTlsAddr::Host { hostname, port: _ }) => {
                ChannelAddr::MetaTls(MetaTlsAddr::Host {
                    hostname: hostname.clone(),
                    port: 0,
                })
            }
            ChannelAddr::MetaTls(MetaTlsAddr::Socket(socket)) => {
                let mut new_socket = socket.clone();
                new_socket.set_port(0);
                ChannelAddr::MetaTls(MetaTlsAddr::Socket(new_socket))
            }
            _ => ChannelAddr::any(serve_addr.transport()),
        };
        AllocChannelAddr(new_addr)
    }

    pub(crate) fn serve_with_config<M: RemoteMessage>(
        self,
    ) -> anyhow::Result<(ChannelAddr, ChannelRx<M>)> {
        fn set_as_inaddr_any(original: &mut SocketAddr) {
            let inaddr_any: IpAddr = match &original {
                SocketAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
                SocketAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
            };
            original.set_ip(inaddr_any);
        }

        let use_inaddr_any = config::global::get(config::REMOTE_ALLOC_BIND_TO_INADDR_ANY);
        let mut bind_to = self.0;
        let mut original_ip: Option<IpAddr> = None;
        match &mut bind_to {
            ChannelAddr::Tcp(socket) => {
                original_ip = Some(socket.ip().clone());
                if use_inaddr_any {
                    set_as_inaddr_any(socket);
                    tracing::debug!("binding {} to INADDR_ANY", original_ip.as_ref().unwrap(),);
                }
                if socket.port() == 0 {
                    socket.set_port(next_allowed_port(socket.ip().clone())?);
                }
            }
            ChannelAddr::MetaTls(MetaTlsAddr::Socket(socket)) => {
                original_ip = Some(socket.ip().clone());
                if use_inaddr_any {
                    set_as_inaddr_any(socket);
                }
                if socket.port() == 0 {
                    socket.set_port(next_allowed_port(socket.ip().clone())?);
                }
            }
            ChannelAddr::MetaTls(MetaTlsAddr::Host { hostname: _, port }) => {
                if use_inaddr_any {
                    anyhow::bail!(
                        "cannot bind to INADDR_ANY for MetaTlsAddr::Host; addr: {}",
                        bind_to,
                    );
                }
                if *port == 0 {
                    *port = next_allowed_port(Ipv6Addr::UNSPECIFIED.into())?;
                }
            }
            _ => {
                if use_inaddr_any {
                    tracing::debug!(
                        "can only bind to INADDR_ANY for TCP and MetaTlsAddr::Socket; got transport {}, addr {}",
                        bind_to.transport(),
                        bind_to
                    );
                }
            }
        };

        let (mut bound, rx) = channel::serve(bind_to)?;

        // Restore the original IP address if we used INADDR_ANY.
        match &mut bound {
            ChannelAddr::Tcp(socket) => {
                if use_inaddr_any {
                    socket.set_ip(original_ip.unwrap());
                }
            }
            ChannelAddr::MetaTls(MetaTlsAddr::Socket(socket)) => {
                if use_inaddr_any {
                    socket.set_ip(original_ip.unwrap());
                }
            }
            _ => (),
        }

        Ok((bound, rx))
    }
}

enum AllowedPorts {
    Config { range: Vec<u16>, next: AtomicUsize },
    Any,
}

impl AllowedPorts {
    fn next(&self, ip: IpAddr) -> anyhow::Result<u16> {
        match self {
            Self::Config { range, next } => {
                let mut count = 0;
                loop {
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    count += 1;
                    // Since we do not have a good way to put release ports back to the list,
                    // we opportunistically hope ports previously took already released. If
                    // not, we'll just see error when binding to it later. This
                    // is not much different from raising error here.
                    let port = range.get(i % range.len()).cloned().unwrap();
                    let socket = SocketAddr::new(ip, port);
                    if TcpListener::bind(socket).is_ok() {
                        tracing::debug!("taking port {port} from the allowed list",);
                        return Ok(port);
                    }
                    if count == range.len() {
                        anyhow::bail!(
                            "fail to find a port because all ports in the allowed list are already bound"
                        );
                    }
                }
            }
            Self::Any => Ok(0),
        }
    }
}

static ALLOWED_PORTS: OnceLock<Mutex<AllowedPorts>> = OnceLock::new();
fn next_allowed_port(ip: IpAddr) -> anyhow::Result<u16> {
    let mutex = ALLOWED_PORTS.get_or_init(|| {
        let ports = match config::global::try_get_cloned(config::REMOTE_ALLOC_ALLOWED_PORT_RANGE) {
            Some(input) => {
                let (start, end) = input.split_once("-").unwrap();
                let start = start.parse::<u16>().unwrap();
                let end = end.parse::<u16>().unwrap();
                assert!(start <= end);
                AllowedPorts::Config {
                    range: (start..=end).collect(),
                    next: AtomicUsize::new(0),
                }
            }
            None => AllowedPorts::Any,
        };
        Mutex::new(ports)
    });
    mutex.lock().unwrap().next(ip)
}

pub mod test_utils {
    use std::time::Duration;

    use hyperactor::Actor;
    use hyperactor::Context;
    use hyperactor::Handler;
    use hyperactor::Named;
    use libc::atexit;
    use tokio::sync::broadcast::Receiver;
    use tokio::sync::broadcast::Sender;

    use super::*;

    extern "C" fn exit_handler() {
        loop {
            #[allow(clippy::disallowed_methods)]
            std::thread::sleep(Duration::from_secs(60));
        }
    }

    // This can't be defined under a `#[cfg(test)]` because there needs to
    // be an entry in the spawnable actor registry in the executable
    // 'hyperactor_mesh_test_bootstrap' for the `tests::process` actor
    // mesh test suite.
    #[derive(Debug, Default, Actor)]
    #[hyperactor::export(
        spawn = true,
        handlers = [
            Wait
        ],
    )]
    pub struct TestActor;

    #[derive(Debug, Serialize, Deserialize, Named, Clone)]
    pub struct Wait;

    #[async_trait]
    impl Handler<Wait> for TestActor {
        async fn handle(&mut self, _: &Context<Self>, _: Wait) -> Result<(), anyhow::Error> {
            // SAFETY:
            // This is in order to simulate a process in tests that never exits.
            unsafe {
                atexit(exit_handler);
            }
            Ok(())
        }
    }

    /// Test wrapper around MockAlloc to allow us to block next() calls since
    /// mockall doesn't support returning futures.
    pub struct MockAllocWrapper {
        pub alloc: MockAlloc,
        pub block_next_after: usize,
        notify_tx: Sender<()>,
        notify_rx: Receiver<()>,
        next_unblocked: bool,
    }

    impl MockAllocWrapper {
        pub fn new(alloc: MockAlloc) -> Self {
            Self::new_block_next(alloc, usize::MAX)
        }

        pub fn new_block_next(alloc: MockAlloc, count: usize) -> Self {
            let (tx, rx) = tokio::sync::broadcast::channel(1);
            Self {
                alloc,
                block_next_after: count,
                notify_tx: tx,
                notify_rx: rx,
                next_unblocked: false,
            }
        }

        pub fn notify_tx(&self) -> Sender<()> {
            self.notify_tx.clone()
        }
    }

    #[async_trait]
    impl Alloc for MockAllocWrapper {
        async fn next(&mut self) -> Option<ProcState> {
            match self.block_next_after {
                0 => {
                    if !self.next_unblocked {
                        self.notify_rx.recv().await.unwrap();
                        self.next_unblocked = true;
                    }
                }
                1.. => {
                    self.block_next_after -= 1;
                }
            }

            self.alloc.next().await
        }

        fn spec(&self) -> &AllocSpec {
            self.alloc.spec()
        }

        fn extent(&self) -> &Extent {
            self.alloc.extent()
        }

        fn world_id(&self) -> &WorldId {
            self.alloc.world_id()
        }

        async fn stop(&mut self) -> Result<(), AllocatorError> {
            self.alloc.stop().await
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use core::panic;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::time::Duration;

    use hyperactor::Instance;
    use hyperactor::actor::remote::Remote;
    use hyperactor::channel;
    use hyperactor::context;
    use hyperactor::mailbox;
    use hyperactor::mailbox::BoxedMailboxSender;
    use hyperactor::mailbox::DialMailboxRouter;
    use hyperactor::mailbox::IntoBoxedMailboxSender;
    use hyperactor::mailbox::MailboxServer;
    use hyperactor::mailbox::UndeliverableMailboxSender;
    use hyperactor::proc::Proc;
    use hyperactor::reference::Reference;
    use ndslice::extent;
    use tokio::process::Command;

    use super::*;
    use crate::alloc::test_utils::TestActor;
    use crate::alloc::test_utils::Wait;
    use crate::proc_mesh::default_transport;
    use crate::proc_mesh::mesh_agent::GspawnResult;
    use crate::proc_mesh::mesh_agent::MeshAgentMessageClient;

    #[macro_export]
    macro_rules! alloc_test_suite {
        ($allocator:expr) => {
            #[tokio::test]
            async fn test_allocator_basic() {
                $crate::alloc::testing::test_allocator_basic($allocator).await;
            }
        };
    }

    pub(crate) async fn test_allocator_basic(mut allocator: impl Allocator) {
        let extent = extent!(replica = 4);
        let mut alloc = allocator
            .allocate(AllocSpec {
                extent: extent.clone(),
                constraints: Default::default(),
                proc_name: None,
                transport: default_transport(),
            })
            .await
            .unwrap();

        // Get everything up into running state. We require that we get
        // procs 0..4.
        let mut procs = HashMap::new();
        let mut created = HashMap::new();
        let mut running = HashSet::new();
        while running.len() != 4 {
            match alloc.next().await.unwrap() {
                ProcState::Created {
                    create_key, point, ..
                } => {
                    created.insert(create_key, point);
                }
                ProcState::Running {
                    create_key,
                    proc_id,
                    ..
                } => {
                    assert!(running.insert(create_key.clone()));
                    procs.insert(proc_id, created.remove(&create_key).unwrap());
                }
                event => panic!("unexpected event: {:?}", event),
            }
        }

        // We should have complete coverage of all points.
        let points: HashSet<_> = procs.values().collect();
        for x in 0..4 {
            assert!(points.contains(&extent.point(vec![x]).unwrap()));
        }

        // Every proc should belong to the same "world" (alloc).
        let worlds: HashSet<_> = procs.keys().map(|proc_id| proc_id.world_id()).collect();
        assert_eq!(worlds.len(), 1);

        // Now, stop the alloc and make sure it shuts down cleanly.

        alloc.stop().await.unwrap();
        let mut stopped = HashSet::new();
        while let Some(ProcState::Stopped {
            create_key, reason, ..
        }) = alloc.next().await
        {
            assert_eq!(reason, ProcStopReason::Stopped);
            stopped.insert(create_key);
        }
        assert!(alloc.next().await.is_none());
        assert_eq!(stopped, running);
    }

    async fn spawn_proc(
        transport: ChannelTransport,
    ) -> (DialMailboxRouter, Instance<()>, Proc, ChannelAddr) {
        let (router_channel_addr, router_rx) =
            channel::serve(ChannelAddr::any(transport.clone())).unwrap();
        let router =
            DialMailboxRouter::new_with_default((UndeliverableMailboxSender {}).into_boxed());
        router.clone().serve(router_rx);

        let client_proc_id = ProcId::Ranked(WorldId("test_stuck".to_string()), 0);
        let (client_proc_addr, client_rx) = channel::serve(ChannelAddr::any(transport)).unwrap();
        let client_proc = Proc::new(
            client_proc_id.clone(),
            BoxedMailboxSender::new(router.clone()),
        );
        client_proc.clone().serve(client_rx);
        router.bind(client_proc_id.clone().into(), client_proc_addr);
        (
            router,
            client_proc.instance("test_proc").unwrap().0,
            client_proc,
            router_channel_addr,
        )
    }

    async fn spawn_test_actor(
        rank: usize,
        client_proc: &Proc,
        cx: &impl context::Actor,
        router_channel_addr: ChannelAddr,
        mesh_agent: ActorRef<ProcMeshAgent>,
    ) -> ActorRef<TestActor> {
        let (supervisor, _supervisor_handle) = client_proc.instance("supervisor").unwrap();
        let (supervison_port, _) = supervisor.open_port();
        let (config_handle, _) = cx.mailbox().open_port();
        mesh_agent
            .configure(
                cx,
                rank,
                router_channel_addr,
                Some(supervison_port.bind()),
                HashMap::new(),
                config_handle.bind(),
                false,
            )
            .await
            .unwrap();
        let remote = Remote::collect();
        let actor_type = remote
            .name_of::<TestActor>()
            .ok_or(anyhow::anyhow!("actor not registered"))
            .unwrap()
            .to_string();
        let params = &();
        let (completed_handle, mut completed_receiver) = mailbox::open_port(cx);
        // gspawn actor
        mesh_agent
            .gspawn(
                cx,
                actor_type,
                "Stuck".to_string(),
                bincode::serialize(params).unwrap(),
                completed_handle.bind(),
            )
            .await
            .unwrap();
        let result = completed_receiver.recv().await.unwrap();
        match result {
            GspawnResult::Success { actor_id, .. } => ActorRef::attest(actor_id),
            GspawnResult::Error(error_msg) => {
                panic!("gspawn failed: {}", error_msg);
            }
        }
    }

    /// In order to simulate stuckness, we have to do two things:
    /// An actor that is blocked forever AND
    /// a proc that does not time out when it is asked to wait for
    /// a stuck actor.
    #[tokio::test]
    async fn test_allocator_stuck_task() {
        // Override config.
        // Use temporary config for this test
        let config = hyperactor::config::global::lock();
        let _guard = config.override_key(
            hyperactor::config::PROCESS_EXIT_TIMEOUT,
            Duration::from_secs(1),
        );

        let command = Command::new(crate::testresource::get(
            "monarch/hyperactor_mesh/bootstrap",
        ));
        let mut allocator = ProcessAllocator::new(command);
        let mut alloc = allocator
            .allocate(AllocSpec {
                extent: extent! { replica = 1 },
                constraints: Default::default(),
                proc_name: None,
                transport: ChannelTransport::Unix,
            })
            .await
            .unwrap();

        // Get everything up into running state. We require that we get
        let mut procs = HashMap::new();
        let mut running = HashSet::new();
        let mut actor_ref = None;
        let (router, client, client_proc, router_addr) = spawn_proc(alloc.transport()).await;
        while running.is_empty() {
            match alloc.next().await.unwrap() {
                ProcState::Created {
                    create_key, point, ..
                } => {
                    procs.insert(create_key, point);
                }
                ProcState::Running {
                    create_key,
                    proc_id,
                    mesh_agent,
                    addr,
                } => {
                    router.bind(Reference::Proc(proc_id.clone()), addr.clone());

                    assert!(procs.contains_key(&create_key));
                    assert!(!running.contains(&create_key));

                    actor_ref = Some(
                        spawn_test_actor(0, &client_proc, &client, router_addr, mesh_agent).await,
                    );
                    running.insert(create_key.clone());
                    break;
                }
                event => panic!("unexpected event: {:?}", event),
            }
        }
        assert!(actor_ref.unwrap().send(&client, Wait).is_ok());

        // There is a stuck actor! We should get a watchdog failure.
        alloc.stop().await.unwrap();
        let mut stopped = HashSet::new();
        while let Some(ProcState::Stopped {
            create_key, reason, ..
        }) = alloc.next().await
        {
            assert_eq!(reason, ProcStopReason::Watchdog);
            stopped.insert(create_key);
        }
        assert!(alloc.next().await.is_none());
        assert_eq!(stopped, running);
    }
}
